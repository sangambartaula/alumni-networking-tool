import logging
import os
import sys
from urllib.parse import urlparse
from selenium.common.exceptions import WebDriverException

from .backoff import BackoffController
from .page_health import PageHealthChecker
from .proxy_manager import ProxyManager

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    return default


_SCRAPER_DEBUG = _env_bool("SCRAPER_DEBUG", False)
logger.setLevel(logging.DEBUG if _SCRAPER_DEBUG else logging.WARNING)

CHALLENGE_MARKERS = [
    "let's do a quick security check",
    "verify your identity",
    "security verification",
    "checkpoint/challenge",
    "/checkpoint/",
    "please verify you are a human",
    "unusual activity",
]

ALLOWED_LINKEDIN_HOSTS = {"linkedin.com", "www.linkedin.com"}
ALLOWED_LINKEDIN_PATH_PREFIXES = (
    "/home",
    "/login",
    "/uas/login",
    "/search",
    "/search/",
    "/feed",
    "/in",
    "/in/",
    "/company/",
    "/school/",
    "/jobs/",
    "/groups/",
)
ALLOWED_LINKEDIN_EXACT_PATHS = {"", "/"}

class SafeNavigator:
    """
    A safe wrapper around driver.get(url):
    - loads URL
    - checks page health
    - if unhealthy: backoff + rotate (logical) + retry a few times
    """
    def __init__(self, driver, max_retries: int = 2):
        self.driver = driver
        self.max_retries = max_retries
        self.backoff = BackoffController()
        self.health = PageHealthChecker()
        self.proxies = ProxyManager()

    def _is_familiar_linkedin_url(self, url: str) -> bool:
        if not url:
            return False
        try:
            parsed = urlparse(url)
        except Exception:
            return False

        host = (parsed.netloc or "").lower().strip()
        if host not in ALLOWED_LINKEDIN_HOSTS:
            return False

        path = (parsed.path or "").strip()
        if path in ALLOWED_LINKEDIN_EXACT_PATHS:
            return True

        return any(path.startswith(prefix) for prefix in ALLOWED_LINKEDIN_PATH_PREFIXES)

    def _wait_for_user_on_unfamiliar_url(self, url: str) -> bool:
        logger.warning("⏸️ Unfamiliar URL detected: %s", url)
        logger.warning("Scraper paused for safety.")

        if not hasattr(sys.stdin, "isatty") or not sys.stdin.isatty():
            logger.warning("Non-interactive terminal detected; cannot prompt. Stopping navigation.")
            return False

        logger.warning("Type 'allow' to continue this URL once, or 'exit' to stop the run.")
        while True:
            try:
                response = input("[unfamiliar-url] allow/exit: ").strip().lower()
            except (EOFError, KeyboardInterrupt):
                return False

            if response == "allow":
                logger.warning("User approved unfamiliar URL once: %s", url)
                return True
            if response in {"exit", "quit", "force exit"}:
                logger.warning("User aborted run after unfamiliar URL: %s", url)
                return False

            logger.warning("Unknown response '%s'. Type 'allow' or 'exit'.", response)

    def _is_page_healthy(self):
        """Check if the current page is a real LinkedIn page, not a challenge."""
        try:
            page_source = self.driver.page_source.lower()
            current_url = self.driver.current_url.lower()

            for marker in CHALLENGE_MARKERS:
                if marker in page_source or marker in current_url:
                    logger.warning(
                        f"⚠️ Page unhealthy: challenge marker '{marker}' found "
                        f"url={current_url}"
                    )
                    return False

            return True
        except Exception as e:
            logger.warning(f"⚠️ Health check error: {e}")
            return False

    def get(self, url: str) -> bool:
        if not self._is_familiar_linkedin_url(url):
            return self._wait_for_user_on_unfamiliar_url(url)

        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"🌐 GET: {url} (attempt {attempt+1}/{self.max_retries+1})")
                self.driver.get(url)

                current_url = getattr(self.driver, "current_url", "") or ""
                if not self._is_familiar_linkedin_url(current_url):
                    return self._wait_for_user_on_unfamiliar_url(current_url)

                # small normal delay to reduce flakiness
                self.backoff.normal_delay()

                result = self.health.check(self.driver)
                if result.ok and self._is_page_healthy():
                    return True

                logger.warning(f"⚠️ Page unhealthy: {result.reason} url={result.url}")
                self.proxies.mark_failure()
                rotated = self.proxies.rotate()
                logger.info(f"🔁 Rotating connection profile -> {rotated.name}")

                if attempt < self.max_retries:
                    self.backoff.recovery_delay(attempt)
                    continue
                return False

            except WebDriverException as e:
                logger.warning(f"⚠️ WebDriverException on get(): {e}")
                self.proxies.mark_failure()
                self.proxies.rotate()
                if attempt < self.max_retries:
                    self.backoff.recovery_delay(attempt)
                    continue
                return False
