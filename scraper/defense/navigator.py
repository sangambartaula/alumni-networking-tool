import logging
from selenium.common.exceptions import WebDriverException

from .backoff import BackoffController
from .page_health import PageHealthChecker
from .proxy_manager import ProxyManager

logger = logging.getLogger(__name__)

CHALLENGE_MARKERS = [
    "let's do a quick security check",
    "verify your identity",
    "security verification",
    "checkpoint/challenge",
    "/checkpoint/",
    "please verify you are a human",
    "unusual activity",
]

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
        for attempt in range(self.max_retries + 1):
            try:
                logger.info(f"🌐 GET: {url} (attempt {attempt+1}/{self.max_retries+1})")
                self.driver.get(url)

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
