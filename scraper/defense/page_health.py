from dataclasses import dataclass
from selenium.common.exceptions import WebDriverException

@dataclass
class HealthResult:
    ok: bool
    reason: str = ""
    url: str = ""

class PageHealthChecker:
    """
    Detects common "bad states" without trying to bypass anything.
    """
    def __init__(self):
        # keywords that often appear when login expires or page isn't normal
        self.login_markers = [
            "/login", "checkpoint", "session", "authwall"
        ]
        self.block_markers = [
            "try again", "unusual", "verify your", "blocked", "temporarily", "something went wrong"
        ]

    def check(self, driver) -> HealthResult:
        try:
            current_url = (driver.current_url or "").lower()
            title = (driver.title or "").lower()
            page_text = ""
            try:
                page_text = (driver.page_source or "").lower()
            except Exception:
                page_text = ""

            # Login/auth redirects
            for m in self.login_markers:
                if m in current_url:
                    return HealthResult(False, f"redirected_to_login({m})", current_url)

            if "sign in" in title and "linkedin" in title:
                return HealthResult(False, "signin_title_detected", current_url)

            # Very basic block/issue hints
            for m in self.block_markers:
                if m in page_text:
                    return HealthResult(False, f"page_contains_marker({m})", current_url)

            # Empty-ish page
            if len(page_text) < 2000:
                return HealthResult(False, "page_too_small", current_url)

            return HealthResult(True, "ok", current_url)

        except WebDriverException as e:
            return HealthResult(False, f"webdriver_exception({type(e).__name__})", "")
