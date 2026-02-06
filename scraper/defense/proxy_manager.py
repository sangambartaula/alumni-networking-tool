from dataclasses import dataclass, field
from typing import List, Optional

@dataclass
class ConnectionProfile:
    name: str
    failures: int = 0
    disabled: bool = False

class ProxyManager:
    """
    Proxy-ready abstraction.
    For now: no real proxies. It just rotates 'connection profiles'
    and tracks failures so later you can plug in actual proxies easily.
    """
    def __init__(self, profiles: Optional[List[ConnectionProfile]] = None, max_failures: int = 3):
        self.max_failures = max_failures
        self.profiles: List[ConnectionProfile] = profiles or [
            ConnectionProfile("default-1"),
            ConnectionProfile("default-2"),
            ConnectionProfile("default-3"),
        ]
        self.index = 0

    def current(self) -> ConnectionProfile:
        return self.profiles[self.index]

    def mark_failure(self):
        p = self.current()
        p.failures += 1
        if p.failures >= self.max_failures:
            p.disabled = True

    def rotate(self) -> ConnectionProfile:
        # move to next enabled profile
        n = len(self.profiles)
        for _ in range(n):
            self.index = (self.index + 1) % n
            if not self.current().disabled:
                return self.current()
        # if all disabled, re-enable all (safe fallback)
        for p in self.profiles:
            p.disabled = False
            p.failures = 0
        self.index = 0
        return self.current()
