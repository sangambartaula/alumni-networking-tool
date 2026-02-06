import random
import time

class BackoffController:
    """
    Simple backoff strategy:
    - normal_delay(): used between normal actions
    - recovery_delay(attempt): used when page looks unhealthy
    """
    def __init__(self, min_delay=1.0, max_delay=3.0, recovery_base=5.0, recovery_cap=60.0):
        self.min_delay = float(min_delay)
        self.max_delay = float(max_delay)
        self.recovery_base = float(recovery_base)
        self.recovery_cap = float(recovery_cap)

    def normal_delay(self):
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def recovery_delay(self, attempt: int):
        # exponential-ish backoff with jitter
        base = min(self.recovery_cap, self.recovery_base * (2 ** max(0, attempt)))
        jitter = random.uniform(0.6, 1.4)
        time.sleep(base * jitter)
