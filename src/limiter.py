import time
from urllib import robotparser
from threading import RLock
import logging
from configs import USER_AGENT, DEFAULT_PER_DOMAIN_DELAY

# ---------- Domain limiter (robots + delay) ----------
from collections import deque

class DomainLimiter:
    # bounds for crawl_delay (seconds)
    MIN_DELAY = 0.1
    MAX_DELAY = 30.0

    def __init__(self, domain: str, window: int = 8):
        self.domain = domain
        self.rp = robotparser.RobotFileParser()
        self._robots_urls = [f"https://{domain}/robots.txt", f"http://{domain}/robots.txt"]
        self.crawl_delay = DEFAULT_PER_DOMAIN_DELAY
        self.latency_samples = deque(maxlen=window)
        self.lock = RLock()
        self.last_request = 0.0
        self.error_count = 0
        self.request_count = 0
        self._read_robots()

    def _read_robots(self):
        for rurl in self._robots_urls:
            try:
                self.rp.set_url(rurl)
                self.rp.read()
                cd = None
                try:
                    cd = self.rp.crawl_delay(USER_AGENT)
                except Exception:
                    cd = None
                if cd is None:
                    try:
                        cd = self.rp.crawl_delay("*")
                    except Exception:
                        cd = None
                if cd is not None:
                    self.crawl_delay = float(cd)
                return
            except Exception:
                continue

    def can_fetch(self, url: str):
        try:
            return self.rp.can_fetch(USER_AGENT, url)
        except Exception:
            return True

    def wait_for_slot(self):
        with self.lock:
            now = time.time()
            wait = self.crawl_delay - (now - self.last_request)
            if wait <= 0:
                self.last_request = time.time()
                return
        # sleep outside lock (prevents blocking other threads)
        time.sleep(wait)

        # after sleeping, update last_request under lock
        with self.lock:
            self.last_request = time.time()

    def record_response(self, latency: float, status_code: int):
        """
        Call this AFTER a request completes.
        - latency: elapsed seconds for the request (float)
        - status_code: HTTP status code (int)
        """
        try:
            with self.lock:
                self.request_count += 1
                if latency is None:
                    latency = max(self.crawl_delay * 2, 10.0)
                self.latency_samples.append(float(latency))
                if status_code is None or status_code >= 500 or status_code == 429:
                    self.error_count += 1

                avg_latency = sum(self.latency_samples) / len(self.latency_samples)

                # adaptive policy
                if avg_latency > (self.crawl_delay * 2) or self.error_rate() > 0.2:
                    new_delay = min(self.crawl_delay * 1.8, self.MAX_DELAY)
                    if new_delay > self.crawl_delay:
                        self.crawl_delay = new_delay
                else:
                    if avg_latency > 0 and avg_latency < (self.crawl_delay * 0.6):
                        new_delay = max(self.crawl_delay * 0.85, self.MIN_DELAY)
                        if new_delay < self.crawl_delay:
                            self.crawl_delay = new_delay

                # bounds
                if self.crawl_delay < self.MIN_DELAY:
                    self.crawl_delay = self.MIN_DELAY
                if self.crawl_delay > self.MAX_DELAY:
                    self.crawl_delay = self.MAX_DELAY
        except Exception:
            logging.exception("record_response failed for domain %s", self.domain)

    def error_rate(self) -> float:
        with self.lock:
            if self.request_count == 0:
                return 0.0
            return float(self.error_count) / float(self.request_count)

    def avg_latency(self) -> float:
        with self.lock:
            if not self.latency_samples:
                return 0.0
            return sum(self.latency_samples) / len(self.latency_samples)

    def get_health(self) -> dict:
        """Return a snapshot dict suitable for JSON serialization."""
        with self.lock:
            return {
                "domain": self.domain,
                "crawl_delay": float(self.crawl_delay),
                "avg_latency": float(self.avg_latency()),
                "errors": int(self.error_count),
                "requests": int(self.request_count),
                "error_rate": float(self.error_rate()),
            }
