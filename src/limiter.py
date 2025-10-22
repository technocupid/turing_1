import time
from urllib import robotparser
from threading import Lock

from configs import USER_AGENT, DEFAULT_PER_DOMAIN_DELAY

# ---------- Domain limiter (robots + delay) ----------
class DomainLimiter:
    def __init__(self, domain: str):
        self.domain = domain
        self.rp = robotparser.RobotFileParser()
        self._robots_urls = [f"https://{domain}/robots.txt", f"http://{domain}/robots.txt"]
        self.crawl_delay = DEFAULT_PER_DOMAIN_DELAY
        self.lock = Lock()
        self.last_request = 0.0
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
            if wait > 0:
                time.sleep(wait)
            self.last_request = time.time()
