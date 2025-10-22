# -----------------------------
# File: tests/test_domain_limiter.py
# -----------------------------
import time

from limiter import DomainLimiter


def test_wait_for_slot_enforces_delay():
    d = DomainLimiter('example.com')
    # force a short delay
    d.crawl_delay = 0.4
    t0 = time.time()
    d.wait_for_slot()
    # immediate second call should wait approximately crawl_delay
    d.wait_for_slot()
    elapsed = time.time() - t0
    assert elapsed >= 0.4