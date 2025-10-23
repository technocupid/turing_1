import threading
import time
import pytest

from limiter import DomainLimiter


def test_record_response_and_get_health(monkeypatch):
    # Prevent network when reading robots.txt
    monkeypatch.setattr(DomainLimiter, "_read_robots", lambda self: None)
    dl = DomainLimiter("example.com")

    # initial health snapshot
    h0 = dl.get_health()
    assert isinstance(h0, dict)
    assert 'crawl_delay' in h0 and 'avg_latency' in h0

    # simulate fast responses
    for _ in range(5):
        dl.record_response(0.05, 200)
    fast_delay = dl.crawl_delay
    fast_avg = dl.avg_latency()
    assert fast_avg > 0

    # simulate slow responses to force backoff
    for _ in range(8):
        dl.record_response(2.5, 200)
    slow_delay = dl.crawl_delay
    assert slow_delay >= fast_delay
    assert dl.avg_latency() >= fast_avg

    # simulate errors to increase error_count
    prev_errors = dl.error_count
    for _ in range(6):
        dl.record_response(5.0, 500)
    assert dl.error_count >= prev_errors
    assert dl.error_rate() > 0.0

    h = dl.get_health()
    # health snapshot contains keys and reasonable types
    assert isinstance(h['crawl_delay'], float)
    assert isinstance(h['avg_latency'], float)
    assert isinstance(h['errors'], int)
    assert isinstance(h['requests'], int)


def test_wait_for_slot_does_not_deadlock(monkeypatch):
    monkeypatch.setattr(DomainLimiter, "_read_robots", lambda self: None)
    dl = DomainLimiter("example.org")

    # artificially set a long crawl delay so wait_for_slot will sleep
    dl.crawl_delay = 1.5

    def call_wait():
        # should sleep for approx 1.5s but must not hold the lock while sleeping
        dl.wait_for_slot()

    t = threading.Thread(target=call_wait)
    t.start()

    # Give the thread a moment to enter wait_for_slot and sleep
    time.sleep(0.1)

    # Now, while the other thread is sleeping, we should be able to record a response
    # without deadlocking; do it with a timeout to fail the test in case of deadlock
    def try_record():
        dl.record_response(0.1, 200)

    rec_thread = threading.Thread(target=try_record)
    rec_thread.start()
    rec_thread.join(timeout=2.0)
    t.join(timeout=2.0)

    assert not rec_thread.is_alive(), "record_response() appears to have deadlocked"
    assert not t.is_alive(), "wait_for_slot() thread did not finish in time"


def test_concurrent_record_response(monkeypatch):
    # Ensure record_response is safe under concurrent calls
    monkeypatch.setattr(DomainLimiter, "_read_robots", lambda self: None)
    dl = DomainLimiter("concurrent.example")

    def worker(n):
        for i in range(n):
            dl.record_response(0.05 + (i % 3) * 0.01, 200 if i % 5 else 500)

    threads = [threading.Thread(target=worker, args=(50,)) for _ in range(4)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=5.0)
        assert not th.is_alive(), "worker thread did not finish"

    # after concurrent writes, request_count should equal 4*50
    assert dl.request_count == 4 * 50
    assert dl.error_count >= 0
    # error_rate should be in [0,1]
    assert 0.0 <= dl.error_rate() <= 1.0
