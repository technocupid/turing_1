# -----------------------------
# File: tests/test_integration_mock.py
# -----------------------------
import pytest
import threading
import os

from crawler import threaded_crawl_enhanced


def test_run_small_crawl(tmp_path, monkeypatch):
    import requests

    class DummyResp:
        def __init__(self, url):
            self.url = url
            self.status_code = 200
            self.headers = {'Content-Type': 'text/html'}
            # simple page with one link and one image
            self.text = '<html><body><p>Hi</p><a href="/next">next</a><img src="/img.png"/></body></html>'
            self.content = self.text.encode('utf-8')

    class DummySession:
        def __init__(self):
            self.headers = {}
        def get(self, url, headers=None, timeout=None, stream=False):
            return DummyResp(url)

    # monkeypatch requests.Session to return our DummySession
    # monkeypatch.setattr('scraper_threaded_enhanced.requests.Session', lambda: DummySession())
    monkeypatch.setattr('crawler.requests.Session', lambda: DummySession())

    out = tmp_path / 'data'
    os.makedirs(out, exist_ok=True)
    # run crawler small - max_pages=2 so it finishes quickly
    threaded_crawl_enhanced('https://example.com', str(out), max_pages=2, max_depth=1, allow_external=False, max_workers=2, image_workers=1, resume=False, logfile=None, verbose=False)

    # check outputs exist
    assert (out / 'texts').exists()
    assert (out / 'urls' / 'urls.csv').exists()
    assert (out / 'images' / 'manifest.csv').exists()
