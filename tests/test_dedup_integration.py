# tests/test_dedup_integration.py
import os
import shutil
import tempfile
import sqlite3
from pathlib import Path
import pytest

# Ensure tests find the src package (use conftest.py or set PYTHONPATH=src)
from crawler import threaded_crawl_enhanced
from db import CrawlDB

# Monkeypatch requests.Session to return the same content for two URLs
class DummyResp:
    def __init__(self, url):
        self.url = url
        self.status_code = 200
        self.headers = {'Content-Type': 'text/html'}
        # same HTML for different URLs -> should be deduplicated
        self.text = '<html><body><h1>Same Content</h1><p>Body text.</p></body></html>'
        self.content = self.text.encode('utf-8')

class DummySession:
    def __init__(self):
        self.headers = {}
    def get(self, url, headers=None, timeout=None, stream=False):
        return DummyResp(url)

def test_dedup_two_identical_pages(monkeypatch, tmp_path):
    # monkeypatch requests.Session used by the module
    monkeypatch.setattr('crawler.requests.Session', lambda: DummySession())

    outdir = tmp_path / "data"
    outdir.mkdir()
    # run crawling with two start seeds to emulate two pages with same content
    # We pass max_pages=2 so both are processed
    start_url = 'https://example.com/page1'
    # We'll also manually ensure the frontier contains page2 by using resume DB or by seeding
    # Simpler: run crawler with start_url and rely on sitemap or links - but we control DummyResp content so
    # We will call threaded_crawl_enhanced twice: once for page1 and once for page2, using same DB (resume).
    # Setup DB path
    db_path = outdir / 'crawl_state.db'
    # First run: crawl page1 and register content
    threaded_crawl_enhanced(start_url, str(outdir), max_pages=1, max_depth=0, allow_external=False, max_workers=1, image_workers=1, resume=True, logfile=None, verbose=False)
    # second run: crawl page2, using same DB -> should detect duplicate
    threaded_crawl_enhanced('https://example.com/page2', str(outdir), max_pages=1, max_depth=0, allow_external=False, max_workers=1, image_workers=1, resume=True, logfile=None, verbose=False)

    # Inspect DB to verify content_map and pages table
    db = CrawlDB(str(db_path))
    try:
        conn = db.conn
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM content_map")
        content_map_count = cur.fetchone()[0]
        # content_map should have exactly 1 entry (both pages share same content)
        assert content_map_count == 1

        cur.execute("SELECT url, is_duplicate, duplicate_of FROM pages")
        rows = cur.fetchall()
        # Should have two pages recorded
        assert len(rows) >= 2
        # Find which one is duplicate
        duplicates = [r for r in rows if r[1] == 1]
        assert len(duplicates) >= 1
        # duplicate_of should point to canonical url
        for dup in duplicates:
            assert dup[2] != ''
    finally:
        db.close()
