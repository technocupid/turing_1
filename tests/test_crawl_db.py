# -----------------------------
# File: tests/test_crawl_db.py
# -----------------------------
import sqlite3
import os

from db import CrawlDB


def test_crawl_db_basic(tmp_path):
    dbfile = tmp_path / 'test.db'
    db = CrawlDB(str(dbfile))
    try:
        db.add_page('https://a/', status='200', depth=0, parent='', visited=0)
        db.add_frontier('https://a/', 0, '')
        rows = db.pop_frontier_batch(limit=10)
        assert len(rows) >= 1
        # after popping, frontier should be empty
        rows2 = db.pop_frontier_batch(limit=10)
        assert len(rows2) == 0
        db.add_image_manifest('img.jpg', 'https://a/img.jpg', 'https://a/', 123)
        # check sqlite content directly
        conn = sqlite3.connect(str(dbfile))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM images")
        c = cur.fetchone()[0]
        assert c == 1
        conn.close()
    finally:
        db.close()

