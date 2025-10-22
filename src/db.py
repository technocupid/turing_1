import logging
from threading import Lock
import sqlite3

class CrawlDB:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self._init_tables()
        self.lock = Lock()

    def _init_tables(self):
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                status TEXT,
                depth INTEGER,
                parent TEXT,
                visited INTEGER DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS frontier (
                url TEXT PRIMARY KEY,
                depth INTEGER,
                parent TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS images (
                image_file TEXT,
                image_url TEXT PRIMARY KEY,
                page_url TEXT,
                size_bytes INTEGER
            )
            """
        )
        self.conn.commit()

    def add_page(self, url, status=None, depth=0, parent=None, visited=0):
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO pages(url,status,depth,parent,visited) VALUES(?,?,?,?,?)",
                    (url, status or "", depth, parent or "", visited),
                )
                self.conn.commit()
            except Exception:
                logging.exception("Failed to add page to DB: %s", url)

    def mark_visited(self, url, status):
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("UPDATE pages SET visited=1, status=? WHERE url=?", (str(status), url))
                cur.execute("DELETE FROM frontier WHERE url=?", (url,))
                self.conn.commit()
            except Exception:
                logging.exception("Failed to mark visited in DB: %s", url)

    def add_frontier(self, url, depth, parent):
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("INSERT OR IGNORE INTO frontier(url,depth,parent) VALUES(?,?,?)", (url, depth, parent or ""))
                self.conn.commit()
            except Exception:
                logging.exception("Failed to add frontier in DB: %s", url)

    def pop_frontier_batch(self, limit=100):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT url,depth,parent FROM frontier ORDER BY rowid LIMIT ?", (limit,))
            rows = cur.fetchall()
            for r in rows:
                cur.execute("DELETE FROM frontier WHERE url=?", (r[0],))
            self.conn.commit()
            return rows

    def get_unvisited_pages(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT url,depth,parent FROM pages WHERE visited=0")
            return cur.fetchall()

    def add_image_manifest(self, image_file, image_url, page_url, size):
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("INSERT OR REPLACE INTO images(image_file,image_url,page_url,size_bytes) VALUES(?,?,?,?)",
                            (image_file, image_url, page_url, int(size or 0)))
                self.conn.commit()
            except Exception:
                logging.exception("Failed to insert image manifest: %s", image_url)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
