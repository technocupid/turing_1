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
        # pages: added content_hash, is_duplicate, duplicate_of
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                status TEXT,
                depth INTEGER,
                parent TEXT,
                visited INTEGER DEFAULT 0,
                content_hash TEXT,
                is_duplicate INTEGER DEFAULT 0,
                duplicate_of TEXT DEFAULT ''
            )
            """
        )
        # frontier unchanged
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS frontier (
                url TEXT PRIMARY KEY,
                depth INTEGER,
                parent TEXT
            )
            """
        )
        # images unchanged
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
        # content map: one canonical_url per content_hash
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS content_map (
                content_hash TEXT PRIMARY KEY,
                canonical_url TEXT
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


    def has_content_hash(self, content_hash: str) -> bool:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT 1 FROM content_map WHERE content_hash=? LIMIT 1", (content_hash,))
            return cur.fetchone() is not None

    def get_canonical_url_for_hash(self, content_hash: str) -> str:
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("SELECT canonical_url FROM content_map WHERE content_hash=? LIMIT 1", (content_hash,))
            r = cur.fetchone()
            return r[0] if r else ''

    def register_content_hash(self, content_hash: str, canonical_url: str):
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("INSERT OR REPLACE INTO content_map(content_hash, canonical_url) VALUES(?,?)",
                            (content_hash, canonical_url))
                # update pages table for canonical_url if present
                cur.execute("UPDATE pages SET content_hash=? WHERE url=?", (content_hash, canonical_url))
                self.conn.commit()
            except Exception:
                logging.exception("Failed to register content hash: %s", content_hash)

    def mark_page_duplicate(self, url: str, content_hash: str, canonical_url: str):
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute(
                    "UPDATE pages SET content_hash=?, is_duplicate=1, duplicate_of=? WHERE url=?",
                    (content_hash, canonical_url, url),
                )
                self.conn.commit()
            except Exception:
                logging.exception("Failed to mark page duplicate: %s", url)
