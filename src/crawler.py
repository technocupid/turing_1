import os
import time
import hashlib
import logging
from logging.handlers import RotatingFileHandler
from collections import deque
from urllib.parse import urlparse
from threading import Event
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import requests

from configs import DB_NAME, GRACEFUL_SHUTDOWN_WAIT, USER_AGENT
from db import CrawlDB
from download_utils import download_image, fetch_page
from html_parsing import parse_html_for_links_and_text, parse_sitemap_xml
from io_helpers import make_csv_writer, save_binary, save_text
from limiter import DomainLimiter
from topic_detect import classify_topic
from url_utils import domain_of
from utils import safe_filename, ensure_dirs, compute_content_hash


# Global shutdown event set by signal handler
shutdown_event = Event()


def _signal_handler(signum, frame):
    logging.info("Received signal %s - initiating graceful shutdown...", signum)
    shutdown_event.set()


# Register signal handlers for graceful shutdown
signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def threaded_crawl_enhanced(start_url, output_base, max_pages=200, max_depth=2, allow_external=False,
                            max_workers=10, image_workers=4, resume=False, logfile=None, verbose=False):
    # logging setup
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    # console handler
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    root_logger.addHandler(ch)
    # file handler
    if logfile:
        fh = RotatingFileHandler(logfile, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        root_logger.addHandler(fh)

    dirs = ensure_dirs(output_base)
    urls_csv = os.path.join(dirs["urls"], "urls.csv")
    images_csv = os.path.join(dirs["images"], "manifest.csv")
    write_url_row, close_urls = make_csv_writer(urls_csv, ['url', 'status', 'depth', 'parent', 'topic'])
    write_image_row, close_images = make_csv_writer(images_csv, ["image_file", "image_url", "page_url", "size_bytes"])    

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    domain_cache = {}
    def get_domain_limiter_for(u):
        d = domain_of(u)
        if d not in domain_cache:
            domain_cache[d] = DomainLimiter(d)
        return domain_cache[d]

    # SQLite DB for resume
    db_path = os.path.join(output_base, DB_NAME)
    db = None
    if resume:
        try:
            db = CrawlDB(db_path)
            logging.info("Using DB for resume: %s", db_path)
        except Exception:
            logging.exception("Failed to open DB for resume; proceeding without resume")
            db = None

    # frontier
    frontier = deque()

    # if resume and DB has frontier, load it
    if db:
        rows = db.pop_frontier_batch(limit=1000)
        if rows:
            for u, depth, parent in rows:
                frontier.append((u, depth, parent))
            logging.info("Resumed frontier from DB: %d items", len(rows))
        else:
            db.add_page(start_url, status=None, depth=0, parent=None, visited=0)
            db.add_frontier(start_url, 0, None)
            frontier.append((start_url, 0, None))
    else:
        frontier.append((start_url, 0, None))

    # try sitemap to seed more URLs (only when not resuming or frontier small)
    try:
        parsed = urlparse(start_url)
        sitemap_url = f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"
        r = session.get(sitemap_url, headers={"User-Agent": USER_AGENT}, timeout=5)
        if r.status_code == 200 and r.text:
            sitemap_urls = parse_sitemap_xml(r.text)
            for u in sitemap_urls:
                frontier.append((u, 0, "sitemap"))
                if db:
                    db.add_page(u, status=None, depth=0, parent='sitemap', visited=0)
                    db.add_frontier(u, 0, 'sitemap')
            logging.info("Seeded %d URLs from sitemap", len(sitemap_urls))
    except Exception:
        logging.debug("Sitemap unavailable or failed")

    visited = set()

    # image executor (background)
    image_executor = ThreadPoolExecutor(max_workers=image_workers)

    def submit_image_download(img_url, page_url):
        # returns future
        if shutdown_event.is_set():
            logging.debug("Shutdown requested: skipping image submission: %s", img_url)
            return None
        return image_executor.submit(process_image_job, img_url, page_url, get_domain_limiter_for(img_url), dirs, write_image_row, db)

    # helper image job (runs in background executor)
    def process_image_job(img_url, page_url, domain_limiter, dirs, write_image_row_func, db_obj):
        try:
            if shutdown_event.is_set():
                logging.debug("Shutdown requested: aborting image job: %s", img_url)
                return
            status, data = download_image(session, img_url, domain_limiter)
            if status == 200 and data:
                p = urlparse(img_url).path
                ext = 'jpg'
                if '.' in p:
                    ext_candidate = p.split('.')[-1]
                    if len(ext_candidate) <= 5:
                        ext = ext_candidate
                name = hashlib.sha1(img_url.encode('utf-8')).hexdigest()[:16]
                fname = f"{name}.{ext}"
                path = os.path.join(dirs['images'], fname)
                save_binary(path, data)
                size = os.path.getsize(path) if os.path.exists(path) else 0
                write_image_row_func([fname, img_url, page_url, size])
                if db_obj:
                    db_obj.add_image_manifest(fname, img_url, page_url, size)
            else:
                logging.debug("Image download failed: %s status=%s", img_url, status)
        except Exception:
            logging.exception("Image job failed for: %s", img_url)

    # main page processing function executed by page worker pool
    def process_url(url, depth, parent):
        if shutdown_event.is_set():
            logging.debug("Shutdown requested: skipping page processing: %s", url)
            return []
        if url in visited:
            return []
        if depth > max_depth:
            return []
        logging.info("Processing (depth=%d): %s", depth, url)
        try:
            dl = get_domain_limiter_for(url)
            status, ctype, text = fetch_page(session, url, dl)
            visited.add(url)
            topic = classify_topic(text)


            write_url_row([url, status, depth, parent or "", topic])
            if db:
                db.add_page(url, status=status, depth=depth, parent=parent or '', visited=1)
                db.mark_visited(url, status)

            new_links = []
            if text:
                visible_text, links, images = parse_html_for_links_and_text(text, url)
                content_hash = compute_content_hash(visible_text)
                is_dup = False
                canonical_url = ''

                if db and content_hash:
                    if db.has_content_hash(content_hash):
                        canonical_url = db.get_canonical_url_for_hash(content_hash)
                        logging.info("Duplicate content detected for %s (same as %s) - skipping save", url, canonical_url)
                        db.mark_page_duplicate(url, content_hash, canonical_url)
                        is_dup = True
                    else:
                        db.register_content_hash(content_hash, url)

                # If not duplicate, save and process images
                if not is_dup:
                    fname = safe_filename(url)
                    textpath = os.path.join(dirs['texts'], fname)
                    save_text(textpath, visible_text)

                    for img in images:
                        if shutdown_event.is_set():
                            break
                        if not img:
                            continue
                        if (not allow_external) and domain_of(img) != domain_of(url):
                            continue
                        if db:
                            db.add_image_manifest('', img, url, 0)
                        try:
                            submit_image_download(img, url)
                        except Exception:
                            logging.exception("Failed to submit image job: %s", img)
                else:
                    # Optional: mark duplicates differently in logs
                    logging.debug("Skipped saving duplicate page %s", url)

                # collect new links
                for link in links:
                    if shutdown_event.is_set():
                        break
                    if not link:
                        continue
                    if (not allow_external) and domain_of(link) != domain_of(start_url):
                        continue
                    if link not in visited:
                        new_links.append((link, depth + 1, url))
                        if db:
                            db.add_page(link, status=None, depth=depth+1, parent=url, visited=0)
                            db.add_frontier(link, depth+1, url)
            return new_links
        except Exception:
            logging.exception("Error processing URL: %s", url)
            try:
                write_url_row([url, 'error', depth, parent or ''])
            except Exception:
                pass
            return []

    # page worker pool
    page_executor = ThreadPoolExecutor(max_workers=max_workers)
    futures_to_item = {}

    try:
        while frontier and len(visited) < max_pages and not shutdown_event.is_set():
            # submit page jobs up to available worker slots
            while frontier and len(futures_to_item) < max_workers and len(visited) + len(futures_to_item) < max_pages and not shutdown_event.is_set():
                item = frontier.popleft()
                url, depth, parent = item
                if url in visited:
                    continue
                if depth > max_depth:
                    continue
                fut = page_executor.submit(process_url, url, depth, parent)
                futures_to_item[fut] = item

            if not futures_to_item:
                # nothing in flight; either frontier empty or reached max
                if not frontier or shutdown_event.is_set():
                    break
                else:
                    time.sleep(0.1)
                    continue

            # wait for a future to complete
            done_iter = as_completed(futures_to_item)
            try:
                done = next(done_iter)
            except StopIteration:
                time.sleep(0.1)
                continue

            try:
                new_links = done.result()
            except Exception:
                logging.exception("Future raised during result()")
                new_links = []

            originating_item = futures_to_item.pop(done, None)
            # add new links to frontier (BFS)
            for nl in new_links:
                if shutdown_event.is_set():
                    break
                if len(visited) + len(futures_to_item) >= max_pages:
                    break
                if nl[0] not in visited:
                    frontier.append(nl)

        # If shutdown requested, log and persist frontier
        if shutdown_event.is_set():
            logging.info("Shutdown requested - saving frontier and stopping submission of new tasks...")

        # wait for remaining page futures to complete, but do not block forever
        wait_deadline = time.time() + GRACEFUL_SHUTDOWN_WAIT
        for fut in list(futures_to_item.keys()):
            remaining = max(0.0, wait_deadline - time.time())
            try:
                fut.result(timeout=remaining if remaining > 0 else 0.1)
            except Exception:
                logging.debug("Page future did not finish before timeout or raised an exception")

        # attempt graceful image executor shutdown (allow already submitted images to finish)
        logging.info("Shutting down image executor, waiting briefly for image jobs to finish")
        try:
            image_executor.shutdown(wait=False)
            # give a short grace period for running image tasks to finish
            grace_end = time.time() + max(2.0, min(GRACEFUL_SHUTDOWN_WAIT, 5.0))
            while time.time() < grace_end:
                if shutdown_event.is_set() and not image_executor._work_queue.qsize():
                    break
                time.sleep(0.2)
        except Exception:
            logging.exception("Error shutting down image executor")

    except KeyboardInterrupt:
        logging.info("Interrupted by user, initiating graceful shutdown...")
        shutdown_event.set()
    except Exception:
        logging.exception("Top-level crawler exception")
    finally:
        logging.info("Finalizing: persisting state and closing resources")
        # stop submitting new tasks
        try:
            page_executor.shutdown(wait=False)
        except Exception:
            logging.exception("Error shutting down page executor")

        # dump remaining frontier to DB if resume enabled
        if db:
            try:
                for (u, d, p) in frontier:
                    db.add_frontier(u, d, p)
                logging.info("Saved frontier to DB (%d items)", len(frontier))
            except Exception:
                logging.exception("Failed saving frontier to DB")
            try:
                db.close()
            except Exception:
                pass

        # close CSVs
        try:
            close_urls()
        except Exception:
            pass
        try:
            close_images()
        except Exception:
            pass

        # dump domain health to JSON
        try:
            health = {}
            for d, dl in domain_cache.items():
                try:
                    health[d] = dl.get_health()
                except Exception:
                    health[d] = {"error": "failed to collect"}
            import json
            outpath = os.path.join(output_base, "domain_health.json")
            with open(outpath, "w", encoding="utf-8") as f:
                json.dump(health, f, ensure_ascii=False, indent=2)
            logging.info("Wrote domain health to %s", outpath)
        except Exception:
            logging.exception("Failed to write domain health")

        logging.info("Crawl finished. Processed %d pages. Data in %s", len(visited), output_base)
