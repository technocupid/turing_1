import logging
import time

from configs import IMAGE_TIMEOUT, REQUEST_TIMEOUT, USER_AGENT


def fetch_page(session, url, domain_limiter):
    try:
        if not domain_limiter.can_fetch(url):
            logging.debug("Blocked by robots: %s", url)
            return 403, "", None
        domain_limiter.wait_for_slot()
        start = time.perf_counter()
        resp = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT)
        elapsed = time.perf_counter() - start
        status = resp.status_code
        ctype = resp.headers.get("Content-Type", "") or ""
        try:
            domain_limiter.record_response(elapsed, status)
        except Exception:
            logging.debug("Failed to record domain response for %s", url)
        if status != 200:
            return status, ctype, None
        # attempt to return text; if it fails, decode bytes
        try:
            return status, ctype, resp.text
        except Exception:
            try:
                return status, ctype, resp.content.decode("utf-8", errors="replace")
            except Exception:
                return status, ctype, ""
    except Exception:
        logging.exception("Exception fetching page: %s", url)
        return 0, "", None


def download_image(session, img_url, domain_limiter):
    try:
        if not domain_limiter.can_fetch(img_url):
            logging.debug("Image blocked by robots: %s", img_url)
            return None, None
        domain_limiter.wait_for_slot()
        start = time.perf_counter()
        resp = session.get(img_url, headers={"User-Agent": USER_AGENT}, stream=True, timeout=IMAGE_TIMEOUT)
        elapsed = time.perf_counter() - start
        status = resp.status_code
        try:
            domain_limiter.record_response(elapsed, status)
        except Exception:
            logging.debug("Failed to record domain response for %s", img_url)
        if resp.status_code != 200:
            return resp.status_code, None
        data = resp.content
        return 200, data
    except Exception:
        logging.exception("Exception downloading image: %s", img_url)
        return None, None
