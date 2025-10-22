import os
import re
import hashlib
from urllib.parse import urlparse

def safe_filename(url: str) -> str:
    h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    parsed = urlparse(url)
    path = parsed.path.rstrip("/") or "/"
    name = re.sub(r"[^0-9a-zA-Z-_.]+", "-", parsed.netloc + path)
    name = name.strip("-")
    if len(name) > 140:
        name = name[:140]
    return f"{name}-{h}.txt"


def ensure_dirs(base: str):
    urls = os.path.join(base, "urls")
    texts = os.path.join(base, "texts")
    images = os.path.join(base, "images")
    os.makedirs(urls, exist_ok=True)
    os.makedirs(texts, exist_ok=True)
    os.makedirs(images, exist_ok=True)
    return {"base": base, "urls": urls, "texts": texts, "images": images}