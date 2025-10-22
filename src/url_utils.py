from urllib.parse import urlparse, urljoin, urldefrag

def normalize_url(base: str, link: str):
    if not link:
        return None
    link = link.strip()
    if link.startswith("javascript:") or link.startswith("mailto:") or link.startswith("data:"):
        return None
    try:
        joined = urljoin(base, link)
        clean, _ = urldefrag(joined)
        p = urlparse(clean)
        scheme = p.scheme or "http"
        netloc = p.hostname or ""
        if p.port:
            if (scheme == "http" and p.port != 80) or (scheme == "https" and p.port != 443):
                netloc = f"{netloc}:{p.port}"
        normalized = f"{scheme}://{netloc}{p.path or ''}{('?' + p.query) if p.query else ''}"
        return normalized
    except Exception:
        return None


def domain_of(url: str):
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""