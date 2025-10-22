from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

from url_utils import normalize_url

def parse_html_for_links_and_text(html, base_url):
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return "", set(), []

    for el in soup(["script", "style", "noscript", "header", "footer", "svg", "meta", "link"]):
        try:
            el.extract()
        except Exception:
            pass

    try:
        texts = list(soup.stripped_strings)
        visible_text = "\n".join(texts)
    except Exception:
        visible_text = ""

    links = set()
    try:
        for a in soup.find_all("a", href=True):
            n = normalize_url(base_url, a.get("href"))
            if n:
                links.add(n)
    except Exception:
        pass

    images = []
    try:
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or img.get("data-original")
            n = normalize_url(base_url, src) if src else None
            if n:
                images.append(n)
    except Exception:
        pass

    return visible_text, links, images


def parse_sitemap_xml(text):
    urls = []
    try:
        root = ET.fromstring(text)
    except Exception:
        return urls
    for elem in root.iter():
        if elem.tag.endswith("loc") and elem.text:
            urls.append(elem.text.strip())
    return urls
