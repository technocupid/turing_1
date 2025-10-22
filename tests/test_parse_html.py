# -----------------------------
# File: tests/test_parse_html.py
# -----------------------------
import pytest

from crawler import parse_html_for_links_and_text


def test_parse_html_basic():
    html = '''
    <html><head><title>Hi</title><script>var a=1</script></head>
    <body>
    <h1>Header</h1>
    <p>Some <b>text</b></p>
    <a href="/link1">L1</a>
    <a href="https://other.example/two">L2</a>
    <img src="/img.png" />
    </body></html>
    '''
    text, links, images = parse_html_for_links_and_text(html, 'https://example.com/base/')
    assert 'Header' in text
    assert any('/link1' in l or 'link1' in l for l in links)
    assert any('img.png' in im for im in images)

