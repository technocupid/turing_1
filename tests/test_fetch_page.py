# -----------------------------
# File: tests/test_fetch_page.py
# -----------------------------
import types

from download_utils import fetch_page


class DummyResp:
    def __init__(self, status=200, headers=None, text='ok', content=b'ok'):
        self.status_code = status
        self.headers = headers or {'Content-Type': 'text/html'}
        self.text = text
        self.content = content


def test_fetch_page_returns_text(monkeypatch):
    # create a fake session object with get method returning DummyResp
    class DummySession:
        def get(self, url, headers=None, timeout=None):
            return DummyResp(200, {'Content-Type': 'text/html'}, '<html>hello</html>', b'<html>hello</html>')

    # DomainLimiter stub with minimal methods
    class StubDomain:
        def can_fetch(self, ua, url=None):
            return True
        def wait_for_slot(self):
            return

    s = DummySession()
    status, ctype, text = fetch_page(s, 'https://example.com', StubDomain())
    # fetch_page in this threaded variant returns (status, ctype, text)
    assert status == 200
    assert 'html' in ctype or 'text' in ctype
    assert 'hello' in text

