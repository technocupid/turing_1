# -----------------------------
# File: tests/test_utils.py
# -----------------------------
import os
import re
from urllib.parse import urlparse

import pytest

from utils import safe_filename
from url_utils import normalize_url



def test_normalize_url_relative_and_fragments():
    base = 'https://example.com/path/page.html'
    rel = '../other?x=1#frag'
    n = normalize_url(base, rel)
    assert n is not None
    p = urlparse(n)
    assert p.scheme in ('http', 'https')
    assert 'frag' not in n
    assert 'x=1' in n


def test_normalize_url_ignores_javascript_and_mailto():
    assert normalize_url('https://a/', 'javascript:alert(1)') is None
    assert normalize_url('https://a/', 'mailto:me@x.com') is None


def test_safe_filename_stable_and_short():
    u = 'https://example.com/some/long/path/with spaces/?q=1'
    f1 = safe_filename(u)
    f2 = safe_filename(u)
    assert f1 == f2
    assert len(f1) < 200

