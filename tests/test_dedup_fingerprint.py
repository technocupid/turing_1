# tests/test_dedup_fingerprint.py
import pytest
from utils import compute_content_hash

def test_compute_content_hash_idempotent_and_normalizes_whitespace():
    a = "  Hello   World\n\nThis is a TEST.  "
    b = "hello world this is a test."
    # note we lowercase and normalize whitespace in compute_content_hash
    h1 = compute_content_hash(a)
    h2 = compute_content_hash(b)
    assert isinstance(h1, str) and len(h1) == 64
    assert h1 == h2

def test_compute_content_hash_empty_returns_empty():
    assert compute_content_hash("") == ""
    assert compute_content_hash(None) == ""
