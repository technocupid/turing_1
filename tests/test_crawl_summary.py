# tests/test_crawl_summary.py
# Stricter tests for crawl_summary.py

import os
import json
import csv

from crawl_summary import generate_summary


def make_sample_output(tmp_path):
    out = tmp_path / 'data'
    urls_dir = out / 'urls'
    texts_dir = out / 'texts'
    images_dir = out / 'images'
    urls_dir.mkdir(parents=True)
    texts_dir.mkdir(parents=True)
    images_dir.mkdir(parents=True)

    # create urls CSV with headers including topic and language and is_duplicate
    urls_csv = urls_dir / 'urls.csv'
    with open(urls_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        header = ['url', 'status', 'depth', 'parent', 'topic', 'language', 'is_duplicate']
        w.writerow(header)
        w.writerow(['https://a.com/page1', '200', '0', '', 'technology', 'en', '0'])
        w.writerow(['https://b.com/page2', '200', '1', 'https://a.com/page1', 'finance', 'en', '1'])
        w.writerow(['https://a.com/page3', '404', '1', 'https://a.com/page1', '', '', '0'])

    # create some text files
    t1 = texts_dir / 'a.txt'
    t1.write_text('This is some technology content. AI and cloud.')
    t2 = texts_dir / 'b.txt'
    t2.write_text('Finance related text about stock market and crypto.')

    # images manifest
    img_csv = images_dir / 'manifest.csv'
    with open(img_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['image_file', 'image_url', 'page_url', 'size_bytes'])
        w.writerow(['i1.jpg', 'https://a.com/i1.jpg', 'https://a.com/page1', '1234'])
        w.writerow(['i2.jpg', 'https://b.com/i2.jpg', 'https://b.com/page2', '2345'])

    return str(out)


def test_generate_summary_creates_files_and_checks_fields(tmp_path):
    outdir = make_sample_output(tmp_path)
    summary = generate_summary(outdir)

    # strict checks on JSON summary
    jpath = os.path.join(outdir, 'crawl_summary.json')
    assert os.path.exists(jpath), 'crawl_summary.json must exist'

    with open(jpath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # exact expected counts
    assert data['pages_total'] == 3
    assert data['images_total'] == 2

    # topics: technology=1, finance=1
    assert isinstance(data.get('topics'), dict)
    assert data['topics'].get('technology', 0) == 1
    assert data['topics'].get('finance', 0) == 1
    # languages: en=2
    # assert isinstance(data.get('languages'), dict)
    # assert data['languages'].get('en', 0) == 2

    # status counts exact mapping
    assert data['status_counts'].get('200', 0) == 2
    assert data['status_counts'].get('404', 0) == 1

    # text stats: min, max, avg must be consistent
    assert data['page_text_count'] == 2
    assert data['min_text_len'] is not None and data['max_text_len'] is not None
    assert data['min_text_len'] <= data['max_text_len']
    # avg should be between min and max
    assert data['avg_text_len'] >= data['min_text_len']
    assert data['avg_text_len'] <= data['max_text_len']

    # domain report CSV strict header and content
    dpath = os.path.join(outdir, 'domain_report.csv')
    assert os.path.exists(dpath), 'domain_report.csv must exist'
    with open(dpath, 'r', encoding='utf-8') as f:
        rows = list(csv.reader(f))
    # header exact
    assert rows[0] == ['domain', 'count']
    # expect a.com count == 2, b.com count == 1 (may be ordering independent)
    domain_counts = {r[0]: int(r[1]) for r in rows[1:]}
    assert domain_counts.get('a.com', 0) == 2
    assert domain_counts.get('b.com', 0) == 1

    # ensure images manifest was read correctly and contributes to images_total
    assert data['images_total'] == 2

    # ensure duplicates_skipped counted as 1 (from is_duplicate column)
    assert data['duplicates_skipped'] == 1


# end of test file
