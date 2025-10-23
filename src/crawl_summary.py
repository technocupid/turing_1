#!/usr/bin/env python3
"""
Generate a compact crawl summary from the existing crawler output.

This small utility is intentionally non-invasive: it does not modify the crawler; it
post-processes the output directory (the `data/` layout) and produces a JSON summary
and a CSV domain report.

Outputs (placed inside the same `output_dir`):
 - crawl_summary.json  (summary statistics)
 - domain_report.csv   (per-domain counts)

Usage:
    python scripts_generate_crawl_summary.py --output data

"""
import argparse
import csv
import json
import os
from collections import Counter, defaultdict


def read_urls_csv(path):
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                rows.append(row)
    except FileNotFoundError:
        return rows
    return rows


def read_images_manifest(path):
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                rows.append(row)
    except FileNotFoundError:
        return rows
    return rows


def collect_text_stats(texts_dir):
    stats = {
        'page_text_count': 0,
        'total_text_chars': 0,
        'avg_text_len': 0,
        'min_text_len': None,
        'max_text_len': None,
    }
    if not os.path.isdir(texts_dir):
        return stats
    files = [os.path.join(texts_dir, p) for p in os.listdir(texts_dir) if p.endswith('.txt') or p.endswith('.txt.gz')]
    for p in files:
        try:
            if p.endswith('.gz'):
                import gzip
                with gzip.open(p, 'rt', encoding='utf-8', errors='ignore') as f:
                    t = f.read()
            else:
                with open(p, 'r', encoding='utf-8', errors='ignore') as f:
                    t = f.read()
            ln = len(t)
            stats['page_text_count'] += 1
            stats['total_text_chars'] += ln
            if stats['min_text_len'] is None or ln < stats['min_text_len']:
                stats['min_text_len'] = ln
            if stats['max_text_len'] is None or ln > stats['max_text_len']:
                stats['max_text_len'] = ln
        except Exception:
            continue
    if stats['page_text_count']:
        stats['avg_text_len'] = stats['total_text_chars'] / stats['page_text_count']
    else:
        stats['avg_text_len'] = 0
    return stats


def make_domain_from_url(url):
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.lower()
    except Exception:
        return ''


def generate_summary(output_dir: str):
    urls_csv = os.path.join(output_dir, 'urls', 'urls.csv')
    images_csv = os.path.join(output_dir, 'images', 'manifest.csv')
    texts_dir = os.path.join(output_dir, 'texts')

    urls = read_urls_csv(urls_csv)
    images = read_images_manifest(images_csv)
    text_stats = collect_text_stats(texts_dir)

    summary = {}
    summary['pages_total'] = len(urls)

    # topics and languages if available as columns
    topic_counter = Counter()
    # lang_counter = Counter()
    dup_count = 0
    status_counter = Counter()
    domain_counter = Counter()

    for r in urls:
        # handle possible header variations robustly
        topic = r.get('topic') or r.get('label') or r.get('category') or ''
        # lang = r.get('language') or r.get('lang') or ''
        status = r.get('status') or ''
        url = r.get('url') or r.get('link') or ''
        is_dup = False
        # some CSVs may include is_duplicate column
        try:
            if 'is_duplicate' in r and r['is_duplicate']:
                v = r['is_duplicate']
                if str(v).strip() not in ('0', '', 'False', 'false'):
                    is_dup = True
        except Exception:
            pass

        if topic:
            topic_counter[topic] += 1
        # if lang:
        #     lang_counter[lang] += 1
        if is_dup:
            dup_count += 1
        if status:
            status_counter[status] += 1
        if url:
            domain = make_domain_from_url(url)
            if domain:
                domain_counter[domain] += 1

    summary['duplicates_skipped'] = dup_count
    summary['status_counts'] = dict(status_counter)
    summary['topics'] = dict(topic_counter)
    # summary['languages'] = dict(lang_counter)
    summary.update(text_stats)
    summary['images_total'] = len(images)
    # top domains
    top_domains = domain_counter.most_common(20)
    summary['top_domains'] = top_domains

    # write outputs
    json_out = os.path.join(output_dir, 'crawl_summary.json')
    with open(json_out, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # domain CSV
    domain_csv = os.path.join(output_dir, 'domain_report.csv')
    with open(domain_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['domain', 'count'])
        for dom, cnt in domain_counter.most_common():
            w.writerow([dom, cnt])

    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='data', help='Output directory that contains urls/, texts/, images/')
    args = parser.parse_args()
    summary = generate_summary(args.output)
    print('Summary written. pages_total=%d, images=%d' % (summary['pages_total'], summary['images_total']))


if __name__ == '__main__':
    main()
