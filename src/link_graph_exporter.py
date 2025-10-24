#!/usr/bin/env python3
"""
Link Graph Exporter

Builds a simple directed link graph from crawler output and writes a JSON file
containing edges and basic node/domain stats.

Input layout (default `data/`):
 - data/urls/urls.csv  (expects columns: url,parent, ...)

Output:
 - data/link_graph.json  -> {"nodes": [...], "edges": [[src,dst],...], "domain_counts": {...}}

Usage:
    python src/link_graph_exporter.py --output data --out-file link_graph.json

This is intentionally small and robust: it reads the `parent` field written by the
crawler's urls.csv manifest. If parent is empty, no incoming edge is produced.
"""

import argparse
import csv
import json
import os
from collections import defaultdict


def read_urls_csv(path):
    rows = []
    try:
        with open(path, newline='', encoding='utf-8') as f:
            r = csv.DictReader(f)
            for row in r:
                rows.append(row)
    except FileNotFoundError:
        raise
    return rows


def make_domain(url):
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc.lower()
    except Exception:
        return ''


def build_link_graph(url_rows):
    edges = set()
    nodes = set()
    domain_counts = defaultdict(int)

    for r in url_rows:
        url = (r.get('url') or '').strip()
        parent = (r.get('parent') or '').strip()
        if not url:
            continue
        nodes.add(url)
        domain_counts[make_domain(url)] += 1
        if parent:
            # only add edge if parent is non-empty
            edges.add((parent, url))
            nodes.add(parent)

    # convert edges to sorted list for deterministic output
    edges_list = [list(e) for e in sorted(edges)]
    nodes_list = sorted(nodes)
    domain_counts = dict(sorted(domain_counts.items(), key=lambda kv: kv[1], reverse=True))

    return {
        'nodes': nodes_list,
        'edges': edges_list,
        'domain_counts': domain_counts,
    }


def write_graph(outpath, graph):
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(graph, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description='Export link graph from crawler output')
    parser.add_argument('--output', default='data', help='Crawler output dir (contains urls/urls.csv)')
    parser.add_argument('--out-file', default='link_graph.json', help='Output filename inside output dir')
    args = parser.parse_args()

    urls_csv = os.path.join(args.output, 'urls', 'urls.csv')
    if not os.path.exists(urls_csv):
        print('ERROR: urls.csv not found in', urls_csv)
        return 2

    rows = read_urls_csv(urls_csv)
    graph = build_link_graph(rows)
    outpath = os.path.join(args.output, args.out_file)
    write_graph(outpath, graph)
    print('Wrote link graph:', outpath)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
