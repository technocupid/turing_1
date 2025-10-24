# tests/test_link_graph_exporter.py
# Tests for src/link_graph_exporter.py

import os
import json
import csv

from link_graph_exporter import build_link_graph, write_graph


def make_sample_urls(tmp_path):
    out = tmp_path / 'data'
    urls_dir = out / 'urls'
    urls_dir.mkdir(parents=True)
    urls_csv = urls_dir / 'urls.csv'
    with open(urls_csv, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        # header must include 'url' and 'parent'
        w.writerow(['url', 'status', 'depth', 'parent', 'topic'])
        w.writerow(['https://a.com/page1', '200', '0', '', 'technology'])
        w.writerow(['https://a.com/page2', '200', '1', 'https://a.com/page1', 'technology'])
        w.writerow(['https://b.com/home', '200', '0', '', 'news'])
        w.writerow(['https://a.com/page3', '200', '1', 'https://b.com/home', 'finance'])
    return str(out)


def test_build_link_graph_and_write(tmp_path):
    outdir = make_sample_urls(tmp_path)
    urls_csv = os.path.join(outdir, 'urls', 'urls.csv')
    # read rows via the module helper
    import link_graph_exporter as exporter
    rows = exporter.read_urls_csv(urls_csv)
    graph = build_link_graph(rows)

    # nodes should include all urls and parents
    assert 'https://a.com/page1' in graph['nodes']
    assert 'https://a.com/page2' in graph['nodes']
    assert 'https://b.com/home' in graph['nodes']

    # edges should reflect parent relationships
    expected_edges = set([
        ('https://a.com/page1', 'https://a.com/page2'),
        ('https://b.com/home', 'https://a.com/page3'),
    ])
    assert set(tuple(e) for e in graph['edges']) == expected_edges

    # domain counts
    dc = graph['domain_counts']
    assert dc.get('a.com', 0) == 3
    assert dc.get('b.com', 0) == 1

    # write to file and read back
    outpath = os.path.join(outdir, 'link_graph.json')
    write_graph(outpath, graph)
    assert os.path.exists(outpath)
    with open(outpath, 'r', encoding='utf-8') as f:
        loaded = json.load(f)
    assert loaded['nodes'] == graph['nodes']
    assert loaded['edges'] == graph['edges']
    assert loaded['domain_counts'] == graph['domain_counts']
