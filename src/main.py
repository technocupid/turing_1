#!/usr/bin/env python3
"""
Threaded web scraper — Enhanced with graceful SIGTERM handling

Features:
- Background image downloads in a separate thread pool.
- Resume capability using SQLite (optional --resume).
- Verbose/logfile support with rotating logs.
- **Graceful SIGINT/SIGTERM handling:** catches termination signals, sets a shutdown flag,
  stops accepting new work, persists frontier to the DB (if enabled), and attempts a clean
  shutdown of thread pools so in-progress work has a chance to finish.

Usage:
  pip install requests beautifulsoup4
  python scraper_threaded_enhanced.py https://example.com --max-pages 200 --depth 2 --output data --workers 10 --resume --logfile scraper.log --verbose

"""

import argparse
import os
from urllib.parse import urlparse
from crawler import threaded_crawl_enhanced
# ---------- CLI ----------

def main():
    parser = argparse.ArgumentParser(description="Threaded web scraper enhanced: image background, resume (SQLite), verbose/logfile, graceful shutdown")
    parser.add_argument('--start_url', default = "https://github.com/ssdajoker/Solo-Git", help='Starting URL to crawl')
    parser.add_argument("--max-pages", type=int, default=20, help="Maximum number of pages to process")
    parser.add_argument("--depth", type=int, default=2)
    parser.add_argument("--output", default="data")
    parser.add_argument("--allow-external", action="store_true")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--image-workers", type=int, default=4)
    parser.add_argument("--resume", action="store_true", help="Enable resume using SQLite DB in output dir")
    parser.add_argument("--logfile", type=str, default=None, help="Optional rotating logfile path")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose console logging (DEBUG)")
    args = parser.parse_args()

    if not urlparse(args.start_url).scheme:
        print("start_url missing scheme (http:// or https://)")
        return

    os.makedirs(args.output, exist_ok=True)
    threaded_crawl_enhanced(args.start_url, args.output, max_pages=args.max_pages, max_depth=args.depth,
                            allow_external=args.allow_external, max_workers=args.workers,
                            image_workers=args.image_workers, resume=args.resume, logfile=args.logfile, verbose=args.verbose)


if __name__ == "__main__":
    main()
