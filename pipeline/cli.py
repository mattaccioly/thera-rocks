from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from pipeline.orchestrator.flow import ingest_csv_to_db, ingest_and_scrape_csv, scrape_url_with_llm
from pipeline.utils.logging import setup_logger

logger = setup_logger(__name__)


def _cmd_ingest_csv(args: argparse.Namespace) -> None:
    path = Path(args.csv)
    if not path.exists():
        raise SystemExit(f"CSV not found: {path}")
    if args.scrape:
        ingest_and_scrape_csv(str(path), max_pages=args.max_pages, max_depth=args.max_depth)
    else:
        ingest_csv_to_db(str(path))


def _cmd_scrape_urls(args: argparse.Namespace) -> None:
    url: Optional[str] = args.url
    input_path: Optional[str] = args.input

    if not url and not input_path:
        raise SystemExit("Provide --url or --input file with URLs")

    urls = []
    if url:
        urls.append(url)
    if input_path:
        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    urls.append(line)

    for u in urls:
        try:
            startup_id = scrape_url_with_llm(u, max_pages=args.max_pages, max_depth=args.max_depth)
            if startup_id is None:
                logger.info("Skipped or stored only first page for %s", u)
            else:
                logger.info("Upserted startup id=%s for %s", startup_id, u)
        except Exception as exc:  # pragma: no cover - network/LLM errors
            logger.exception("Failed to process %s: %s", u, exc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline", description="Thera Rocks Data Pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p_ing = sub.add_parser("ingest-csv", help="Normalize a CSV and store in DB, optionally scrape websites from CSV")
    p_ing.add_argument("--csv", required=True, help="Path to CSV file")
    p_ing.add_argument("--scrape", action="store_true", help="After ingest, scrape website URLs from the CSV")
    p_ing.add_argument("--max-pages", type=int, default=None, help="Max pages to crawl per domain")
    p_ing.add_argument("--max-depth", type=int, default=None, help="Max crawl depth")
    p_ing.set_defaults(func=_cmd_ingest_csv)

    p_scrape = sub.add_parser("scrape-urls", help="Scrape URLs with LLM gating and extraction")
    p_scrape.add_argument("--url", help="Single URL to process")
    p_scrape.add_argument("--input", help="Text file with URLs (one per line)")
    p_scrape.add_argument("--max-pages", type=int, default=None, help="Max pages to crawl per domain")
    p_scrape.add_argument("--max-depth", type=int, default=None, help="Max crawl depth")
    p_scrape.set_defaults(func=_cmd_scrape_urls)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
