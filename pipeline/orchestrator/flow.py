from __future__ import annotations

from typing import List, Optional

from pipeline.config import CONFIG
from pipeline.ingestion.csv_normalizer import normalize_csv
from pipeline.llm.classifier import classify_first_page
from pipeline.llm.extractor import extract_startup_profile
from pipeline.scraping.crawler import crawl_site
from pipeline.scraping.fetch import PageContent
from pipeline.storage.db import init_db, insert_pages, upsert_startup
from pipeline.storage.schemas import ScrapedPage, StartupProfile
from pipeline.utils.logging import setup_logger

logger = setup_logger(__name__)


def ingest_csv_to_db(csv_path: str) -> int:
    init_db()
    rows = normalize_csv(csv_path)
    count = 0
    for r in rows:
        profile = StartupProfile(**{k: r.get(k) for k in StartupProfile.model_fields.keys() if k in r})
        profile.raw_data = r.get("raw_data", {})
        upsert_startup(profile)
        count += 1
    logger.info("Ingested %d CSV rows into DB", count)
    return count


def _ensure_url_scheme(url: str) -> str:
    if not url:
        return url
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "https://" + url


def ingest_and_scrape_csv(csv_path: str, max_pages: Optional[int] = None, max_depth: Optional[int] = None) -> int:
    # First ingest CSV data for baseline records
    ingest_csv_to_db(csv_path)

    # Then scrape URLs present in CSV
    rows = normalize_csv(csv_path)
    scraped = 0
    for r in rows:
        website = (r.get("website") or "").strip()
        if not website:
            continue
        website = _ensure_url_scheme(website)
        try:
            startup_id = scrape_url_with_llm(website, max_pages=max_pages, max_depth=max_depth)
            if startup_id is not None:
                scraped += 1
        except Exception as exc:  # pragma: no cover - network/LLM errors
            logger.exception("Failed scraping for %s: %s", website, exc)
    logger.info("Completed scrape for %d website(s) from CSV", scraped)
    return scraped


def scrape_url_with_llm(url: str, max_pages: Optional[int] = None, max_depth: Optional[int] = None) -> Optional[int]:
    init_db()

    # 1) Fetch first page only
    pages_iter = crawl_site(url, max_pages=1, max_depth=0)
    first_page_list = list(pages_iter)
    if not first_page_list:
        logger.warning("No content fetched from %s", url)
        return None
    first_page: PageContent = first_page_list[0]

    # 2) LLM gate
    is_real, conf, reason = classify_first_page(url=url, title=first_page.title, text_snippet=first_page.text)
    logger.info("LLM gate for %s: %s (confidence=%.2f). Reason: %s", url, "REAL" if is_real else "SCAM", conf, reason)
    if not is_real:
        insert_pages(
            [
                ScrapedPage(
                    url=first_page.url,
                    title=first_page.title,
                    content_text=first_page.text,
                    content_hash=first_page.content_hash,
                    http_status=first_page.status_code,
                )
            ]
        )
        return None

    # 3) Continue crawling limited pages
    pages: List[PageContent] = list(
        crawl_site(
            url,
            max_pages=max_pages or CONFIG.max_pages_per_domain,
            max_depth=max_depth or CONFIG.crawl_max_depth,
        )
    )

    # 4) Persist pages
    insert_pages(
        [
            ScrapedPage(
                url=p.url,
                title=p.title,
                content_text=p.text,
                content_hash=p.content_hash,
                http_status=p.status_code,
                referer_url=None,
            )
            for p in pages
        ]
    )

    # 5) Aggregate text and extract profile
    aggregated_text = "\n\n".join(p.text for p in pages)
    extracted = extract_startup_profile(website=url, aggregated_text=aggregated_text)
    profile = StartupProfile(**extracted)
    startup_id = upsert_startup(profile)
    return startup_id
