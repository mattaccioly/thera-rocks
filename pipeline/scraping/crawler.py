from __future__ import annotations

from collections import deque
from typing import Iterable, List, Set

from bs4 import BeautifulSoup

from pipeline.config import CONFIG
from pipeline.scraping.fetch import PageContent, fetch_page, get_domain
from pipeline.utils.logging import setup_logger

logger = setup_logger(__name__)


def extract_links(html_text: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html_text, "lxml")
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            from urllib.parse import urlparse, urlunparse

            parsed = urlparse(base_url)
            href = urlunparse((parsed.scheme, parsed.netloc, href, "", "", ""))
        links.append(href)
    return links


def is_internal_link(start_url: str, candidate_url: str) -> bool:
    start_domain, _ = get_domain(start_url)
    candidate_domain, _ = get_domain(candidate_url)
    return start_domain == candidate_domain


def crawl_site(start_url: str, max_pages: int | None = None, max_depth: int | None = None) -> Iterable[PageContent]:
    max_pages = max_pages or CONFIG.max_pages_per_domain
    max_depth = max_depth or CONFIG.crawl_max_depth

    visited: Set[str] = set()
    queue: deque[tuple[str, int, str | None]] = deque([(start_url, 0, None)])

    results: List[PageContent] = []
    while queue and len(results) < max_pages:
        url, depth, referer = queue.popleft()
        if url in visited or depth > max_depth:
            continue

        try:
            page = fetch_page(url, referer=referer)
            results.append(page)
            visited.add(url)
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("Error fetching %s: %s", url, exc)
            visited.add(url)
            continue

        for link in extract_links(page.text, base_url=url):
            if len(results) + len(queue) >= max_pages:
                break
            if is_internal_link(start_url, link) and link not in visited:
                queue.append((link, depth + 1, url))

    return results





