from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

from pipeline.config import CONFIG
from pipeline.utils.logging import setup_logger

logger = setup_logger(__name__)


@dataclass
class PageContent:
    url: str
    title: str
    text: str
    status_code: int
    content_hash: str


def fetch_page(url: str, referer: Optional[str] = None) -> PageContent:
    headers = {"User-Agent": CONFIG.user_agent}
    if referer:
        headers["Referer"] = referer

    logger.info("Fetching %s", url)
    resp = requests.get(url, headers=headers, timeout=CONFIG.http_timeout_seconds)
    status = resp.status_code
    soup = BeautifulSoup(resp.text or "", "lxml")

    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    title = soup.title.text.strip() if soup.title else ""
    text = soup.get_text(" ", strip=True)
    content_hash = hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

    return PageContent(
        url=url,
        title=title,
        text=text,
        status_code=status,
        content_hash=content_hash,
    )


def get_domain(url: str) -> Tuple[str, str]:
    import tldextract

    ext = tldextract.extract(url)
    registered = f"{ext.domain}.{ext.suffix}" if ext.suffix else ext.domain
    subdomain = ext.subdomain
    return registered, subdomain
