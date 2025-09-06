from __future__ import annotations

from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field


class StartupProfile(BaseModel):
    name: str = ""
    website: Optional[str] = None
    summary: str = ""
    industry: str = ""
    location: str = ""
    founders: List[str] = Field(default_factory=list)
    funding_stage: str = ""
    last_funding_round: str = ""
    contact_email: str = ""
    links: List[str] = Field(default_factory=list)
    raw_notes: str = ""
    raw_data: Dict[str, Any] = Field(default_factory=dict)


class ScrapedPage(BaseModel):
    url: str
    title: str = ""
    content_text: str
    content_hash: str
    http_status: int
    referer_url: Optional[str] = None





