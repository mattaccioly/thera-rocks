from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import declarative_base, sessionmaker

from pipeline.config import CONFIG
from pipeline.storage.schemas import ScrapedPage, StartupProfile
from pipeline.utils.logging import setup_logger

logger = setup_logger(__name__)


engine = create_engine(CONFIG.database_url, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base(metadata=MetaData())


class Startup(Base):
    __tablename__ = "startups"

    id = Column(Integer, primary_key=True)
    name = Column(String(512), nullable=False, default="")
    website = Column(String(1024), nullable=True, unique=True)

    summary = Column(Text, nullable=False, default="")
    industry = Column(String(256), nullable=False, default="")
    location = Column(String(256), nullable=False, default="")
    founders = Column(JSON, nullable=False, default=list)
    funding_stage = Column(String(128), nullable=False, default="")
    last_funding_round = Column(String(128), nullable=False, default="")
    contact_email = Column(String(320), nullable=False, default="")
    links = Column(JSON, nullable=False, default=list)

    raw_notes = Column(Text, nullable=False, default="")
    raw_data = Column(JSON, nullable=False, default=dict)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Page(Base):
    __tablename__ = "pages"

    id = Column(Integer, primary_key=True)
    url = Column(String(2048), nullable=False)
    title = Column(String(1024), nullable=False, default="")
    content_text = Column(Text, nullable=False)
    content_hash = Column(String(64), nullable=False)
    http_status = Column(Integer, nullable=False)
    referer_url = Column(String(2048), nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("url", "content_hash", name="uq_url_content_hash"),
    )


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    logger.info("Database initialized at %s", CONFIG.database_url)


def upsert_startup(profile: StartupProfile) -> int:
    with SessionLocal() as session:
        now = datetime.utcnow()

        existing = None
        if profile.website:
            existing = session.query(Startup).filter(Startup.website == profile.website).one_or_none()

        if existing is None:
            record = Startup(
                name=profile.name or "",
                website=profile.website,
                summary=profile.summary or "",
                industry=profile.industry or "",
                location=profile.location or "",
                founders=profile.founders or [],
                funding_stage=profile.funding_stage or "",
                last_funding_round=profile.last_funding_round or "",
                contact_email=profile.contact_email or "",
                links=profile.links or [],
                raw_notes=profile.raw_notes or "",
                raw_data=profile.raw_data or {},
                created_at=now,
                updated_at=now,
            )
            session.add(record)
            session.commit()
            logger.info("Inserted startup id=%s website=%s", record.id, profile.website)
            return record.id
        else:
            existing.name = profile.name or existing.name
            existing.summary = profile.summary or existing.summary
            existing.industry = profile.industry or existing.industry
            existing.location = profile.location or existing.location
            existing.founders = profile.founders or existing.founders
            existing.funding_stage = profile.funding_stage or existing.funding_stage
            existing.last_funding_round = profile.last_funding_round or existing.last_funding_round
            existing.contact_email = profile.contact_email or existing.contact_email
            existing.links = profile.links or existing.links
            existing.raw_notes = (existing.raw_notes or "") + ("\n" + (profile.raw_notes or "") if profile.raw_notes else "")
            existing.raw_data = {**(existing.raw_data or {}), **(profile.raw_data or {})}
            existing.updated_at = now
            session.commit()
            logger.info("Updated startup id=%s website=%s", existing.id, profile.website)
            return existing.id


def insert_pages(pages: Iterable[ScrapedPage]) -> int:
    count = 0
    with SessionLocal() as session:
        for p in pages:
            exists = (
                session.query(Page)
                .filter(Page.url == p.url, Page.content_hash == p.content_hash)
                .one_or_none()
            )
            if exists:
                continue
            record = Page(
                url=p.url,
                title=p.title or "",
                content_text=p.content_text,
                content_hash=p.content_hash,
                http_status=p.http_status,
                referer_url=p.referer_url,
            )
            session.add(record)
            count += 1
        session.commit()
    logger.info("Inserted %d new pages", count)
    return count
