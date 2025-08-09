import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class AppConfig:
    database_url: str = os.getenv("DB_URL", "sqlite:///thera_data.db")
    gemini_api_key: Optional[str] = os.getenv("GEMINI_API_KEY")
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")

    offline_mode: bool = os.getenv("OFFLINE_MODE", "false").lower() in {"1", "true", "yes", "y"}

    http_timeout_seconds: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
    user_agent: str = os.getenv("USER_AGENT", "TheraRocksBot/1.0 (+https://thera.rocks)")

    max_pages_per_domain: int = int(os.getenv("MAX_PAGES_PER_DOMAIN", "5"))
    crawl_max_depth: int = int(os.getenv("CRAWL_MAX_DEPTH", "2"))


CONFIG = AppConfig()
