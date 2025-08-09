from __future__ import annotations

from typing import List, Dict, Any

import pandas as pd

from pipeline.utils.logging import setup_logger

logger = setup_logger(__name__)


def snake_case(name: str) -> str:
    import re

    s = name.strip()
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_").lower()


DEFAULT_FIELD_MAP = {
    "company": "name",
    "startup": "name",
    "name": "name",
    "url": "website",
    "website": "website",
    "site": "website",
    "description": "summary",
    "summary": "summary",
    "industry": "industry",
    "sector": "industry",
    "location": "location",
    "city": "location",
    "country": "location",
    "founders": "founders",
    "founder": "founders",
    "stage": "funding_stage",
    "funding_stage": "funding_stage",
    "last_round": "last_funding_round",
    "email": "contact_email",
}


CANONICAL_FIELDS = [
    "name",
    "website",
    "summary",
    "industry",
    "location",
    "founders",
    "funding_stage",
    "last_funding_round",
    "contact_email",
]


def normalize_csv(csv_path: str, field_map: Dict[str, str] | None = None) -> List[Dict[str, Any]]:
    df = pd.read_csv(csv_path)

    df.columns = [snake_case(col) for col in df.columns]

    mapping = dict(DEFAULT_FIELD_MAP)
    if field_map:
        for k, v in field_map.items():
            mapping[snake_case(k)] = v

    canonical_rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        record: Dict[str, Any] = {k: None for k in CANONICAL_FIELDS}
        raw: Dict[str, Any] = {}
        for col, value in row.items():
            if isinstance(value, float) and pd.isna(value):
                value = None
            target = mapping.get(col)
            if target in CANONICAL_FIELDS:
                if target == "founders" and isinstance(value, str):
                    parts = [p.strip() for p in value.replace(";", ",").split(",") if p.strip()]
                    record[target] = parts
                else:
                    record[target] = value
            else:
                raw[col] = value
        record["raw_data"] = raw
        canonical_rows.append(record)

    logger.info("Normalized %d rows from %s", len(canonical_rows), csv_path)
    return canonical_rows
