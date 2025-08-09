from __future__ import annotations

from typing import Dict, Any

from pipeline.llm.gemini_client import GeminiClient


EXTRACTION_SYSTEM_PROMPT = (
    "You extract structured startup information from website text.\n"
    "Output strictly valid JSON with keys: name (string), website (string), summary (string), industry (string),\n"
    "location (string), founders (array[string]), funding_stage (string), last_funding_round (string),\n"
    "contact_email (string), links (array[string]), raw_notes (string)."
)


def extract_startup_profile(website: str, aggregated_text: str, llm: GeminiClient | None = None) -> Dict[str, Any]:
    client = llm or GeminiClient()

    user_prompt = (
        f"WEBSITE: {website}\n"
        "Extract as much as possible from the text. If unknown, set a sensible empty value.\n"
        f"TEXT: {aggregated_text[:8000]}\n"
        "Respond with JSON only."
    )

    result = client.generate_json(EXTRACTION_SYSTEM_PROMPT, user_prompt)

    # Ensure keys exist
    result.setdefault("name", "")
    result.setdefault("website", website)
    result.setdefault("summary", "")
    result.setdefault("industry", "")
    result.setdefault("location", "")
    result.setdefault("founders", [])
    result.setdefault("funding_stage", "")
    result.setdefault("last_funding_round", "")
    result.setdefault("contact_email", "")
    result.setdefault("links", [])
    result.setdefault("raw_notes", "")

    return result
