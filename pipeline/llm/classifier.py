from __future__ import annotations

from typing import Tuple

from pipeline.llm.gemini_client import GeminiClient


CLASSIFIER_SYSTEM_PROMPT = (
    "You are a strict classifier that determines if a startup webpage appears legitimate or a scam.\n"
    "Return strictly valid JSON with keys: status ('real'|'scam'), confidence (0..1), reason (short)."
)


def classify_first_page(url: str, title: str, text_snippet: str, llm: GeminiClient | None = None) -> Tuple[bool, float, str]:
    client = llm or GeminiClient()

    user_prompt = (
        "Assess the following website homepage. Only use indicators visible in the text/title.\n"
        f"URL: {url}\n"
        f"TITLE: {title}\n"
        f"SNIPPET: {text_snippet[:1500]}\n"
        "Respond with JSON only."
    )

    result = client.generate_json(CLASSIFIER_SYSTEM_PROMPT, user_prompt)
    status = str(result.get("status", "real")).lower()
    confidence = float(result.get("confidence", 0.5))
    reason = str(result.get("reason", ""))
    is_real = status == "real"
    return is_real, confidence, reason

