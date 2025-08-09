from __future__ import annotations

import json
from typing import Any, Dict

from tenacity import retry, stop_after_attempt, wait_exponential

from pipeline.config import CONFIG
from pipeline.utils.logging import setup_logger

logger = setup_logger(__name__)

try:
    import google.generativeai as genai  # type: ignore
except Exception:  # pragma: no cover - optional
    genai = None  # type: ignore


class GeminiClient:
    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self.api_key = api_key or CONFIG.gemini_api_key
        self.model_name = model or CONFIG.gemini_model
        self.offline_mode = CONFIG.offline_mode

        if not self.offline_mode and (genai is None or not self.api_key):
            raise RuntimeError(
                "Gemini is required but not configured. Set GEMINI_API_KEY or enable OFFLINE_MODE=true."
            )

        if not self.offline_mode and genai is not None:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel(self.model_name)
        else:
            self.model = None

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(3))
    def generate_json(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        if self.offline_mode:
            return self._mock_response(system_prompt, user_prompt)

        assert self.model is not None  # for type checkers
        full_prompt = f"{system_prompt}\n\n{user_prompt}"
        logger.info("Calling Gemini model=%s", self.model_name)
        response = self.model.generate_content(full_prompt)
        text = response.text or "{}"
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            logger.warning("Gemini returned non-JSON, attempting to extract JSON block")
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return json.loads(text[start : end + 1])
            raise

    def _mock_response(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:  # pragma: no cover - dev helper
        if "classifier" in system_prompt.lower() or "classify" in system_prompt.lower():
            return {"status": "real", "confidence": 0.85, "reason": "Domain appears legitimate."}
        # extraction mock
        return {
            "name": "Example Startup",
            "website": "https://example.com",
            "summary": "Example Startup provides innovative solutions.",
            "industry": "Software",
            "location": "Remote",
            "founders": ["Jane Doe", "John Smith"],
            "funding_stage": "Seed",
            "last_funding_round": "2024-03",
            "contact_email": "contact@example.com",
            "links": ["https://twitter.com/example"],
            "raw_notes": "offline mock",
        }
