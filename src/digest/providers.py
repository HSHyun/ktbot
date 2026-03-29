from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any

import requests


GEMINI_API_BASE = "https://generativelanguage.googleapis.com"
GEMINI_DIGEST_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "issues": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "One-line Korean headline for a key issue.",
                    },
                    "summary": {
                        "type": "string",
                        "description": "Concise Korean summary in 2 to 4 sentences.",
                    },
                },
                "required": ["title", "summary"],
            },
            "description": "List of 3 to 5 key issues extracted from the dataset.",
        }
    },
    "required": ["issues"],
}


@dataclass(frozen=True)
class DigestModelConfig:
    provider: str
    model_name: str


def resolve_digest_model(hours: int) -> DigestModelConfig:
    if hours <= 0:
        raise RuntimeError(f"hours must be positive: {hours}")
    if hours == 6:
        return DigestModelConfig(
            provider="groq",
            model_name=(os.getenv("GROQ_DIGEST_MODEL") or "openai/gpt-oss-120b").strip(),
        )
    return DigestModelConfig(
        provider="gemini",
        model_name=(os.getenv("GEMINI_DIGEST_MODEL") or "gemini-2.5-flash").strip(),
    )


def required_gemini_api_key() -> str:
    for name in ("GEMINI_API_KEY", "GEMINI_API_KEY2"):
        value = (os.getenv(name) or "").strip()
        if value:
            return value
    raise RuntimeError("Missing GEMINI_API_KEY in environment.")


def summarise_with_gemini(prompt: str, model_name: str) -> dict[str, Any]:
    api_key = required_gemini_api_key()
    resp = requests.post(
        f"{GEMINI_API_BASE}/v1beta/models/{model_name}:generateContent",
        params={"key": api_key},
        headers={"Content-Type": "application/json"},
        json={
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": {
                "temperature": 0.2,
                "maxOutputTokens": 8192,
                "responseMimeType": "application/json",
                "responseJsonSchema": GEMINI_DIGEST_RESPONSE_SCHEMA,
            },
        },
        timeout=120,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Gemini API returned status {resp.status_code}: {_extract_error_message(resp)}"
        )

    try:
        payload = resp.json()
    except ValueError as exc:
        raise RuntimeError("Invalid JSON from Gemini API.") from exc

    text = _extract_text(payload)
    if not text:
        raise RuntimeError("Gemini returned no digest text.")

    return parse_issues_json(text)


def _extract_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        return ""
    for candidate in candidates:
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        texts: list[str] = []
        for part in parts:
            if not isinstance(part, dict):
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                texts.append(text.strip())
        if texts:
            return "\n".join(texts).strip()
    return ""


def _extract_error_message(resp: requests.Response) -> str:
    try:
        payload = resp.json()
    except ValueError:
        return resp.text.strip()
    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        status = error.get("status")
        if message and status:
            return f"{status}: {message}"
        if message:
            return str(message)
    return json.dumps(payload, ensure_ascii=False)


def parse_issues_json(raw: str) -> dict[str, Any]:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 3:
            text = "\n".join(lines[1:-1]).strip()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Model output is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("Model JSON must be an object.")
    issues = payload.get("issues")
    if not isinstance(issues, list):
        raise RuntimeError("Model JSON must include 'issues' array.")
    cleaned: list[dict[str, str]] = []
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        title = str(issue.get("title") or "").strip()
        summary = str(issue.get("summary") or "").strip()
        if not title or not summary:
            continue
        cleaned.append({"title": title, "summary": summary})
    if not cleaned:
        raise RuntimeError("Model JSON returned no valid issues.")
    payload["issues"] = cleaned
    return payload
