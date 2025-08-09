from __future__ import annotations

from typing import Any, Dict, Optional
from .logger import log_message


def fetch_market_data_via_gemini(scrip_code: str, company_name: Optional[str] = None, yahoo_symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Fallback: use Gemini with web access to fetch live market data when Yahoo fails.

    Returns a dict like:
      {
        current_price, previous_close, day_change_percent, market_cap, company_name
      }
    or None on failure.
    """
    try:
        import os
        if not (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
            return None

        # Lazily import pdf_genai to reuse client + robust JSON parsing
        try:
            from . import pdf_genai as pdf_ai  # type: ignore
        except Exception:
            pdf_ai = None  # type: ignore
        if pdf_ai is None:
            return None

        client = pdf_ai.get_client()
        model_name = os.environ.get("GEMINI_MODEL", "gemini-1.5-pro")

        prompt = (
            "You are a market data assistant with web access. "
            "Find the most recent live price and market data for the given Indian stock. "
            "Prefer NSE figures. Return strict JSON with keys: current_price (number), previous_close (number), "
            "day_change_percent (number), market_cap (string), company_name (string). If unknown, use null."
        )

        resp = client.models.generate_content(
            model=model_name,
            contents=[
                prompt,
                f"Context: scrip_code={scrip_code}, company_name={company_name}, yahoo_symbol={yahoo_symbol}.",
                "Respond ONLY with strict JSON and no extra text.",
            ],
        )
        text = getattr(resp, "text", "{}") or "{}"

        try:
            from .pdf_genai import parse_json_safely  # type: ignore
            data = parse_json_safely(text)
        except Exception:
            import json as _json
            data = _json.loads(text)

        return {
            "current_price": data.get("current_price"),
            "previous_close": data.get("previous_close"),
            "day_change_percent": data.get("day_change_percent"),
            "market_cap": data.get("market_cap"),
            "company_name": data.get("company_name") or company_name,
        }
    except Exception as e:
        log_message(f"[AI] Gemini web fallback failed: {e}")
        return None


