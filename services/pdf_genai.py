from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from google import genai
    try:
        from google.genai import types as genai_types
    except Exception:
        genai_types = None  # type: ignore
except Exception:
    # Handle namespace package issues by adding venv path
    venv_root = os.environ.get("VIRTUAL_ENV") or os.path.expanduser("~/venv")
    py_ver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = [
        os.path.join(venv_root, "lib", py_ver, "site-packages"),
        os.path.join(venv_root, "Lib", "site-packages"),
    ]
    for cand in candidates:
        if os.path.isdir(cand) and cand not in sys.path:
            sys.path.insert(0, cand)
    try:
        from google import genai  # type: ignore
        try:
            from google.genai import types as genai_types  # type: ignore
        except Exception:
            genai_types = None  # type: ignore
    except Exception:
        genai = None  # type: ignore
        genai_types = None  # type: ignore

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None  # type: ignore


@dataclass
class AppConfig:
    model_name: str = "gemini-1.5-pro"
    temperature: float = 0.3
    top_p: float = 0.95
    candidate_count: int = 1
    max_wait_seconds: int = 120
    poll_interval_seconds: float = 2.0


def require_api_key() -> str:
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        legacy = os.environ.get("GOOGLE_API_KEY", "").strip()
        if legacy:
            os.environ["GEMINI_API_KEY"] = legacy
            api_key = legacy
    if not api_key:
        print("GEMINI_API_KEY is not set.")
        sys.exit(2)
    return api_key


def guess_mime_type(file_path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(file_path))
    if guessed:
        return guessed
    if file_path.suffix.lower() == ".pdf":
        return "application/pdf"
    return "application/octet-stream"


def upload_file_with_wait(client: Any, file_path: Path, cfg: AppConfig) -> Any:
    mime = guess_mime_type(file_path)
    uploaded = client.files.upload(file=str(file_path), mime_type=mime, display_name=file_path.name)

    name = getattr(uploaded, "name", None) or getattr(uploaded, "id", None)
    start = time.time()
    while True:
        try:
            fresh = client.files.get(name=name) if name else uploaded
        except Exception:
            fresh = uploaded
        state = getattr(getattr(fresh, "state", None), "name", None) or getattr(fresh, "state", None)
        if state in ("ACTIVE", "SUCCEEDED", None):
            return fresh
        if state in ("FAILED", "ERROR"):
            raise RuntimeError(f"File processing failed with state: {state}")
        if time.time() - start > cfg.max_wait_seconds:
            raise TimeoutError("Timed out waiting for file processing.")
        time.sleep(cfg.poll_interval_seconds)


def strip_code_fences(text: str) -> str:
    code_fence_pattern = r"^\s*```[a-zA-Z]*\s*|\s*```\s*$"
    return re.sub(code_fence_pattern, "", text, flags=re.MULTILINE).strip()


def parse_json_safely(text: str) -> Dict[str, Any]:
    raw = strip_code_fences(text)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        fixed = raw.replace("\u201c", '"').replace("\u201d", '"').replace("\u2019", "'")
        fixed = re.sub(r",\s*([}\]])", r"\1", fixed)
        return json.loads(fixed)


def build_extraction_prompt(yahoo_data: dict | None = None) -> str:
    yahoo_context = ""
    if yahoo_data:
        yahoo_context = f"""

🚀 LIVE MARKET DATA (Use this in your analysis):
Company: {yahoo_data.get('company_name', 'N/A')}
Current Price: ₹{yahoo_data.get('current_price', 'N/A')}
Day Change: {yahoo_data.get('day_change_percent', 'N/A')}%
5-Day Trend: {yahoo_data.get('trend_5d', 'N/A')}
Market Cap: {yahoo_data.get('market_cap', 'N/A')}
P/E Ratio: {yahoo_data.get('pe_ratio', 'N/A')}
52W High/Low: {yahoo_data.get('52_week_high', 'N/A')}/{yahoo_data.get('52_week_low', 'N/A')}
Revenue (TTM): {yahoo_data.get('revenue_ttm', 'N/A')}
Net Income: {yahoo_data.get('net_income', 'N/A')}
Analyst Rec: {yahoo_data.get('recommendation_key', 'N/A')}
Target Price: {yahoo_data.get('target_mean_price', 'N/A')}

💡 INTEGRATE this LIVE data with the PDF announcement for SUPERIOR analysis!
"""

    return (
        "🔥 ELITE INVESTMENT ANALYST MODE 🔥\n"
        "You are the TOP stock analyst on Dalal Street - the one HNI clients call for investment advice. "
        "Your track record: 87% successful calls, ₹500Cr in client profits. "
        "You have access to LIVE market data AND web search capabilities.\n\n"
        "🌐 SEARCH THE WEB for additional context about this company and announcement if needed. "
        "Cross-reference with recent news, analyst reports, sector trends, and competitor analysis.\n\n"
        f"{yahoo_context}"
        "🎯 MISSION: Analyze this PDF + LIVE data + WEB research to give clients the ULTIMATE investment decision.\n\n"
        "EXECUTION REQUIREMENTS:\n"
        "• AGGRESSIVE analysis - no corporate speak, tell it like it is\n"
        "• INTEGRATE live market data with PDF content\n"
        "• SEARCH web for recent news/analyst updates on this company\n"
        "• Compare announcement impact vs current market price\n"
        "• Give SPECIFIC price targets and timelines\n"
        "• ALL responses ULTRA-CONCISE (max words specified)\n\n"
        "Respond ONLY with strict JSON:\n"
        "{\n"
        '  "company_name": "Company name (max 20 chars)",\n'
        '  "scrip_code": "BSE/NSE code",\n'
        '  "announcement_title": "Short title (max 40 chars)",\n'
        '  "announcement_date": "YYYY-MM-DD format",\n'
        '  "current_stock_price": "Live price from market data",\n'
        '  "price_change": "Today\'s change with % and direction",\n'
        '  "price_target": "Your 3-6 month target price",\n'
        '  "market_cap": "Current market cap",\n'
        '  "key_financials": {\n'
        '    "revenue": "Latest revenue (growth %)",\n'
        '    "net_income": "Profit with YoY change",\n'
        '    "eps": "EPS and growth trend",\n'
        '    "debt": "Debt levels and trend",\n'
        '    "cash": "Cash position strength",\n'
        '    "pe_ratio": "Current P/E vs sector avg",\n'
        '    "roe": "Return metrics"\n'
        '  },\n'
        '  "investment_recommendation": "STRONG BUY/BUY/HOLD/SELL/STRONG SELL + BRUTAL 25-word reason with price impact prediction",\n'
        '  "sentiment_analysis": "ULTRA BULLISH/BULLISH/NEUTRAL/BEARISH/ULTRA BEARISH",\n'
        '  "public_perception": "How street views this (max 20 words)",\n'
        '  "general_perception": "Your take vs consensus (max 20 words)",\n'
        '  "catalyst_impact": "How this moves stock price (max 25 words)",\n'
        '  "risk_reward": "Risk/reward ratio and key triggers (max 30 words)",\n'
        '  "motive_and_meaning": "Why company did this strategically (max 25 words)",\n'
        '  "gist": "Core news impact in plain English (max 30 words)",\n'
        '  "tldr": "Bottom line for money-making (max 20 words)",\n'
        '  "web_insights": "Key info from web search (max 30 words)",\n'
        '  "price_momentum": "Technical and fundamental momentum (max 20 words)",\n'
        '  "confidence": "1-10 confidence in this call"\n'
        "}\n\n"
        "⚡ EXECUTE WITH PRECISION: Your clients' portfolios depend on this analysis!"
    )


def build_summary_prompt() -> str:
    return (
        "Create a concise executive markdown brief of the attached finance PDF: gist, key financials, guidance, risks, opportunities, management tone, view, sentiment, one-paragraph bottom line."
    )


def get_client() -> Any:
    require_api_key()
    return genai.Client()


def generate_json_analysis_with_market_data(client: Any, file_obj: Any, cfg: AppConfig, yahoo_data: dict | None = None) -> Dict[str, Any]:
    prompt = build_extraction_prompt(yahoo_data)

    config = None
    if genai_types is not None:
        try:
            config = genai_types.GenerateContentConfig(
                temperature=cfg.temperature,
                top_p=cfg.top_p,
            )
        except Exception:
            config = None

    response = client.models.generate_content(
        model=cfg.model_name,
        contents=[file_obj, prompt],
        config=config,
    )
    text = getattr(response, "text", None) or "{}"
    return parse_json_safely(text)


def generate_json_analysis(client: Any, file_obj: Any, cfg: AppConfig) -> Dict[str, Any]:
    return generate_json_analysis_with_market_data(client, file_obj, cfg, None)


def generate_markdown_summary(client: Any, file_obj: Any, cfg: AppConfig) -> str:
    prompt = build_summary_prompt()

    config = None
    if genai_types is not None:
        try:
            config = genai_types.GenerateContentConfig(
                temperature=cfg.temperature,
                top_p=cfg.top_p,
            )
        except Exception:
            config = None

    response = client.models.generate_content(
        model=cfg.model_name,
        contents=[file_obj, prompt],
        config=config,
    )
    return (getattr(response, "text", "") or "").strip()


def analyze_pdf_with_market_data(pdf_path: Path, cfg: AppConfig, yahoo_data: dict | None = None) -> tuple[Dict[str, Any], str]:
    client = get_client()
    try:
        file_obj = upload_file_with_wait(client, pdf_path, cfg)
        json_data = generate_json_analysis_with_market_data(client, file_obj, cfg, yahoo_data)
        md_summary = generate_markdown_summary(client, file_obj, cfg)
        return json_data, md_summary
    except Exception:
        plain_text = fallback_extract_text(pdf_path)
        if not plain_text:
            raise

        config = None
        if genai_types is not None:
            try:
                config = genai_types.GenerateContentConfig(
                    temperature=cfg.temperature,
                    top_p=cfg.top_p,
                )
            except Exception:
                config = None

        json_resp = client.models.generate_content(
            model=cfg.model_name,
            contents=[plain_text, build_extraction_prompt(yahoo_data)],
            config=config,
        )
        md_resp = client.models.generate_content(
            model=cfg.model_name,
            contents=[plain_text, build_summary_prompt()],
            config=config,
        )
        return parse_json_safely(getattr(json_resp, "text", "{}") or "{}"), (getattr(md_resp, "text", "") or "").strip()


def analyze_pdf(pdf_path: Path, cfg: AppConfig) -> tuple[Dict[str, Any], str]:
    return analyze_pdf_with_market_data(pdf_path, cfg, None)


def fallback_extract_text(pdf_path: Path, max_chars: int = 40000) -> Optional[str]:
    if PdfReader is None:
        return None
    try:
        reader = PdfReader(str(pdf_path))
        texts = []
        for page in reader.pages:
            try:
                texts.append(page.extract_text() or "")
            except Exception:
                continue
        combined = "\n\n".join(texts).strip()
        if not combined:
            return None
        if len(combined) > max_chars:
            combined = combined[:max_chars]
        return combined
    except Exception:
        return None


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze a finance PDF using Gemini and produce JSON + summary.")
    parser.add_argument("--pdf", required=True, help="Absolute path to the finance PDF")
    parser.add_argument("--model", default="gemini-1.5-pro", help="Gemini model name")
    parser.add_argument("--temperature", type=float, default=0.3)
    parser.add_argument("--top_p", type=float, default=0.95)
    args = parser.parse_args(argv)

    pdf_path = Path(args.pdf).expanduser().resolve()
    if not pdf_path.exists() or not pdf_path.is_file():
        print(f"PDF not found: {pdf_path}")
        return 2
    if not str(pdf_path).lower().endswith(".pdf"):
        print("Warning: input does not look like a PDF.")

    cfg = AppConfig(model_name=args.model, temperature=args.temperature, top_p=args.top_p)

    try:
        json_data, md_summary = analyze_pdf(pdf_path, cfg)
    except Exception as e:
        print(f"Error: {e}")
        return 1

    json_out = pdf_path.with_suffix("")
    json_out = json_out.parent / f"{json_out.name}_analysis.json"
    md_out = pdf_path.with_suffix("")
    md_out = md_out.parent / f"{md_out.name}_summary.md"
    json_out.write_text(json.dumps(json_data, indent=2, ensure_ascii=False))
    md_out.write_text(md_summary)
    print(f"Saved JSON: {json_out}\nSaved Summary: {md_out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
