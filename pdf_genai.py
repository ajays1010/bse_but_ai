#!/usr/bin/env python3
"""
Finance PDF Analyzer using Gemini Generative API (pure Python CLI)

Features
- Accepts a finance-related PDF and uploads it via Gemini File API
- Extracts structured financial data as strict JSON
- Generates a consolidated, plain-English summary with an investment view
- Saves results next to the input file as `<name>_analysis.json` and `<name>_summary.md`

Prerequisites
- Set GOOGLE_API_KEY in your environment
    export GOOGLE_API_KEY="your_api_key"

Usage
    python pdf_genai.py --pdf /path/to/report.pdf

Dependencies (see requirements.txt)
- google-generativeai
- pypdf (fallback text extraction if upload fails)
- rich (pretty CLI; optional)
"""

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
    from rich.console import Console
    from rich.panel import Panel
    from rich.progress import Progress, SpinnerColumn, TextColumn
    from rich.table import Table
except Exception:  # pragma: no cover - rich is optional
    Console = None  # type: ignore
    Panel = None  # type: ignore
    Progress = None  # type: ignore
    SpinnerColumn = None  # type: ignore
    TextColumn = None  # type: ignore
    Table = None  # type: ignore

try:
    # New SDK per quickstart: https://ai.google.dev/gemini-api/docs/quickstart?lang=python
    from google import genai as genai
    try:
        from google.genai import types as genai_types  # optional, used for configs if available
    except Exception:
        genai_types = None  # type: ignore
except Exception:
    # Try to add common venv site-packages if running under a constrained interpreter
    venv_root = os.environ.get("VIRTUAL_ENV") or os.path.expanduser("~/venv")
    py_ver = f"python{sys.version_info.major}.{sys.version_info.minor}"
    candidates = [
        os.path.join(venv_root, "lib", py_ver, "site-packages"),
        os.path.join(venv_root, "Lib", "site-packages"),  # Windows-style just in case
    ]
    for cand in candidates:
        if os.path.isdir(cand) and cand not in sys.path:
            sys.path.insert(0, cand)
    from google import genai as genai  # type: ignore
    try:
        from google.genai import types as genai_types  # type: ignore
    except Exception:
        genai_types = None  # type: ignore

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover - optional fallback
    PdfReader = None  # type: ignore


def _console() -> Any:
    if Console is None:
        class _Dummy:
            def print(self, *a, **k):
                print(*a)
        return _Dummy()
    return Console()


console = _console()


@dataclass
class AppConfig:
    model_name: str = "gemini-1.5-pro"
    temperature: float = 0.3
    top_p: float = 0.95
    candidate_count: int = 1
    max_wait_seconds: int = 120
    poll_interval_seconds: float = 2.0


def require_api_key() -> str:
    # Prefer new env var per quickstart
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        legacy = os.environ.get("GOOGLE_API_KEY", "").strip()
        if legacy:
            os.environ["GEMINI_API_KEY"] = legacy
            api_key = legacy
    if not api_key:
        console.print("[red]GEMINI_API_KEY is not set. Please export it and retry.[/red]")
        console.print("Example: export GEMINI_API_KEY=\"your_api_key\"")
        sys.exit(2)
    return api_key


def guess_mime_type(file_path: Path) -> str:
    guessed, _ = mimetypes.guess_type(str(file_path))
    if guessed:
        return guessed
    # Fallback for unknown
    if file_path.suffix.lower() == ".pdf":
        return "application/pdf"
    return "application/octet-stream"


def upload_file_with_wait(client: Any, file_path: Path, cfg: AppConfig) -> Any:
    mime = guess_mime_type(file_path)
    console.print(f"Uploading file: [bold]{file_path.name}[/bold] ({mime})")
    # Prefer binary handle to avoid large path handling issues
    with open(file_path, "rb") as fh:
        uploaded = client.files.upload(file=fh, mime_type=mime, display_name=file_path.name)  # type: ignore

    # Wait until processing completes if the SDK exposes state
    start = time.time()
    name = getattr(uploaded, "name", None) or getattr(uploaded, "id", None)
    while True:
        try:
            fresh = client.files.get(name=name) if name else uploaded  # type: ignore
        except Exception:
            fresh = uploaded
        state = getattr(getattr(fresh, "state", None), "name", None) or getattr(fresh, "state", None)
        if state in ("ACTIVE", "SUCCEEDED", None):
            console.print("[green]File is ready.[/green]")
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
        # Attempt to fix common trailing commas and smart quotes
        fixed = raw.replace("\u201c", '"').replace("\u201d", '"').replace("\u2019", "'")
        fixed = re.sub(r",\s*([}\]])", r"\1", fixed)
        return json.loads(fixed)


def build_extraction_prompt() -> str:
    return (
        "You are a finance analyst. Analyze the attached PDF (earnings report, investor presentation, filing, or research).\n"
        "Extract a concise but complete snapshot. Respond ONLY with strict JSON matching this schema: \n\n"
        "{\n"
        "  \"document_metadata\": {\"title\": string, \"date\": string|null, \"company\": string|null, \"tickers\": string[]},\n"
        "  \"gist\": string,\n"
        "  \"motive\": string,\n"
        "  \"time_period\": string|null,\n"
        "  \"key_financials\": {\n"
        "    \"revenue\": string|null, \"gross_margin\": string|null, \"operating_income\": string|null, \"ebitda\": string|null,\n"
        "    \"net_income\": string|null, \"eps\": string|null, \"cash_flow\": string|null, \"capex\": string|null, \"debt\": string|null, \"cash\": string|null\n"
        "  },\n"
        "  \"segments\": [{\"name\": string, \"performance\": string}] ,\n"
        "  \"guidance\": string|null,\n"
        "  \"risk_factors\": string[],\n"
        "  \"opportunities\": string[],\n"
        "  \"management_tone\": string,\n"
        "  \"valuation_notes\": string|null,\n"
        "  \"stock_recommendation\": {\n"
        "     \"rating\": one of [\"invest\", \"hold\", \"avoid\"],\n"
        "     \"rationale\": string,\n"
        "     \"catalysts\": string[],\n"
        "     \"risks\": string[],\n"
        "     \"time_horizon\": string\n"
        "  },\n"
        "  \"predicted_sentiment\": one of [\"bullish\", \"neutral\", \"bearish\"],\n"
        "  \"confidence\": number (0-1)\n"
        "}\n\n"
        "Rules: Be factual to the PDF; avoid hallucination; no external data. Use strings for numbers if units/period needed."
    )


def build_summary_prompt() -> str:
    return (
        "Create a concise, executive-style markdown brief of the attached finance PDF.\n"
        "Include:\n"
        "- Gist and motive\n"
        "- Key financials with units/timeframes\n"
        "- Guidance, risks, opportunities\n"
        "- Management tone\n"
        "- Clear stock view (Invest/Hold/Avoid) with rationale and horizon\n"
        "- Short predictive note consistent with the PDF (no external data)\n"
        "- One-paragraph bottom line\n\n"
        "Write for a PM. Keep it crisp."
    )


def get_client() -> Any:
    # Client reads GEMINI_API_KEY from environment per quickstart
    require_api_key()
    return genai.Client()


def generate_json_analysis(client: Any, file_obj: Any, cfg: AppConfig) -> Dict[str, Any]:
    prompt = build_extraction_prompt()
    # Optional: configure thinking to 0 for speed if available
    config = None
    if genai_types is not None:
        try:
            config = genai_types.GenerateContentConfig(
                temperature=cfg.temperature,
                top_p=cfg.top_p,
                thinking_config=genai_types.ThinkingConfig(thinking_budget=0),  # speed
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


def generate_markdown_summary(client: Any, file_obj: Any, cfg: AppConfig) -> str:
    prompt = build_summary_prompt()
    config = None
    if genai_types is not None:
        try:
            config = genai_types.GenerateContentConfig(
                temperature=cfg.temperature,
                top_p=cfg.top_p,
                thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
            )
        except Exception:
            config = None
    response = client.models.generate_content(
        model=cfg.model_name,
        contents=[file_obj, prompt],
        config=config,
    )
    return (getattr(response, "text", "") or "").strip()


def save_outputs(base_path: Path, json_data: Dict[str, Any], markdown_summary: str) -> tuple[Path, Path]:
    json_path = base_path.with_suffix("")
    json_path = json_path.parent / f"{json_path.name}_analysis.json"
    md_path = base_path.with_suffix("")
    md_path = md_path.parent / f"{md_path.name}_summary.md"
    json_path.write_text(json.dumps(json_data, indent=2, ensure_ascii=False))
    md_path.write_text(markdown_summary)
    return json_path, md_path


def fallback_extract_text(pdf_path: Path, max_chars: int = 40000) -> Optional[str]:  # Best-effort only
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


def analyze_pdf(pdf_path: Path, cfg: AppConfig) -> tuple[Dict[str, Any], str]:
    # Client init (reads GEMINI_API_KEY)
    client = get_client()

    # Upload via Files API; fallback to raw text if needed
    try:
        file_obj = upload_file_with_wait(client, pdf_path, cfg)
        json_data = generate_json_analysis(client, file_obj, cfg)
        md_summary = generate_markdown_summary(client, file_obj, cfg)
        return json_data, md_summary
    except Exception as upload_error:
        console.print(f"[yellow]Upload/processing failed, attempting text fallback: {upload_error}[/yellow]")
        plain_text = fallback_extract_text(pdf_path)
        if not plain_text:
            raise
        # Use plain text as content
        config = None
        if genai_types is not None:
            try:
                config = genai_types.GenerateContentConfig(
                    temperature=cfg.temperature,
                    top_p=cfg.top_p,
                    thinking_config=genai_types.ThinkingConfig(thinking_budget=0),
                )
            except Exception:
                config = None
        json_resp = client.models.generate_content(
            model=cfg.model_name,
            contents=[plain_text, build_extraction_prompt()],
            config=config,
        )
        md_resp = client.models.generate_content(
            model=cfg.model_name,
            contents=[plain_text, build_summary_prompt()],
            config=config,
        )
        return parse_json_safely(getattr(json_resp, "text", "{}") or "{}"), (getattr(md_resp, "text", "") or "").strip()


def print_brief(json_data: Dict[str, Any]) -> None:
    try:
        doc = json_data.get("document_metadata", {})
        rec = json_data.get("stock_recommendation", {})
        sentiment = json_data.get("predicted_sentiment")
        confidence = json_data.get("confidence")
        table = Table(title="Finance PDF Analysis (brief)") if Table else None
        if table:
            table.add_column("Field")
            table.add_column("Value")
            table.add_row("Company", str(doc.get("company")))
            table.add_row("Tickers", ", ".join(doc.get("tickers", [])))
            table.add_row("Date", str(doc.get("date")))
            table.add_row("Rating", str(rec.get("rating")))
            table.add_row("Sentiment", str(sentiment))
            table.add_row("Confidence", str(confidence))
            console.print(table)
        else:
            console.print(f"Company: {doc.get('company')} | Tickers: {', '.join(doc.get('tickers', []))}")
            console.print(f"Rating: {rec.get('rating')} | Sentiment: {sentiment} | Confidence: {confidence}")
    except Exception:
        pass


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Analyze a finance PDF using Gemini and produce JSON + summary.")
    parser.add_argument("--pdf", required=True, help="Absolute path to the finance PDF")
    parser.add_argument("--model", default="gemini-2.5-flash", help="Gemini model name (default: gemini-2.5-flash)")
    parser.add_argument("--temperature", type=float, default=0.3, help="Sampling temperature")
    parser.add_argument("--top_p", type=float, default=0.95, help="Top-p nucleus sampling")
    parser.add_argument("--no-pretty", action="store_true", help="Disable pretty console output")
    args = parser.parse_args(argv)

    pdf_path = Path(args.pdf).expanduser().resolve()
    if not pdf_path.exists() or not pdf_path.is_file():
        console.print(f"[red]PDF not found: {pdf_path}[/red]")
        return 2
    if not str(pdf_path).lower().endswith(".pdf"):
        console.print("[yellow]Warning: input does not look like a PDF.[/yellow]")

    cfg = AppConfig(
        model_name=args.model,
        temperature=args.temperature,
        top_p=args.top_p,
    )

    if args.no_pretty or Console is None:
        try:
            json_data, md_summary = analyze_pdf(pdf_path, cfg)
        except Exception as e:
            print(f"Error: {e}")
            return 1
    else:
        with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), transient=True) as progress:
            progress.add_task(description="Analyzing PDF with Gemini...", total=None)
            try:
                json_data, md_summary = analyze_pdf(pdf_path, cfg)
            except Exception as e:
                console.print(Panel.fit(str(e), title="Error", border_style="red"))
                return 1

    json_path, md_path = save_outputs(pdf_path, json_data, md_summary)

    if Panel:
        console.print(Panel.fit(f"Saved JSON: {json_path}\nSaved Summary: {md_path}", title="Outputs", border_style="green"))
    else:
        console.print(f"Saved JSON: {json_path}")
        console.print(f"Saved Summary: {md_path}")

    print_brief(json_data)

    console.print("\n[dim]Note: This analysis is model-generated from the PDF only and is not financial advice.[/dim]")
    return 0


if __name__ == "__main__":
    sys.exit(main())


