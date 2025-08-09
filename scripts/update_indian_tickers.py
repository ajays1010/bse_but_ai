from __future__ import annotations

import csv
import json
import sys
import time
from pathlib import Path
from typing import List, Dict, Any

import requests


BSE_SCRIP_MASTER_URLS = [
    # Primary: Scrip Master (EQ)
    "https://api.bseindia.com/BseIndiaAPI/api/MktSMEq/newGetScripMaster/w?type=eq",
]


def fetch_bse_scrip_master(timeout: float = 20.0) -> List[Dict[str, Any]]:
    last_err = None
    for url in BSE_SCRIP_MASTER_URLS:
        try:
            r = requests.get(url, headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.bseindia.com/",
                "Accept": "application/json, text/plain, */*",
            }, timeout=timeout)
            r.raise_for_status()
            data = r.json()
            # Heuristic normalize
            if isinstance(data, dict):
                # some APIs return { "Table": [...] }
                if "Table" in data and isinstance(data["Table"], list):
                    return data["Table"]
                # or { "scrips": [...] }
                for key in ("scrips", "data", "result"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
            if isinstance(data, list):
                return data
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"Failed to fetch BSE scrip master: {last_err}")


def normalize_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for row in rows:
        # Accept variety of keys
        code = str(row.get("SC_CODE") or row.get("SCRIP_CD") or row.get("SecurityCode") or row.get("code") or "").strip()
        name = str(row.get("SC_NAME") or row.get("SecurityName") or row.get("name") or row.get("CompanyName") or "").strip()
        symbol = str(row.get("SC_ID") or row.get("SecurityId") or row.get("symbol") or row.get("SYMBOL") or "").strip()
        # Require both BSE code and a trading symbol to build a reliable NSE Yahoo symbol
        if not code or not symbol:
            continue
        # Build Yahoo symbol as <SYMBOL>.NS (per NSE list convention)
        clean_symbol = symbol.replace(" ", "")
        yahoo_symbol = f"{clean_symbol}.NS"
        out.append({
            "Yahoo Symbol": yahoo_symbol,
            "Company Name": name or symbol or code,
            "BSE Code": code,
        })
    # de-duplicate by Yahoo Symbol
    seen = set()
    unique: List[Dict[str, str]] = []
    for r in out:
        ys = r.get("Yahoo Symbol")
        if ys in seen:
            continue
        seen.add(ys)
        unique.append(r)
    return unique


def main() -> int:
    try:
        rows = fetch_bse_scrip_master()
    except Exception as e:
        print(f"Error: {e}")
        return 1
    norm = normalize_rows(rows)
    if not norm:
        print("No rows fetched.")
        return 2
    out_path = Path(__file__).resolve().parents[1] / "indian_stock_tickers.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Yahoo Symbol", "Company Name", "BSE Code"])
        writer.writeheader()
        writer.writerows(norm)
    print(f"Wrote {len(norm)} rows to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())



