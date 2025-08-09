from typing import Optional, Dict, Any
import tempfile
from pathlib import Path
from .logger import log_message
from .yahoo_finance_service import get_stock_data_yahoo, get_stock_recommendation_yahoo


try:
    from . import pdf_genai as pdf_ai
except Exception:
    pdf_ai = None  # type: ignore


def analyze_pdf_bytes_with_gemini(pdf_bytes: bytes, pdf_name: str, scrip_code: str = None) -> Optional[Dict[str, Any]]:
    from .memory_guard import should_allow_ai, get_process_memory_mb

    if pdf_ai is None:
        log_message("[AI] pdf_genai module not available; skipping AI analysis.")
        return None
    import os
    if not (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
        log_message("[AI] GEMINI_API_KEY not set; skipping AI analysis.")
        return None
    try:
        # Memory gating for low-resource deployments
        if not should_allow_ai():
            current_mb = get_process_memory_mb()
            log_message(f"[AI] Skipping AI analysis due to low-memory mode (RSS ~{current_mb:.1f}MB)")
            return None
        # Fetch live market data from Yahoo Finance
        yahoo_data = None
        if scrip_code:
            log_message(f"[AI] Fetching live market data for {scrip_code}")
            yahoo_data = get_stock_data_yahoo(scrip_code)
            if yahoo_data and not yahoo_data.get('error'):
                log_message(f"[AI] Got live data: Price ₹{yahoo_data.get('current_price', 'N/A')}, Change {yahoo_data.get('day_change_percent', 'N/A')}%")
        
        client = pdf_ai.get_client()
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(pdf_bytes)
            tmp_path = Path(tmp.name)
        try:
            cfg = pdf_ai.AppConfig(model_name=os.environ.get("GEMINI_MODEL", "gemini-1.5-pro"), temperature=0.2, top_p=0.9)
            # Pass Yahoo Finance data to the enhanced analysis
            json_data, _ = pdf_ai.analyze_pdf_with_market_data(tmp_path, cfg, yahoo_data)  # type: ignore

            # Enrich AI JSON with Yahoo live data when AI fields are missing/NA
            if json_data is None:
                json_data = {}
            if yahoo_data and not yahoo_data.get('error'):
                # Current price
                ai_price = str(json_data.get('current_stock_price') or '').strip()
                if ai_price in ('', 'N/A', None):
                    live_price = yahoo_data.get('current_price')
                    if live_price not in (None, 'N/A'):
                        json_data['current_stock_price'] = str(live_price)
                # Price change
                ai_change = str(json_data.get('price_change') or '').strip()
                if ai_change == '':
                    pct = yahoo_data.get('day_change_percent')
                    chg = yahoo_data.get('day_change')
                    if pct not in (None, 'N/A'):
                        json_data['price_change'] = f"{pct}%" if isinstance(pct, (int, float)) else str(pct)
                    elif chg not in (None, 'N/A'):
                        json_data['price_change'] = str(chg)
                # Market cap
                if not json_data.get('market_cap') and yahoo_data.get('market_cap'):
                    json_data['market_cap'] = yahoo_data.get('market_cap')
                # Company name (fallback)
                if not json_data.get('company_name') and yahoo_data.get('company_name'):
                    json_data['company_name'] = yahoo_data.get('company_name')
            
            # Enhance with Yahoo Finance recommendation if available
            if json_data and yahoo_data and not yahoo_data.get('error'):
                yahoo_rec = get_stock_recommendation_yahoo(yahoo_data)
                if yahoo_rec:
                    # Combine AI recommendation with Yahoo data insights
                    ai_rec = json_data.get('investment_recommendation', 'HOLD')
                    json_data['market_recommendation'] = yahoo_rec
                    json_data['combined_analysis'] = f"AI: {ai_rec} | Market: {yahoo_rec}"
            
            return json_data or None
        finally:
            try:
                tmp_path.unlink(missing_ok=True)  # type: ignore[arg-type]
            except Exception:
                pass
    except Exception as e:
        log_message(f"[AI] Analysis failed: {e}")
        return None


def fetch_market_data_via_gemini(scrip_code: str, company_name: Optional[str] = None, yahoo_symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Fallback: ask Gemini (with web search capability) for live market data when Yahoo fails.

    Returns a dict like { current_price, previous_close, day_change_percent, market_cap, company_name } or None.
    """
    try:
        import os
        if not (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
            return None
        # Build a concise instruction to fetch live price via web search
        prompt = (
            "You are a market data assistant with web access. "
            "Find the most recent live price and market data for the given Indian stock. "
            "Prefer NSE figures. Return strict JSON with keys: current_price (number), previous_close (number), day_change_percent (number), market_cap (string), company_name (string). "
            "If unknown, use null."
        )
        query = {
            "scrip_code": scrip_code or "",
            "company_name": company_name or "",
            "yahoo_symbol": yahoo_symbol or "",
        }
        try:
            from . import pdf_genai as pdf_ai
        except Exception:
            pdf_ai = None  # type: ignore
        if pdf_ai is None:
            return None
        client = pdf_ai.get_client()
        model_name = os.environ.get("GEMINI_MODEL", "gemini-1.5-pro")
        # Ask the model to search and reply with pure JSON
        resp = client.models.generate_content(
            model=model_name,
            contents=[
                prompt,
                f"Context: scrip_code={scrip_code}, company_name={company_name}, yahoo_symbol={yahoo_symbol}.",
                "Respond ONLY with strict JSON and no extra text.",
            ],
        )
        text = getattr(resp, "text", "{}") or "{}"
        # Reuse robust JSON parsing helper
        try:
            from .pdf_genai import parse_json_safely  # type: ignore
        except Exception:
            import json as _json
            return _json.loads(text)
        data = parse_json_safely(text)
        # Basic normalize
        out: Dict[str, Any] = {
            "current_price": data.get("current_price"),
            "previous_close": data.get("previous_close"),
            "day_change_percent": data.get("day_change_percent"),
            "market_cap": data.get("market_cap"),
            "company_name": data.get("company_name") or company_name,
        }
        return out
    except Exception as e:
        log_message(f"[AI] Gemini web fallback failed: {e}")
        return None


def _html_escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def build_consolidated_section(analysis: Dict[str, Any]) -> str:
    try:
        doc = analysis.get("document_metadata") or {}
        rec = analysis.get("stock_recommendation") or {}
        key = analysis.get("key_financials") or {}
        mkt = analysis.get("market_data") or {}
        company = doc.get("company") or ""
        tickers = ", ".join(doc.get("tickers") or [])
        date = doc.get("date") or ""
        rating = (rec.get("rating") or "").title()
        sentiment = (analysis.get("predicted_sentiment") or "").title()
        price = analysis.get("stock_price") or mkt.get("price") or mkt.get("last") or ""
        chg = analysis.get("price_change") or mkt.get("change") or ""
        chg_pct = analysis.get("price_change_pct") or mkt.get("change_percent") or mkt.get("changePct") or ""
        market_cap = analysis.get("market_cap") or mkt.get("market_cap") or ""
        eps = key.get("eps") or ""
        revenue = key.get("revenue") or ""
        ebitda = key.get("ebitda") or ""
        op_inc = key.get("operating_income") or ""
        net_inc = key.get("net_income") or ""
        cash_flow = key.get("cash_flow") or ""
        debt = key.get("debt") or ""
        cash = key.get("cash") or ""
        brief_lines = []
        if company or tickers:
            brief_lines.append(f"🏢 <b>{_html_escape(company)}</b> {('('+_html_escape(tickers)+')') if tickers else ''}")
        if date:
            brief_lines.append(f"🗓️ {_html_escape(str(date))}")
        market_bits = []
        if price:
            market_bits.append(f"₹{_html_escape(str(price))}")
        if chg or chg_pct:
            delta = []
            if chg:
                delta.append(str(chg))
            if chg_pct:
                delta.append(str(chg_pct))
            arrow = ""
            try:
                val = float(str(chg_pct).replace('%','').replace('+','')) if chg_pct else float(str(chg).replace('+',''))
                arrow = "📈" if val >= 0 else "📉"
            except Exception:
                arrow = ""
            market_bits.append(arrow + (" "+" / ".join(delta) if delta else ""))
        if market_cap:
            market_bits.append(f"MktCap {_html_escape(str(market_cap))}")
        if market_bits:
            brief_lines.append("💹 " + "  |  ".join([_html_escape(x).strip() for x in market_bits if x]))
        if rating:
            brief_lines.append(f"📊 <b>View:</b> {_html_escape(rating)}")
        if sentiment:
            brief_lines.append(f"🤖 <b>Sentiment:</b> {_html_escape(sentiment)}")
        perf_bits = []
        if revenue: perf_bits.append(f"Revenue { _html_escape(revenue) }")
        if op_inc: perf_bits.append(f"OpInc { _html_escape(op_inc) }")
        if ebitda: perf_bits.append(f"EBITDA { _html_escape(ebitda) }")
        if net_inc: perf_bits.append(f"NetInc { _html_escape(net_inc) }")
        if eps: perf_bits.append(f"EPS { _html_escape(eps) }")
        if cash_flow: perf_bits.append(f"CFO { _html_escape(cash_flow) }")
        if debt: perf_bits.append(f"Debt { _html_escape(debt) }")
        if cash: perf_bits.append(f"Cash { _html_escape(cash) }")
        if perf_bits:
            brief_lines.append("💰 " + "; ".join(perf_bits))
        risks = (rec.get("risks") or analysis.get("risk_factors") or [])
        catalysts = (rec.get("catalysts") or [])
        if isinstance(risks, list) and risks:
            brief_lines.append("⚠️ " + _truncate(", ".join(risks[:2]), 160))
        if isinstance(catalysts, list) and catalysts:
            brief_lines.append("🚀 " + _truncate(", ".join(catalysts[:2]), 160))
        return "\n" + "\n".join(brief_lines) if brief_lines else ""
    except Exception:
        return ""


def build_tldr(analysis: Dict[str, Any]) -> str:
    try:
        gist = analysis.get("gist") or ""
        if not gist:
            return ""
        return "\n📝 <b>TL;DR:</b> " + _html_escape(_truncate(str(gist), 200))
    except Exception:
        return ""


def format_structured_telegram_message(analysis: Dict[str, Any], scrip_code: str, announcement_title: str, ann_date_ist) -> str:
    """Format the Telegram message according to the requested structure"""
    from datetime import datetime
    
    try:
        # Extract data from AI analysis
        company_name = analysis.get("company_name", "N/A")
        ai_scrip_code = analysis.get("scrip_code", scrip_code)  # Use AI extracted or fallback to BSE code
        stock_price = analysis.get("current_stock_price", "N/A")
        price_change = analysis.get("price_change", "")
        ai_title = analysis.get("announcement_title", announcement_title)
        
        # Format date as DD/MM/YY HH:MM AM/PM
        if ann_date_ist:
            formatted_date = ann_date_ist.strftime("%d/%m/%y %I:%M %p")
        else:
            formatted_date = "N/A"
        
        # Build price display with Yahoo fallback if AI missed it
        price_display = stock_price
        if (not price_display) or price_display == "N/A":
            try:
                live = get_stock_data_yahoo(scrip_code)
                live_price = live.get('current_price') if isinstance(live, dict) else None
                if live_price not in (None, 'N/A'):
                    price_display = str(live_price)
                    # If AI didn't provide change, use Yahoo's
                    if (not price_change) or price_change == "N/A" or price_change == "":
                        pct = live.get('day_change_percent') if isinstance(live, dict) else None
                        if pct not in (None, 'N/A'):
                            price_change = f"{pct}%" if isinstance(pct, (int, float)) else str(pct)
            except Exception:
                pass
        if price_change and price_change != "N/A":
            price_display = f"{price_display} ({price_change})" if price_display else price_change
        
        # Extract financial analysis
        key_financials = analysis.get("key_financials", {})
        investment_rec = analysis.get("investment_recommendation", "N/A")
        price_target = analysis.get("price_target", "N/A")
        sentiment = analysis.get("sentiment_analysis", "N/A")
        public_perception = analysis.get("public_perception", "N/A")
        general_perception = analysis.get("general_perception", "N/A")
        catalyst_impact = analysis.get("catalyst_impact", "N/A")
        risk_reward = analysis.get("risk_reward", "N/A")
        web_insights = analysis.get("web_insights", "N/A")
        price_momentum = analysis.get("price_momentum", "N/A")
        motive = analysis.get("motive_and_meaning", "N/A")
        tldr = analysis.get("tldr", "N/A")
        
        # Enhance investment recommendation with stronger language
        if investment_rec and investment_rec != "N/A":
            investment_rec = investment_rec.upper()  # Make recommendation bold and uppercase
            
        # Add price target to display if available
        if price_target and price_target != "N/A":
            investment_rec += f" | Target: ₹{price_target}"
        
        # Build financial summary
        financials_parts = []
        if isinstance(key_financials, dict):
            for key, value in key_financials.items():
                if value and value != "N/A":
                    financials_parts.append(f"{key.title()}: {value}")
        
        financial_summary = "; ".join(financials_parts) if financials_parts else "Financial data not available"
        
        # Construct the message with length limits
        def truncate_field(text, max_len):
            if len(str(text)) <= max_len:
                return str(text)
            return str(text)[:max_len-3] + "..."
        
        # Truncate long fields to fit within Telegram limits
        company_display = truncate_field(company_name, 35)
        title_display = truncate_field(ai_title, 60)
        financial_display = truncate_field(financial_summary, 100)
        investment_display = truncate_field(investment_rec, 80)
        sentiment_display = truncate_field(sentiment, 15)
        public_display = truncate_field(public_perception, 60)
        general_display = truncate_field(general_perception, 60)
        catalyst_display = truncate_field(catalyst_impact, 70)
        momentum_display = truncate_field(price_momentum, 60)
        web_display = truncate_field(web_insights, 70)
        tldr_display = truncate_field(tldr, 80)
        
        # Construct the enhanced message
        message = f"""📊 <b>{company_display}</b>

🏷️ <b>Scrip:</b> {ai_scrip_code}
💰 <b>Price:</b> {price_display}
📢 <b>Title:</b> {title_display}
📅 <b>Date:</b> {formatted_date}

💹 <b>Financials:</b> {financial_display}

🎯 <b>INVEST?</b> {investment_display}
📈 <b>Sentiment:</b> {sentiment_display}
⚡ <b>Catalyst:</b> {catalyst_display}
📊 <b>Momentum:</b> {momentum_display}
👥 <b>Public:</b> {public_display}
🧠 <b>General:</b> {general_display}
🌐 <b>Web Intel:</b> {web_display}

📝 <b>TL;DR:</b> {tldr_display}"""

        # Append secure deep-link for full view
        try:
            from .security import sign_token
            token = sign_token({"user_id": analysis.get("user_id", ""), "news_id": analysis.get("news_id", "")}, expires_in_seconds=3*24*3600)
            full_url = f"{os.environ.get('APP_BASE_URL','')}/v/{token}" if os.environ.get('APP_BASE_URL') else f"/v/{token}"
            message += f"\n\n🔗 <b>Full details:</b> <a href=\"{full_url}\">Open securely</a>"
        except Exception:
            pass

        return message
        
    except Exception as e:
        # Fallback to basic format; try live price
        live_price_display = "N/A"
        try:
            live = get_stock_data_yahoo(scrip_code)
            lp = live.get('current_price') if isinstance(live, dict) else None
            pct = live.get('day_change_percent') if isinstance(live, dict) else None
            if lp not in (None, 'N/A'):
                live_price_display = str(lp)
                if pct not in (None, 'N/A'):
                    live_price_display = f"{live_price_display} ({pct}%)"
        except Exception:
            pass
        return f"""📊 <b>Company Announcement</b>

🏷️ <b>Scrip:</b> {scrip_code}
💰 <b>Price:</b> {live_price_display}
📢 <b>Title:</b> {announcement_title[:100]}{"..." if len(announcement_title) > 100 else ""}
📅 <b>Date:</b> {ann_date_ist.strftime("%d/%m/%y %I:%M %p") if ann_date_ist else "N/A"}

💹 <b>Financials:</b> AI analysis unavailable

🎯 <b>INVEST?</b> Consult financial advisor
📈 <b>Sentiment:</b> Unknown
👥 <b>Public:</b> To be determined
🧠 <b>General View:</b> Analysis failed
🎭 <b>Motive:</b> Review announcement details

📝 <b>TL;DR:</b> New announcement - manual review needed"""


def enrich_caption_with_ai(base_caption: str, analysis: Dict[str, Any], caption_limit: int) -> str:
    sections = [build_consolidated_section(analysis), build_tldr(analysis)]
    addition = "".join([s for s in sections if s])
    if not addition:
        return base_caption
    max_add_len = max(0, caption_limit - len(base_caption) - 1)
    if max_add_len <= 0:
        return base_caption
    addition = _truncate(addition, max_add_len)
    return base_caption + "\n" + addition


