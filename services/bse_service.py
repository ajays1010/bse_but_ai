from typing import List, Dict, Any, Optional
import os
import requests
from datetime import datetime, timedelta

from .config import AppConfig
from .logger import log_message
from .security import sign_token
from .memory_guard import is_memory_ok
from .db_service import db_check_if_announcement_seen, db_save_announcement, db_check_if_announcement_seen_by_user, db_save_announcement_for_user, db_check_if_pdf_seen_by_user
from .telegram_service import send_document_handling_overflow
from .ai_service import analyze_pdf_bytes_with_gemini, enrich_caption_with_ai, format_structured_telegram_message


def process_announcements_for_scrip(scrip_code: str, recipients: List[Dict[str, Any]], cutoff_time_ist: datetime) -> None:
    log_message(f"-> Checking scrip: {scrip_code}")
    from_date_obj = datetime.now(AppConfig.IST) - timedelta(days=7)
    params = {
        'strCat': '-1',
        'strPrevDate': from_date_obj.strftime('%Y%m%d'),
        'strToDate': datetime.now(AppConfig.IST).strftime('%Y%m%d'),
        'strScrip': scrip_code,
        'strSearch': 'P',
        'strType': 'C'
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.bseindia.com/'
    }

    try:
        response = requests.get(AppConfig.BSE_API_URL, headers=headers, params=params, timeout=30)
        data = response.json()
        if not data.get('Table'):
            return

        for announcement in data['Table']:
            news_id = announcement.get('NEWSID')
            pdf_name = announcement.get('ATTACHMENTNAME')
            if not news_id or not pdf_name:
                continue

            ann_date_str = announcement.get('NEWS_DT') or announcement.get('DissemDT')
            if not ann_date_str:
                continue
            
            # Check if any of the recipients for this scrip have NOT seen this announcement
            users_who_need_notification = []
            for recipient in recipients:
                user_id = recipient.get('user_id')
                if not user_id:
                    continue
                # If the exact news_id wasn't seen, still dedupe by same pdf_name for this user
                if db_check_if_announcement_seen_by_user(news_id, user_id):
                    continue
                if pdf_name and db_check_if_pdf_seen_by_user(pdf_name, user_id):
                    continue
                users_who_need_notification.append(recipient)
            
            # If all users have seen this announcement, skip it
            if not users_who_need_notification:
                continue

            # Parse announcement date
            naive_date: Optional[datetime] = None
            for fmt in ('%d %b %Y %I:%M:%S %p', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S'):
                try:
                    naive_date = datetime.strptime(ann_date_str, fmt)
                    break
                except ValueError:
                    continue
            if naive_date is None:
                continue

            ann_date_ist = AppConfig.IST.localize(naive_date)
            if ann_date_ist < cutoff_time_ist:
                continue

            headline = announcement.get('NEWSSUB') or announcement.get('HEADLINE', 'N/A')
            log_message(f"  [FOUND NEW] {headline} for {scrip_code}")

            # Try AI enrichment if PDF available
            pdf_url = f"{AppConfig.PDF_BASE_URL}{pdf_name}"
            pdf_bytes: Optional[bytes] = None
            try:
                pdf_response = requests.get(pdf_url, timeout=30, headers=headers)
                if pdf_response.status_code == 200 and pdf_response.content:
                    pdf_bytes = pdf_response.content
            except Exception as e:
                log_message(f"Error downloading PDF for AI: {e}")

            # Generate structured Telegram message
            final_caption = ""
            if pdf_bytes and is_memory_ok(soft=True):
                analysis = analyze_pdf_bytes_with_gemini(pdf_bytes, pdf_name, scrip_code)
                if analysis:
                    # Use the new structured format
                    final_caption = format_structured_telegram_message(analysis, scrip_code, headline, ann_date_ist)
                else:
                    # Fallback to basic format if AI analysis fails
                    final_caption = f"""📊 <b>Company Announcement</b>

🏷️ <b>Scrip:</b> {scrip_code}
💰 <b>Price:</b> N/A
📢 <b>Title:</b> {headline[:100]}{"..." if len(headline) > 100 else ""}
📅 <b>Date:</b> {ann_date_ist.strftime('%d/%m/%y %I:%M %p')}

💹 <b>Financials:</b> AI analysis unavailable

🎯 <b>INVEST?</b> Consult financial advisor
📈 <b>Sentiment:</b> Unknown
👥 <b>Public:</b> To be determined
🧠 <b>General View:</b> Analysis failed
🎭 <b>Motive:</b> Review announcement details

📝 <b>TL;DR:</b> New announcement - manual review needed"""
            else:
                # Fallback if PDF download fails
                final_caption = f"""📊 <b>Company Announcement</b>

🏷️ <b>Scrip:</b> {scrip_code}
💰 <b>Price:</b> N/A
📢 <b>Title:</b> {headline[:100]}{"..." if len(headline) > 100 else ""}
📅 <b>Date:</b> {ann_date_ist.strftime('%d/%m/%y %I:%M %p')}

💹 <b>Financials:</b> PDF download failed

🎯 <b>INVEST?</b> Consult financial advisor
📈 <b>Sentiment:</b> Unknown
👥 <b>Public:</b> To be determined
🧠 <b>General View:</b> Document unavailable
🎭 <b>Motive:</b> Check BSE website

📝 <b>TL;DR:</b> New announcement - PDF unavailable"""

            # Send message to users who haven't seen this announcement
            for recipient in users_who_need_notification:
                # Save announcement as seen for this specific user
                user_id = recipient.get('user_id')
                if user_id:
                    db_save_announcement_for_user(
                        news_id=news_id,
                        user_id=user_id,
                        scrip_code=scrip_code,
                        headline=headline,
                        pdf_name=pdf_name,
                        ann_date_ist=ann_date_ist,
                        caption=final_caption,
                    )
                # Append secure deep link for this specific user/news
                try:
                    token = sign_token({"user_id": user_id, "news_id": news_id}, expires_in_seconds=3*24*3600)
                    base = AppConfig.APP_BASE_URL
                    link = f"{base}/v/{token}" if base else None
                    # Put the link at the TOP so it's always visible; also attach inline button via view_url
                    per_user_caption = final_caption
                    if link:
                        per_user_caption = f"🔗 <b>Full details:</b> <a href=\"{link}\">Open securely</a>\n\n" + final_caption
                    else:
                        log_message("[BSE] APP_BASE_URL missing; skipping inline button and caption link. Set APP_BASE_URL or RENDER_EXTERNAL_URL.")
                except Exception:
                    per_user_caption = final_caption

                # Send Telegram message
                send_document_handling_overflow(recipient['chat_id'], pdf_name, per_user_caption, pdf_bytes, link)

    except Exception as e:
        log_message(f"Scrape failed for {scrip_code}: {e}")


