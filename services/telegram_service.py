from typing import Optional
import json
import requests
from .config import AppConfig
from .logger import log_message


def send_telegram_text(chat_id: str, message: str) -> bool:
    url = f"https://api.telegram.org/bot{AppConfig.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    try:
        log_message(f"  [TELEGRAM] > Sending text to chat_id: {chat_id}")
        response = requests.post(url, data=payload, timeout=10)
        if response.json().get("ok"):
            log_message(f"  [TELEGRAM] > Successfully sent text to {chat_id}")
            return True
    except Exception as e:
        log_message(f"  [TELEGRAM] > Text send failed for {chat_id}: {e}")
    return False


def send_telegram_document(chat_id: str, pdf_name: str, caption: str, pdf_content: Optional[bytes] = None, view_url: Optional[str] = None) -> bool:
    try:
        if pdf_content is None:
            pdf_url = f"{AppConfig.PDF_BASE_URL}{pdf_name}"
            bse_headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Referer': 'https://www.bseindia.com/'
            }
            pdf_response = requests.get(pdf_url, timeout=30, headers=bse_headers)
            if pdf_response.status_code != 200 or not pdf_response.content:
                log_message(f"Failed to download PDF: {pdf_url} (Status: {pdf_response.status_code})")
                return False
            pdf_content = pdf_response.content

        url = f"https://api.telegram.org/bot{AppConfig.TELEGRAM_BOT_TOKEN}/sendDocument"
        payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
        if view_url:
            payload["reply_markup"] = json.dumps({
                "inline_keyboard": [[
                    {"text": "read_full_message", "url": view_url}
                ]]
            })
        files = {"document": (pdf_name, pdf_content, "application/pdf")}

        log_message(f"  [TELEGRAM] > Sending document '{pdf_name}' to chat_id: {chat_id}")
        tg_response = requests.post(url, data=payload, files=files, timeout=45)
        if tg_response.json().get("ok"):
            log_message(f"  [TELEGRAM] > Successfully sent document to {chat_id}")
            return True
        else:
            log_message(f"  [TELEGRAM] > API error for {chat_id}: {tg_response.text}")
            return False
    except Exception as e:
        log_message(f"  [TELEGRAM] > Document send failed for {chat_id}: {e}")
        return False


def _split_text(text: str, max_len: int) -> list[str]:
    if len(text) <= max_len:
        return [text]
    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_len)
        slice_text = text[start:end]
        if end < len(text):
            nl = slice_text.rfind("\n")
            if nl >= 0 and (start + nl) > start + int(0.6 * (end - start)):
                end = start + nl + 1
                slice_text = text[start:end]
        chunks.append(slice_text)
        start = end
    return chunks


def send_document_handling_overflow(chat_id: str, pdf_name: str, caption: str, pdf_bytes: Optional[bytes], view_url: Optional[str] = None):
    limit = AppConfig.TELEGRAM_CAPTION_LIMIT
    if len(caption) <= limit:
        if pdf_bytes:
            return send_telegram_document(chat_id, pdf_name, caption, pdf_bytes, view_url)
        return send_telegram_document(chat_id, pdf_name, caption, None, view_url)
    # Ensure first message carries the deep link at the bottom without fail
    first = caption[:limit]
    if view_url:
        link_line = f"\n\n🔗 <b>read_full_message:</b> <a href=\"{view_url}\">open</a>"
        # Trim headroom and append link line
        headroom = max(0, limit - len(link_line))
        first = caption[:headroom] + link_line
    sent = send_telegram_document(chat_id, pdf_name, first, pdf_bytes, view_url)
    if not sent:
        return False
    for extra in chunks[1:]:
        send_telegram_text(chat_id, extra)
    return True


