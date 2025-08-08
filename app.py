import os
import requests
import time
import random
from flask import Flask, request, render_template_string, redirect, url_for, jsonify
from datetime import datetime, timedelta
import json
import pytz
import threading
from supabase import create_client, Client
# APScheduler is no longer needed and has been removed
import pandas as pd

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
# A secret key to protect the trigger endpoint
CRON_SECRET_KEY = os.environ.get("CRON_SECRET_KEY")


BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
PDF_BASE_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
IST = pytz.timezone('Asia/Kolkata')
COMPANY_LIST_CSV = 'bse_company_list_cleaned.csv' 

supabase: Client = None

# --- Shared Resources ---
BSE_HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36', 'Referer': 'https://www.bseindia.com/'}

# --- Flask App Initialization ---
app = Flask(__name__)

# --- Load local company data into memory ---
try:
    company_df = pd.read_csv(COMPANY_LIST_CSV)
    company_df['BSE Code'] = company_df['BSE Code'].astype(str)
except FileNotFoundError:
    print(f"[CRITICAL ERROR] The company list '{COMPANY_LIST_CSV}' was not found. Search will not work.")
    company_df = pd.DataFrame(columns=['BSE Code', 'Company Name'])

# --- Logging Function ---
def log_message(message):
    print(f"[{datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}] {message}")

# --- Supabase Client Initialization ---
def get_supabase_client():
    global supabase
    if supabase is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            log_message("CRITICAL ERROR: Supabase URL or Key not set in environment variables. Database operations will fail.")
            return None
        try:
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            log_message("Supabase client initialized.")
        except Exception as e:
            log_message(f"Error initializing Supabase client: {e}")
            supabase = None
    return supabase

# --- START: HTML Template for the Web UI ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BSE Bot Dashboard</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-100">
    <div class="container mx-auto p-4 md:p-8">
        <h1 class="text-3xl font-bold mb-6 text-center text-gray-800">BSE Bot Dashboard</h1>
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            <div class="bg-white p-6 rounded-lg shadow-lg">
                <h2 class="text-xl font-semibold mb-4">Monitor New Scrip</h2>
                <div class="relative mb-6">
                    <input type="text" id="search-box" placeholder="Search by name or scrip code..." class="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700" autocomplete="off">
                    <div id="search-results" class="absolute z-10 w-full bg-white border mt-1 rounded-lg shadow-lg hidden"></div>
                </div>
                <h3 class="text-lg font-semibold mb-4 border-t pt-4">Monitored Scrips</h3>
                <div class="overflow-y-auto max-h-96">
                    <table class="min-w-full bg-white">
                        <tbody>
                            {% for scrip in monitored_scrips %}
                            <tr class="border-b">
                                <td class="py-2 px-4"><b>{{ scrip.bse_code }}</b><br><span class="text-sm text-gray-600">{{ scrip.company_name }}</span></td>
                                <td class="py-2 px-4 text-right">
                                    <form action="/delete_scrip" method="post" class="inline-block">
                                        <input type="hidden" name="scrip_code" value="{{ scrip.bse_code }}">
                                        <button type="submit" class="bg-red-500 hover:bg-red-700 text-white text-xs font-bold py-1 px-2 rounded">Delete</button>
                                    </form>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>

            <div class="bg-white p-6 rounded-lg shadow-lg">
                <h2 class="text-xl font-semibold mb-4">Add Telegram Recipient</h2>
                <form action="/add_recipient" method="post" class="flex items-center gap-4 mb-6">
                    <input type="text" name="chat_id" placeholder="Enter Telegram Chat ID" class="shadow border rounded w-full py-2 px-3 text-gray-700" required>
                    <button type="submit" class="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded">Add</button>
                </form>
                <h3 class="text-lg font-semibold mb-4 border-t pt-4">Notification Recipients</h3>
                <div class="overflow-y-auto max-h-96">
                    <table class="min-w-full bg-white">
                        <tbody>
                            {% for recipient in telegram_recipients %}
                            <tr class="border-b">
                                <td class="py-2 px-4">{{ recipient.chat_id }}</td>
                                <td class="py-2 px-4 text-right">
                                    <form action="/delete_recipient" method="post" class="inline-block">
                                        <input type="hidden" name="chat_id" value="{{ recipient.chat_id }}">
                                        <button type="submit" class="bg-red-500 hover:bg-red-700 text-white text-xs font-bold py-1 px-2 rounded">Delete</button>
                                    </form>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        <!-- ADDED: Manual Trigger Section -->
        <div class="bg-white p-6 rounded-lg shadow-lg mt-8">
            <h2 class="text-xl font-semibold mb-4">Manual Trigger</h2>
            <p class="text-gray-600 mb-4">This will run the announcement check immediately. This is the same action performed by the external cron job.</p>
            <form action="/trigger_check_manual" method="post">
                <button type="submit" class="bg-green-500 hover:bg-green-700 text-white font-bold py-2 px-4 rounded w-full">
                    Trigger Announcement Check Now
                </button>
            </form>
        </div>
    </div>
    <script>
        const searchBox = document.getElementById('search-box');
        const searchResults = document.getElementById('search-results');
        let debounceTimer;

        searchBox.addEventListener('keyup', () => {
            clearTimeout(debounceTimer);
            const query = searchBox.value;
            if (query.length < 2) { searchResults.classList.add('hidden'); return; }
            debounceTimer = setTimeout(() => {
                fetch(`/search?query=${query}`)
                    .then(response => response.json())
                    .then(data => {
                        searchResults.innerHTML = '';
                        if (data.matches && data.matches.length > 0) {
                            data.matches.forEach(match => {
                                const div = document.createElement('div');
                                div.innerHTML = `<div class="p-3 hover:bg-gray-100 cursor-pointer"><p class="font-bold">${match['BSE Code']} <span class="font-normal text-gray-600">- ${match['Company Name']}</span></p></div>`;
                                div.addEventListener('click', () => addScrip(match['BSE Code'], match['Company Name']));
                                searchResults.appendChild(div);
                            });
                        } else {
                            searchResults.innerHTML = `<div class="p-3 text-gray-500">No matches found.</div>`;
                        }
                        searchResults.classList.remove('hidden');
                    });
            }, 300);
        });

        function addScrip(scripCode, companyName) {
            const form = document.createElement('form');
            form.method = 'POST';
            form.action = '/add_scrip';
            form.innerHTML = `<input type="hidden" name="scrip_code" value="${scripCode}"><input type="hidden" name="company_name" value="${companyName}">`;
            document.body.appendChild(form);
            form.submit();
        }

        document.addEventListener('click', e => !searchBox.contains(e.target) && searchResults.classList.add('hidden'));
    </script>
</body>
</html>
"""
# --- END: HTML Template for the Web UI ---

# --- Supabase Handling Functions ---
def db_read_monitored_scrips():
    sb = get_supabase_client()
    if not sb: return []
    try:
        return sb.table('monitored_scrips').select('bse_code, company_name').execute().data or []
    except Exception as e:
        log_message(f"DB Read monitored_scrips failed: {e}")
        return []

def db_read_telegram_recipients():
    sb = get_supabase_client()
    if not sb: return []
    try:
        return sb.table('telegram_recipients').select('chat_id').execute().data or []
    except Exception as e:
        log_message(f"DB Read telegram_recipients failed: {e}")
        return []

def db_get_seen_announcements_for_today():
    sb = get_supabase_client()
    if not sb: return []
    try:
        today_start = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        return sb.table('seen_announcements').select('*').gte('created_at', today_start).order('created_at').execute().data or []
    except Exception as e:
        log_message(f"DB Read seen_announcements failed: {e}")
        return []

def db_check_if_announcement_seen(news_id):
    sb = get_supabase_client()
    if not sb: return True
    try:
        response = sb.table('seen_announcements').select('news_id', count='exact').eq('news_id', news_id).execute()
        return response.count > 0
    except Exception as e:
        log_message(f"DB check seen failed: {e}")
        return True

def db_save_announcement(news_id, scrip_code, headline, pdf_name, ann_date_ist, caption):
    sb = get_supabase_client()
    if not sb: return

    announcement_data = {
        'news_id': news_id,
        'scrip_code': scrip_code,
        'headline': headline,
        'pdf_name': pdf_name,
        'ann_date': ann_date_ist.isoformat(),
        'caption': caption
    }
    try:
        sb.table('seen_announcements').insert(announcement_data).execute()
        log_message(f"  [DB LOG] > Saved announcement {news_id} for {scrip_code}")
    except Exception as e:
        log_message(f"  [DB LOG] > DB insert failed for {news_id}: {e}")

# --- Telegram & Scraping Logic ---
def send_telegram_text(chat_id, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
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

def send_telegram_document(chat_id, pdf_name, caption, pdf_content=None):
    try:
        if pdf_content is None:
            pdf_url = f"{PDF_BASE_URL}{pdf_name}"
            pdf_response = requests.get(pdf_url, timeout=30, headers=BSE_HEADERS)
            if pdf_response.status_code != 200 or not pdf_response.content:
                log_message(f"Failed to download PDF: {pdf_url} (Status: {pdf_response.status_code})")
                return False
            pdf_content = pdf_response.content
        
        final_caption = caption

        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
        payload = {"chat_id": chat_id, "caption": final_caption, "parse_mode": "HTML"}
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

def process_announcements_for_scrip(scrip_code, recipients, cutoff_time_ist):
    log_message(f"-> Checking scrip: {scrip_code}")
    from_date_obj = datetime.now(IST) - timedelta(days=7)
    params = {'strCat': '-1', 'strPrevDate': from_date_obj.strftime('%Y%m%d'), 'strToDate': datetime.now(IST).strftime('%Y%m%d'), 'strScrip': scrip_code, 'strSearch': 'P', 'strType': 'C'}
    
    try:
        response = requests.get(BSE_API_URL, headers=BSE_HEADERS, params=params, timeout=30)
        data = response.json()
        if not data.get('Table'): return

        for announcement in data['Table']:
            news_id = announcement.get('NEWSID')
            pdf_name = announcement.get('ATTACHMENTNAME')
            
            if not news_id or not pdf_name:
                continue

            ann_date_str = announcement.get('NEWS_DT') or announcement.get('DissemDT')
            if not ann_date_str or db_check_if_announcement_seen(news_id):
                continue
            
            try:
                naive_date = datetime.strptime(ann_date_str, '%d %b %Y %I:%M:%S %p')
            except ValueError:
                try: naive_date = datetime.strptime(ann_date_str, '%Y-%m-%dT%H:%M:%S.%f')
                except ValueError:
                    try: naive_date = datetime.strptime(ann_date_str, '%Y-%m-%dT%H:%M:%S')
                    except ValueError: continue
            
            ann_date_ist = IST.localize(naive_date)

            if ann_date_ist >= cutoff_time_ist:
                headline = announcement.get('NEWSSUB') or announcement.get('HEADLINE', 'N/A')
                
                log_message(f"  [FOUND NEW] {headline} for {scrip_code}")
                caption = f"<b>Scrip:</b> {scrip_code}\n<b>Announcement:</b> {headline}\n<b>Date:</b> {ann_date_ist.strftime('%d-%m-%Y %H:%M')} IST"
                
                db_save_announcement(
                    news_id=news_id,
                    scrip_code=scrip_code,
                    headline=headline,
                    pdf_name=pdf_name,
                    ann_date_ist=ann_date_ist,
                    caption=caption
                )

                for recipient in recipients:
                    send_telegram_document(recipient['chat_id'], pdf_name, caption)
                
    except Exception as e:
        log_message(f"Scrape failed for {scrip_code}: {e}")

def announcement_check_task():
    """The main task, now triggered by the external cron job."""
    with app.app_context():
        log_message("--- Announcement check triggered ---")
        now_ist = datetime.now(IST)
        
        cutoff_time = now_ist - timedelta(hours=24)
        log_message(f"Checking for all announcements since {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
        
        monitored_scrips = db_read_monitored_scrips()
        recipients = db_read_telegram_recipients()

        if not monitored_scrips or not recipients:
            log_message("--- No scrips or recipients. Skipping check. ---")
            return

        for scrip in monitored_scrips:
            process_announcements_for_scrip(scrip['bse_code'], recipients, cutoff_time)
        log_message("--- Announcement check finished. ---")

# --- Flask Routes ---
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, 
                                  monitored_scrips=db_read_monitored_scrips(), 
                                  telegram_recipients=db_read_telegram_recipients())

@app.route('/health')
def health_check():
    """A simple endpoint for Render's health checks."""
    return "OK", 200

# --- MODIFIED: This is now the primary trigger for the scheduled check ---
@app.route('/trigger_check/<secret_key>')
def trigger_check(secret_key):
    """Triggers the announcement check if the secret key is valid."""
    if not CRON_SECRET_KEY or secret_key != CRON_SECRET_KEY:
        log_message("Unauthorized attempt to trigger check.")
        return "Unauthorized", 403
    
    log_message("External trigger received. Starting check in background.")
    threading.Thread(target=announcement_check_task).start()
    return "Check triggered.", 200

# --- ADDED: A manual trigger route for the button on the dashboard ---
@app.route('/trigger_check_manual', methods=['POST'])
def trigger_check_manual():
    log_message("Manual trigger received from dashboard.")
    threading.Thread(target=announcement_check_task).start()
    return redirect(url_for('index'))

@app.route('/search')
def search_stocks():
    query = request.args.get('query', '')
    if not query or len(query) < 2: return jsonify({"matches": []})
    mask = (company_df['Company Name'].str.contains(query, case=False, na=False)) | \
           (company_df['BSE Code'].str.startswith(query))
    matches = company_df[mask].head(10)
    return jsonify({"matches": matches.to_dict('records')})

@app.route('/add_scrip', methods=['POST'])
def add_scrip():
    sb = get_supabase_client()
    if not sb: return redirect(url_for('index'))
    code = request.form.get('scrip_code', '').strip()
    name = request.form.get('company_name', '').strip()
    if code and name:
        try:
            sb.table('monitored_scrips').upsert({'bse_code': code, 'company_name': name}).execute()
            log_message(f"New scrip {code} added. Triggering immediate check.")
            recipients = db_read_telegram_recipients()
            cutoff_time = datetime.now(IST) - timedelta(hours=24)
            threading.Thread(target=process_announcements_for_scrip, args=(code, recipients, cutoff_time)).start()
        except Exception as e:
            log_message(f"DB add scrip failed: {e}")
    return redirect(url_for('index'))

@app.route('/delete_scrip', methods=['POST'])
def delete_scrip():
    sb = get_supabase_client()
    if not sb: return redirect(url_for('index'))
    code = request.form.get('scrip_code', '')
    if code:
        try:
            sb.table('monitored_scrips').delete().eq('bse_code', code).execute()
        except Exception as e:
            log_message(f"DB delete scrip failed: {e}")
    return redirect(url_for('index'))

def catch_up_new_recipient(chat_id):
    with app.app_context():
        log_message(f"Starting catch-up for new recipient {chat_id}...")
        announcements_today = db_get_seen_announcements_for_today()
        if not announcements_today:
            log_message("No announcements from today to catch up on.")
            send_telegram_text(chat_id, "✅ You are subscribed! No announcements from today to catch you up on.")
            return
        
        send_telegram_text(chat_id, f"✅ You are subscribed! Sending {len(announcements_today)} announcements from today to catch you up...")
        time.sleep(2)

        for ann in announcements_today:
            send_telegram_document(chat_id, ann['pdf_name'], ann['caption'])
        log_message(f"Finished catch-up for {chat_id}.")

@app.route('/add_recipient', methods=['POST'])
def add_recipient():
    sb = get_supabase_client()
    if not sb: return redirect(url_for('index'))
    chat_id = request.form.get('chat_id', '').strip()
    if chat_id:
        try:
            sb.table('telegram_recipients').upsert({'chat_id': chat_id}).execute()
            threading.Thread(target=catch_up_new_recipient, args=(chat_id,)).start()
        except Exception as e:
            log_message(f"DB add recipient failed: {e}")
    return redirect(url_for('index'))

@app.route('/delete_recipient', methods=['POST'])
def delete_recipient():
    sb = get_supabase_client()
    if not sb: return redirect(url_for('index'))
    chat_id = request.form.get('chat_id', '')
    if chat_id:
        try:
            sb.table('telegram_recipients').delete().eq('chat_id', chat_id).execute()
        except Exception as e:
            log_message(f"DB delete recipient failed: {e}")
    return redirect(url_for('index'))

if __name__ == '__main__':
    get_supabase_client()
    # The internal scheduler has been removed to rely solely on the external trigger.
    log_message("Application started. Waiting for external trigger to check announcements.")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)), use_reloader=False)
