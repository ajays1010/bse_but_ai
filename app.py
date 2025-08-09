import os
import time
import threading
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for, jsonify, make_response, Response
from datetime import datetime, timedelta
from functools import wraps

# Service-layer imports
from services.config import AppConfig
from services.logger import log_message
from services.db_service import (
    get_supabase_client,
    db_get_seen_announcements_for_today,
    db_read_all_subscribed_scrips,
    user_read_subscriptions,
    user_add_subscription,
    user_delete_subscription,
    is_admin_by_user_id,
    admin_list_users,
    admin_list_users_for_interface,
    admin_list_user_subs,
    admin_upsert_user_sub,
    admin_delete_user_sub,
    admin_list_user_telegrams,
    admin_add_user_telegram,
    admin_delete_user_telegram,
    user_list_telegrams,
    user_add_telegram,
    user_delete_telegram,
    user_get_profile,
    auth_sign_in,
    auth_sign_up,
)
from services.bse_service import process_announcements_for_scrip
from services.telegram_service import send_telegram_text
from services.scheduler_service import start_scheduler
from services.memory_guard import start_memory_watchdog, is_memory_ok, should_allow_ai
from services.security import sign_token, verify_token

# --- Flask App Initialization ---
app = Flask(__name__)
app.secret_key = AppConfig.CRON_SECRET_KEY
app.json.compact = False

# --- Decorators ---
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = request.cookies.get('sb_user_id')
        if not user_id or not is_admin_by_user_id(user_id):
            log_message(f"Unauthorized admin access attempt. UserID: {user_id}")
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated_function

# --- Background scheduler guard ---
_scheduler_started = False
def _start_scheduler_once():
    global _scheduler_started
    if _scheduler_started:
        return
    try:
        start_memory_watchdog()
        start_scheduler(300, announcement_check_task)
        _scheduler_started = True
        log_message("Background scheduler running: checks every 5 minutes.")
    except Exception as e:
        log_message(f"[SCHED] Failed to start background scheduler: {e}")

# Ensure scheduler starts when app serves the first request (works under Gunicorn)
# Note: before_first_request was removed in Flask 3.0, using before_request with flag instead
_first_request_done = False

@app.before_request
def _ensure_scheduler():
    global _first_request_done
    if not _first_request_done:
        _start_scheduler_once()
        _first_request_done = True

# --- Load local company data into memory ---
try:
    company_df = pd.read_csv(AppConfig.COMPANY_LIST_CSV)
    company_df['BSE Code'] = company_df['BSE Code'].astype(str)
except FileNotFoundError:
    print(f"[CRITICAL ERROR] The company list '{AppConfig.COMPANY_LIST_CSV}' was not found. Search will not work.")
    company_df = pd.DataFrame(columns=['BSE Code', 'Company Name'])

def announcement_check_task():
    """The main task, now triggered by the external cron job."""
    with app.app_context():
        log_message("--- Announcement check triggered ---")
        now_ist = datetime.now(AppConfig.IST)
        
        cutoff_time = now_ist - timedelta(hours=24)
        log_message(f"Checking for all announcements since {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} IST")
        
        # Multi-user segregation: iterate each user, fetch their subs and recipients
        users = admin_list_users() or []
        any_work = False
        for u in users:
            if not u:
                continue
            user_id = u.get('id')
            # Per-user subs and recipients
            subs = admin_list_user_subs(user_id) or []
            recs = admin_list_user_telegrams(user_id) or []
            if not subs or not recs:
                continue
            any_work = True
            recipients = [{'chat_id': r['chat_id'], 'user_id': user_id} for r in recs if r.get('chat_id')]
            for s in subs:
                code = s.get('bse_code')
                if not code:
                    continue
                process_announcements_for_scrip(code, recipients, cutoff_time)
        if not any_work:
            log_message("--- No per-user subs/recipients. Skipping check. ---")
        log_message("--- Announcement check finished. ---")

# --- Flask Routes ---
def set_auth_cookies(response: Response, tokens: dict) -> Response:
    """Helper to set access and refresh tokens in cookies."""
    if tokens.get('access_token'):
        response.set_cookie('sb_access_token', tokens['access_token'], httponly=True, samesite='Lax')
    if tokens.get('refresh_token'):
        response.set_cookie('sb_refresh_token', tokens['refresh_token'], httponly=True, samesite='Lax')
    return response

@app.route('/')
def index():
    access_token = request.cookies.get('sb_access_token')
    refresh_token = request.cookies.get('sb_refresh_token')
    user_id = request.cookies.get('sb_user_id')

    if not access_token:
        return redirect(url_for('auth_login_page'))

    # Check if the user has an admin role
    is_admin = is_admin_by_user_id(user_id) if user_id else False

    # The decorated function handles the refresh logic
    user_scrips, new_tokens = user_read_subscriptions(
        access_token=access_token,
        refresh_token=refresh_token
    )

    # If the function failed completely (e.g., refresh token invalid), log the user out
    if user_scrips is None:
        return redirect(url_for('auth_logout'))

    # Fetch user-scoped telegram recipients
    recipients, new_tokens_rec = user_list_telegrams(
        user_id=user_id,
        access_token=access_token,
        refresh_token=refresh_token
    )
    if recipients is None:
        recipients = []

    # Default render
    response = make_response(render_template('index.html',
                               monitored_scrips=user_scrips,
                               telegram_recipients=recipients,
                               is_admin=is_admin))

    # If tokens were refreshed by either call, set the new ones in the cookie
    combined_tokens = new_tokens_rec or new_tokens
    if combined_tokens:
        response = set_auth_cookies(response, combined_tokens)
    
    return response

@app.route('/health')
def health_check():
    return "OK", 200

@app.route('/admin')
@admin_required
def admin_home():
    # Provide dashboard plus an inline dropdown to jump to a user's page
    access_token = request.cookies.get('sb_access_token')
    all_users = admin_list_users_for_interface(access_token)
    # Filter out privileged users from the interface
    non_admin_users = [u for u in all_users if str(u.get('role', '')).strip().lower() not in ('admin', 'superuser')]
    return render_template('admin.html', users=non_admin_users)

@app.route('/admin/manage')
@admin_required
def admin_manage_page():
    access_token = request.cookies.get('sb_access_token')
    all_users = admin_list_users_for_interface(access_token)
    # Filter out privileged users so admins can't edit other admins/superusers in this interface
    non_admin_users = [u for u in all_users if str(u.get('role', '')).strip().lower() not in ('admin', 'superuser')]
    return render_template('admin_manage.html', users=non_admin_users)

@app.route('/admin/user/<user_id>')
@admin_required
def admin_user_detail(user_id):
    subs = admin_list_user_subs(user_id)
    telegrams = admin_list_user_telegrams(user_id)
    return render_template('admin_user.html', user_id=user_id, subs=subs, telegrams=telegrams, is_admin=True)

@app.route('/admin/user/<user_id>/add_sub', methods=['POST'])
@admin_required
def admin_add_sub(user_id: str):
    code = request.form.get('bse_code','').strip()
    name = request.form.get('company_name','').strip()
    if code and name:
        admin_upsert_user_sub(user_id, code, name)
    return redirect(url_for('admin_user_detail', user_id=user_id))

@app.route('/admin/user/<user_id>/delete_sub', methods=['POST'])
@admin_required
def admin_delete_sub(user_id: str):
    code = request.form.get('bse_code','').strip()
    if code:
        admin_delete_user_sub(user_id, code)
    return redirect(url_for('admin_user_detail', user_id=user_id))

@app.route('/admin/user/<user_id>/add_telegram', methods=['POST'])
@admin_required
def admin_add_telegram(user_id: str):
    chat_id = request.form.get('chat_id','').strip()
    if chat_id:
        admin_add_user_telegram(user_id, chat_id)
    return redirect(url_for('admin_user_detail', user_id=user_id))

@app.route('/admin/user/<user_id>/delete_telegram', methods=['POST'])
@admin_required
def admin_delete_telegram(user_id: str):
    chat_id = request.form.get('chat_id','').strip()
    if chat_id:
        admin_delete_user_telegram(user_id, chat_id)
    return redirect(url_for('admin_user_detail', user_id=user_id))

@app.route('/auth/login', methods=['GET'])
def auth_login_page():
    return render_template('auth.html')

@app.route('/auth/login', methods=['POST'])
def auth_login():
    email = request.form.get('email','').strip()
    password = request.form.get('password','').strip()
    result = auth_sign_in(email, password)
    if not result or not result.get('access_token'):
        return render_template('auth.html', error='Invalid credentials')
    resp = make_response(redirect(url_for('index')))
    # Cookie holds user session token for RLS-bound requests
    set_auth_cookies(resp, {
        'access_token': result['access_token'],
        'refresh_token': result.get('refresh_token')
    })
    resp.set_cookie('sb_user_id', result.get('user_id') or '', httponly=True, samesite='Lax')
    return resp

@app.route('/auth/signup', methods=['GET'])
def auth_signup_page():
    return render_template('signup.html')

@app.route('/auth/signup', methods=['POST'])
def auth_signup():
    email = request.form.get('email','').strip()
    password = request.form.get('password','').strip()
    full_name = request.form.get('full_name','').strip()
    ok = auth_sign_up(email, password, full_name)
    if not ok:
        return render_template('signup.html', error='Sign up failed. Try a different email or try again later.')
    return redirect(url_for('auth_login_page'))

@app.route('/auth/logout')
def auth_logout():
    resp = make_response(redirect(url_for('auth_login_page')))
    resp.delete_cookie('sb_access_token')
    resp.delete_cookie('sb_refresh_token')
    resp.delete_cookie('sb_user_id')
    return resp

@app.route('/trigger_check/<secret_key>')
def trigger_check(secret_key):
    """Triggers the announcement check if the secret key is valid."""
    if not AppConfig.CRON_SECRET_KEY or secret_key != AppConfig.CRON_SECRET_KEY:
        log_message("Unauthorized attempt to trigger check.")
        return "Unauthorized", 403
    
    log_message("External trigger received. Starting check in background.")
    threading.Thread(target=announcement_check_task).start()
    return "Check triggered.", 200

@app.route('/trigger_check_manual', methods=['POST'])
def trigger_check_manual():
    log_message("Manual trigger received from dashboard.")
    threading.Thread(target=announcement_check_task).start()
    return redirect(url_for('index'))


@app.route('/v/<token>')
def view_message(token: str):
    """Secure view of a full message for a specific user/news.
    Token payload: { user_id, news_id }
    """
    payload = verify_token(token)
    if not payload:
        return "Unauthorized", 403
    request_user = request.cookies.get('sb_user_id')
    if not request_user or request_user != payload.get('user_id'):
        return "Unauthorized", 403

    # Fetch from DB and render
    from services.db_service import get_supabase_client
    sb = get_supabase_client()
    if not sb:
        return "Service unavailable", 503
    try:
        rec = sb.table('seen_announcements').select('*').eq('news_id', payload.get('news_id')).eq('user_id', request_user).single().execute().data
    except Exception:
        rec = None
    if not rec:
        return "Not found", 404

    # Render a special template with ethereal/telegram look
    return render_template('view_message.html', caption=rec.get('caption'), company=rec.get('headline'), pdf_name=rec.get('pdf_name'))

@app.route('/search')
def search_stocks():
    query = request.args.get('query', '')
    if not query or len(query) < 2: return jsonify({"matches": []})
    mask = (company_df['Company Name'].str.contains(query, case=False, na=False)) | \
           (company_df['BSE Code'].str.startswith(query))
    matches = company_df[mask].head(10)
    return jsonify({"matches": matches.to_dict('records')})

def process_new_scrip_task(code, recipients, cutoff_time):
    """A dedicated task for processing a newly added scrip with app context."""
    with app.app_context():
        process_announcements_for_scrip(code, recipients, cutoff_time)

@app.route('/add_scrip', methods=['POST'])
def add_scrip():
    access_token = request.cookies.get('sb_access_token')
    user_id = request.cookies.get('sb_user_id')
    refresh_token = request.cookies.get('sb_refresh_token')

    if not access_token or not user_id:
        return redirect(url_for('auth_login_page'))

    code = request.form.get('scrip_code', '').strip()
    name = request.form.get('company_name', '').strip()

    if code and name:
        ok, new_tokens = user_add_subscription(
            user_id=user_id,
            bse_code=code,
            company_name=name,
            access_token=access_token,
            refresh_token=refresh_token
        )

        if ok is None: # Hard failure
            return redirect(url_for('auth_logout'))
        
        if ok:
            log_message(f"User {user_id} added scrip {code}. Triggering immediate check.")
            recipients = admin_list_user_telegrams(user_id) or []
            recipients = [{'chat_id': r['chat_id'], 'user_id': user_id} for r in recipients if r.get('chat_id')]
            cutoff_time = datetime.now(AppConfig.IST) - timedelta(hours=24)
            threading.Thread(target=process_new_scrip_task, args=(code, recipients, cutoff_time)).start()
        
        response = make_response(redirect(url_for('index')))
        if new_tokens:
            response = set_auth_cookies(response, new_tokens)
        return response

    return redirect(url_for('index'))

@app.route('/delete_scrip', methods=['POST'])
def delete_scrip():
    access_token = request.cookies.get('sb_access_token')
    refresh_token = request.cookies.get('sb_refresh_token')

    if not access_token:
        return redirect(url_for('auth_login_page'))

    code = request.form.get('scrip_code', '')
    if code:
        ok, new_tokens = user_delete_subscription(
            bse_code=code,
            access_token=access_token,
            refresh_token=refresh_token
        )

        if ok is None: # Hard failure
            return redirect(url_for('auth_logout'))

        response = make_response(redirect(url_for('index')))
        if new_tokens:
            response = set_auth_cookies(response, new_tokens)
        return response
        
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
            send_telegram_text(chat_id, f"{ann['pdf_name']}\n\n{ann['caption']}")
        log_message(f"Finished catch-up for {chat_id}.")

@app.route('/add_recipient', methods=['GET', 'POST'])
def add_recipient():
    # GET fallback: just go back to dashboard
    if request.method == 'GET':
        return redirect(url_for('index'))
    # Backward-compatible: route now acts on current user's recipients
    access_token = request.cookies.get('sb_access_token')
    refresh_token = request.cookies.get('sb_refresh_token')
    user_id = request.cookies.get('sb_user_id')
    if not access_token or not user_id:
        return redirect(url_for('auth_login_page'))
    chat_id = request.form.get('chat_id', '').strip()
    if chat_id:
        ok, new_tokens = user_add_telegram(user_id=user_id, chat_id=chat_id, access_token=access_token, refresh_token=refresh_token)
        if ok:
            threading.Thread(target=catch_up_new_recipient, args=(chat_id,)).start()
        if ok is None:
            return redirect(url_for('auth_logout'))
        response = make_response(redirect(url_for('index')))
        if new_tokens:
            response = set_auth_cookies(response, new_tokens)
        return response
    return redirect(url_for('index'))

@app.route('/delete_recipient', methods=['GET', 'POST'])
def delete_recipient():
    if request.method == 'GET':
        return redirect(url_for('index'))
    access_token = request.cookies.get('sb_access_token')
    refresh_token = request.cookies.get('sb_refresh_token')
    user_id = request.cookies.get('sb_user_id')
    if not access_token or not user_id:
        return redirect(url_for('auth_login_page'))
    chat_id = request.form.get('chat_id', '').strip()
    if chat_id:
        ok, new_tokens = user_delete_telegram(user_id=user_id, chat_id=chat_id, access_token=access_token, refresh_token=refresh_token)
        if ok is None:
            return redirect(url_for('auth_logout'))
        response = make_response(redirect(url_for('index')))
        if new_tokens:
            response = set_auth_cookies(response, new_tokens)
        return response
    return redirect(url_for('index'))

@app.route('/me')
def user_profile_page():
    access_token = request.cookies.get('sb_access_token')
    refresh_token = request.cookies.get('sb_refresh_token')
    user_id = request.cookies.get('sb_user_id')
    if not access_token or not user_id:
        return redirect(url_for('auth_login_page'))
    profile, new_tokens = user_get_profile(user_id=user_id, access_token=access_token, refresh_token=refresh_token)
    if profile is None:
        return redirect(url_for('auth_logout'))
    # Also show this user's current subs and telegram ids
    subs, new_tokens2 = user_read_subscriptions(access_token=access_token, refresh_token=refresh_token)
    telegrams, new_tokens3 = user_list_telegrams(user_id=user_id, access_token=access_token, refresh_token=refresh_token)
    response = make_response(render_template('me.html', profile=profile, subs=subs or [], telegrams=telegrams or []))
    combined = new_tokens or new_tokens2 or new_tokens3
    if combined:
        response = set_auth_cookies(response, combined)
    return response

if __name__ == '__main__':
    get_supabase_client()
    # Log Gemini availability for clarity in ops
    if not (os.environ.get('GEMINI_API_KEY') or os.environ.get('GOOGLE_API_KEY')):
        log_message("[AI] GEMINI_API_KEY not set. AI summaries are disabled.")
    else:
        log_message("[AI] GEMINI_API_KEY detected. AI summaries enabled.")
    log_message("Application started. Scheduler should already be running. You can also trigger manually.")
    # Start scheduler now that all functions are defined
    _start_scheduler_once()
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)), use_reloader=False)