import os
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, render_template, redirect, url_for, session, flash, jsonify
from supabase import create_client
import pandas as pd
import database as db
from firebase_admin import auth
from admin import admin_bp

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "a-super-secret-key-for-local-testing")
app.register_blueprint(admin_bp)

# --- Load local company data into memory for searching ---
try:
    company_df = pd.read_csv('indian_stock_tickers.csv')
    company_df['BSE Code'] = company_df['BSE Code'].astype(str).fillna('')
except FileNotFoundError:
    print("[CRITICAL ERROR] The company list 'indian_stock_tickers.csv' was not found. Search will not work.")
    company_df = pd.DataFrame(columns=['BSE Code', 'Company Name'])


# --- Unified Authentication Routes ---
@app.route('/login')
def login():
    """Renders the new unified login page."""
    return render_template('login_unified.html')

@app.route('/verify_phone_token', methods=['POST'])
def verify_phone_token():
    """Verifies the Firebase phone token and logs the user into Supabase."""
    id_token = request.json.get('token')
    if not id_token:
        return jsonify({"success": False, "error": "No token provided."}), 400

    try:
        decoded_token = auth.verify_id_token(id_token)
        user_result = db.find_or_create_supabase_user(decoded_token)

        if user_result.get('error'):
            return jsonify({"success": False, "error": user_result['error']}), 401
        
        # Prefer Supabase session if available; otherwise fall back to app session with email
        if user_result.get('session'):
            session_data = user_result['session']
            session['access_token'] = session_data.get('access_token')
            session['refresh_token'] = session_data.get('refresh_token')
            session['user_email'] = session_data.get('user', {}).get('email') or user_result.get('email')
            session['user_id'] = user_result.get('user_id')
            session['user_phone'] = user_result.get('phone')
        else:
            # No Supabase session returned; still mark as logged in via email for app access
            session['user_email'] = user_result.get('email')
            session['user_id'] = user_result.get('user_id')
            session['user_phone'] = user_result.get('phone')

        if session.get('user_email'):
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "Authentication succeeded but no user context available."}), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/verify_google_token', methods=['POST'])
def verify_google_token():
    """Verifies the Firebase Google token and logs the user into Supabase."""
    id_token = request.json.get('token')
    if not id_token:
        return jsonify({"success": False, "error": "No token provided."}), 400

    try:
        decoded_token = auth.verify_id_token(id_token)
        user_result = db.find_or_create_supabase_user(decoded_token)

        if user_result.get('error'):
            return jsonify({"success": False, "error": user_result['error']}), 401
        
        # Prefer Supabase session if available; otherwise fall back to app session with email
        if user_result.get('session'):
            session_data = user_result['session']
            session['access_token'] = session_data.get('access_token')
            session['refresh_token'] = session_data.get('refresh_token')
            session['user_email'] = session_data.get('user', {}).get('email') or user_result.get('email')
            session['user_id'] = user_result.get('user_id')
            session['user_phone'] = user_result.get('phone')
        else:
            session['user_email'] = user_result.get('email')
            session['user_id'] = user_result.get('user_id')
            session['user_phone'] = user_result.get('phone')

        if session.get('user_email'):
            return jsonify({"success": True})
        else:
            return jsonify({"success": False, "error": "Authentication succeeded but no user context available."}), 500

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

# --- Helper function to get an authenticated Supabase client ---
def get_authenticated_client():
    # If full Supabase session is present, use it
    access_token = session.get('access_token')
    refresh_token = session.get('refresh_token')
    if access_token and refresh_token:
        sb = db.get_supabase_client()
        try:
            sb.auth.set_session(access_token, refresh_token)
            return sb
        except Exception as e:
            print(f"Session authentication error: {e}")

    # Otherwise, allow read/write via service client gated by Flask session
    if session.get('user_email'):
        return db.get_supabase_client(service_role=True)

    return None

# --- Main Dashboard Route (Protected) ---
@app.route('/')
def dashboard():
    sb = get_authenticated_client()
    if not sb:
        # Redirect to the new unified login page if not logged in
        return redirect(url_for('login'))

    user_id = session.get('user_id')
    monitored_scrips = db.get_user_scrips(sb, user_id)
    telegram_recipients = db.get_user_recipients(sb, user_id)
    
    return render_template('dashboard.html', 
                           monitored_scrips=monitored_scrips,
                           telegram_recipients=telegram_recipients,
                           user_email=session.get('user_email', ''),
                           user_phone=session.get('user_phone', ''))

# --- Search Endpoint for Fuzzy Logic ---
@app.route('/search')
def search():
    # Allow users logged in via email-only session as well
    if not session.get('user_email'):
        return jsonify({"error": "Unauthorized"}), 401

    query = request.args.get('query', '')
    if not query or len(query) < 2:
        return jsonify({"matches": []})
    
    mask = (company_df['Company Name'].str.contains(query, case=False, na=False)) | \
           (company_df['BSE Code'].str.startswith(query))
           
    matches = company_df[mask].head(10)
    return jsonify({"matches": matches.to_dict('records')})


# --- Data Management Routes (Protected) ---
@app.route('/add_scrip', methods=['POST'])
def add_scrip():
    sb = get_authenticated_client()
    if not sb: return redirect(url_for('login'))
    
    user_id = session.get('user_id')
    bse_code = request.form['scrip_code']
    company_name = request.form.get('company_name', '').strip()

    # If company name is not provided, look it up exactly by BSE Code from CSV
    if not company_name:
        try:
            match = company_df[company_df['BSE Code'] == bse_code]
            if not match.empty:
                company_name = str(match.iloc[0]['Company Name'])
            else:
                flash('Scrip code not found in list. Please check the BSE code.', 'error')
                return redirect(url_for('dashboard'))
        except Exception:
            flash('There was an issue looking up the scrip. Try again.', 'error')
            return redirect(url_for('dashboard'))

    db.add_user_scrip(sb, user_id, bse_code, company_name)
    return redirect(url_for('dashboard'))

@app.route('/delete_scrip', methods=['POST'])
def delete_scrip():
    sb = get_authenticated_client()
    if not sb: return redirect(url_for('login'))

    user_id = session.get('user_id')
    bse_code = request.form['scrip_code']
    db.delete_user_scrip(sb, user_id, bse_code)
    return redirect(url_for('dashboard'))

@app.route('/add_recipient', methods=['POST'])
def add_recipient():
    sb = get_authenticated_client()
    if not sb: return redirect(url_for('login'))
    
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    db.add_user_recipient(sb, user_id, chat_id)
    return redirect(url_for('dashboard'))

@app.route('/delete_recipient', methods=['POST'])
def delete_recipient():
    sb = get_authenticated_client()
    if not sb: return redirect(url_for('login'))
    
    user_id = session.get('user_id')
    chat_id = request.form['chat_id']
    db.delete_user_recipient(sb, user_id, chat_id)
    return redirect(url_for('dashboard'))


if __name__ == '__main__':
    db.get_supabase_client()
    db.initialize_firebase() # Initialize Firebase on startup
    app.run(host='0.0.0.0', port=5001, debug=True)
