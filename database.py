import os
from supabase import create_client, Client
from gotrue.errors import AuthApiError
import firebase_admin
from firebase_admin import credentials, auth

# --- Firebase Admin SDK Initialization ---
firebase_app = None

def initialize_firebase():
    """Initializes the Firebase Admin SDK using a direct path to the service account key."""
    global firebase_app
    if firebase_app:
        return

    key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    # Fallback to local service account file in repo if env var is not set
    if (not key_path or not os.path.exists(key_path)) and os.path.exists(
        "bsemonitoring-64a8e-firebase-adminsdk-fbsvc-cb5ca4b412.json"
    ):
        key_path = "bsemonitoring-64a8e-firebase-adminsdk-fbsvc-cb5ca4b412.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    if not key_path or not os.path.exists(key_path):
        print("CRITICAL ERROR: Firebase service account key not found.")
        return

    try:
        cred = credentials.Certificate(key_path)
        firebase_app = firebase_admin.initialize_app(cred)
        print("Firebase Admin SDK initialized successfully.")
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to initialize Firebase Admin SDK: {e}")

# --- Supabase Client Initialization ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")

supabase_anon: Client = None
supabase_service: Client = None

def get_supabase_client(service_role=False):
    """Initializes and returns the appropriate Supabase client."""
    global supabase_anon, supabase_service
    if service_role:
        if supabase_service is None:
            if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
                print("CRITICAL: Supabase Service Key not set.")
                return None
            supabase_service = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
        return supabase_service
    else:
        if supabase_anon is None:
            if not SUPABASE_URL or not SUPABASE_KEY:
                print("CRITICAL: Supabase Anon Key not set.")
                return None
            supabase_anon = create_client(SUPABASE_URL, SUPABASE_KEY)
        return supabase_anon

# --- Unified User Authentication Logic ---
def find_or_create_supabase_user(decoded_token):
    """
    Finds a user in Supabase by their Firebase/Google UID or email.
    If not found, creates a new user. Returns a new Supabase session.
    """
    # Ensure Firebase Admin SDK is initialized
    initialize_firebase()

    sb_admin = get_supabase_client(service_role=True)
    if not sb_admin:
        return {"session": None, "error": "Admin client not configured."}

    provider_uid = decoded_token['uid']
    
    # Prefer values present in the verified token
    email = decoded_token.get('email')
    phone_number = decoded_token.get('phone_number')

    try:
        # Only call Admin API if we still miss fields
        if not email or not phone_number:
            firebase_user_record = auth.get_user(provider_uid)
            email = email or firebase_user_record.email
            phone_number = phone_number or firebase_user_record.phone_number

            if not email and firebase_user_record.provider_data:
                for provider_info in firebase_user_record.provider_data:
                    if provider_info.email:
                        email = provider_info.email
                        break
    except Exception:
        # Ignore Admin lookup failures; we keep whatever we have from the token
        pass

    provider = decoded_token['firebase']['sign_in_provider']
    uid_column = 'google_uid' if provider == 'google.com' else 'firebase_uid'

    # 1. Try to find an existing user
    profile_response = sb_admin.table('profiles').select('id, email').eq(uid_column, provider_uid).execute()
    profile = profile_response.data[0] if profile_response.data else None
    
    if not profile and email:
        profile_response = sb_admin.table('profiles').select('id, email').eq('email', email).execute()
        profile = profile_response.data[0] if profile_response.data else None
        if profile:
            sb_admin.table('profiles').update({uid_column: provider_uid}).eq('id', profile['id']).execute()

    # If we found an existing profile, return identifiers and allow app session login
    if profile:
        # If we have a better email now, update profiles and auth.users when placeholder is present
        if email and (not profile.get('email') or profile.get('email', '').endswith('@yourapp.com')):
            try:
                sb_admin.table('profiles').update({'email': email}).eq('id', profile['id']).execute()
                try:
                    sb_admin.auth.admin.update_user(profile['id'], {'email': email})
                except Exception:
                    # Non-fatal if auth update fails
                    pass
                profile['email'] = email
            except Exception:
                pass
        return {
            "session": None,
            "email": profile['email'],
            "user_id": profile['id'],
            "phone": phone_number,
            "error": None,
        }

    # 3. If no user is found, create a new one
    try:
        user_attrs = {}
        if email:
            user_attrs['email'] = email
        elif phone_number:
            user_attrs['phone'] = phone_number
            user_attrs['email'] = f"{phone_number}@yourapp.com"
        else:
            user_attrs['email'] = f"{provider_uid}@yourapp.com"

        new_user_response = sb_admin.auth.admin.create_user(user_attrs)
        new_user = new_user_response.user
        
        sb_admin.table('profiles').update({uid_column: provider_uid}).eq('id', new_user.id).execute()
        
        # Skip generating Supabase session links; authenticate app-side via Flask session
        return {
            "session": None,
            "email": new_user.email,
            "user_id": new_user.id,
            "phone": phone_number,
            "error": None,
        }

    except Exception as e:
        return {"session": None, "email": email, "user_id": None, "phone": phone_number, "error": str(e)}


# --- User-Specific Data Functions (Remain the same) ---
def get_user_scrips(user_client, user_id: str):
    return (
        user_client
        .table('monitored_scrips')
        .select('bse_code, company_name')
        .eq('user_id', user_id)
        .execute()
        .data or []
    )

def get_user_recipients(user_client, user_id: str):
    return (
        user_client
        .table('telegram_recipients')
        .select('chat_id')
        .eq('user_id', user_id)
        .execute()
        .data or []
    )

def add_user_scrip(user_client, user_id: str, bse_code: str, company_name: str):
    user_client.table('monitored_scrips').insert({'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name}).execute()

def delete_user_scrip(user_client, user_id: str, bse_code: str):
    user_client.table('monitored_scrips').delete().eq('user_id', user_id).eq('bse_code', bse_code).execute()

def add_user_recipient(user_client, user_id: str, chat_id: str):
    user_client.table('telegram_recipients').upsert({'user_id': user_id, 'chat_id': chat_id}).execute()

def delete_user_recipient(user_client, user_id: str, chat_id: str):
    user_client.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).execute()


# --- Admin helpers ---
def admin_get_all_users():
    sb_admin = get_supabase_client(service_role=True)
    resp = sb_admin.table('profiles').select('id, email').order('email').execute()
    return resp.data or []

def admin_get_user_details(user_id: str):
    sb_admin = get_supabase_client(service_role=True)
    profile = sb_admin.table('profiles').select('id, email').eq('id', user_id).single().execute().data
    scrips = sb_admin.table('monitored_scrips').select('bse_code, company_name').eq('user_id', user_id).execute().data or []
    recipients = sb_admin.table('telegram_recipients').select('chat_id').eq('user_id', user_id).execute().data or []
    return {
        'id': profile['id'],
        'email': profile.get('email', ''),
        'scrips': scrips,
        'recipients': recipients,
    }

def admin_add_scrip_for_user(user_id: str, bse_code: str, company_name: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('monitored_scrips').insert({'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name}).execute()

def admin_delete_scrip_for_user(user_id: str, bse_code: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('monitored_scrips').delete().eq('user_id', user_id).eq('bse_code', bse_code).execute()

def admin_add_recipient_for_user(user_id: str, chat_id: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('telegram_recipients').upsert({'user_id': user_id, 'chat_id': chat_id}).execute()

def admin_delete_recipient_for_user(user_id: str, chat_id: str):
    sb_admin = get_supabase_client(service_role=True)
    sb_admin.table('telegram_recipients').delete().eq('user_id', user_id).eq('chat_id', chat_id).execute()
