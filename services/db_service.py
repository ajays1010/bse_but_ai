from typing import List, Dict, Any, Optional, Callable, Tuple
from functools import wraps
from supabase import create_client, Client
from datetime import datetime

from .config import AppConfig
from .logger import log_message


supabase: Optional[Client] = None

# --- Supabase Client Initialization ---
def get_supabase_client() -> Optional[Client]:
    global supabase
    if supabase is None:
        if not AppConfig.SUPABASE_URL or not AppConfig.SUPABASE_KEY:
            log_message("CRITICAL ERROR: Supabase URL or Key not set. Service-level operations will fail.")
            return None
        try:
            supabase = create_client(AppConfig.SUPABASE_URL, AppConfig.SUPABASE_KEY)
            log_message("Supabase service client initialized.")
        except Exception as e:
            log_message(f"Failed to initialize Supabase client: {e}")
            return None
    return supabase

def get_user_client(access_token: str) -> Optional[Client]:
    """Get a user-scoped client for RLS-bound queries."""
    if not AppConfig.SUPABASE_URL or not AppConfig.SUPABASE_ANON_KEY:
        log_message("CRITICAL ERROR: Supabase URL or Anon Key not set. User operations will fail.")
        return None
    try:
        client = create_client(AppConfig.SUPABASE_URL, AppConfig.SUPABASE_ANON_KEY)
        # Set the access token for RLS
        client.auth.set_session(access_token, refresh_token="")
        return client
    except Exception as e:
        log_message(f"Failed to create user client: {e}")
        return None

def with_auto_refresh(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs) -> Tuple[Any, Optional[Dict[str, str]]]:
        access_token = kwargs.get('access_token')
        refresh_token = kwargs.get('refresh_token')
        
        try:
            result = func(*args, **kwargs)
            return result, None
        except Exception as e:
            msg = str(e)
            if refresh_token and ('JWT expired' in msg or 'PGRST303' in msg):
                log_message(f"JWT expired on {func.__name__}. Attempting refresh.")
                refreshed = auth_refresh(refresh_token)

                if refreshed and refreshed.get('access_token'):
                    log_message("Token refresh successful. Retrying the operation.")
                    kwargs['access_token'] = refreshed['access_token']
                    kwargs['refresh_token'] = refreshed.get('refresh_token', refresh_token)
                    new_tokens = {
                        'access_token': refreshed['access_token'],
                        'refresh_token': refreshed.get('refresh_token', refresh_token),
                    }
                    try:
                        result = func(*args, **kwargs)
                        return result, new_tokens
                    except Exception as e2:
                        log_message(f"Operation '{func.__name__}' failed even after token refresh: {e2}")
                else:
                    log_message("Token refresh failed. Session is invalid.")
            else:
                log_message(f"An unexpected error occurred in '{func.__name__}': {e}")
        return None, None
        
    return wrapper

# --- Authentication Functions ---
def auth_sign_in(email: str, password: str) -> Optional[dict]:
    sb = get_supabase_client()
    if not sb:
        return None
    try:
        response = sb.auth.sign_in_with_password({"email": email, "password": password})
        if response.user and response.session:
            return {
                'access_token': response.session.access_token,
                'refresh_token': response.session.refresh_token,
                'user_id': response.user.id
            }
    except Exception as e:
        log_message(f"Auth sign in failed: {e}")
    return None

def auth_sign_up(email: str, password: str, full_name: str = "") -> bool:
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        response = sb.auth.sign_up({"email": email, "password": password})
        if response.user:
            # Create user profile
            try:
                sb.table('app_users').insert({
                    'id': response.user.id, 'email': email, 'full_name': full_name
                }).execute()
            except Exception as profile_error:
                log_message(f"Failed to create user profile in app_users table: {profile_error}")
                # If we can't create the profile due to RLS issues, 
                # still return True since the auth user was created successfully
                # The profile can be created later or handled differently
            return True
    except Exception as e:
        log_message(f"Auth sign up failed: {e}")
    return False

def auth_refresh(refresh_token: str) -> Optional[dict]:
    sb = get_supabase_client()
    if not sb:
        return None
    try:
        response = sb.auth.refresh_session(refresh_token)
        if response.session:
            return {
                'access_token': response.session.access_token,
                'refresh_token': response.session.refresh_token,
                'user_id': response.user.id if response.user else None
            }
    except Exception as e:
        log_message(f"Auth refresh failed: {e}")
    return None

def auth_get_user(access_token: str) -> bool:
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        response = sb.auth.get_user(access_token)
        return bool(response.user)
    except Exception:
        return False

# --- User-scoped Functions (RLS-bound) ---
@with_auto_refresh
def user_read_subscriptions(*, access_token: str, refresh_token: Optional[str] = None, **kwargs) -> Optional[list[dict]]:
    client = get_user_client(access_token)
    if not client:
        return None
    try:
        return client.table('user_scrip_subscriptions').select('bse_code, company_name').execute().data or []
    except Exception as e:
        log_message(f"Read user subscriptions failed: {e}")
        raise e

@with_auto_refresh
def user_add_subscription(*, user_id: str, bse_code: str, company_name: str, access_token: str, refresh_token: Optional[str] = None, **kwargs) -> Optional[bool]:
    client = get_user_client(access_token)
    if not client:
        return None
    try:
        client.table('user_scrip_subscriptions').insert({
            'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name
        }).execute()
        return True
    except Exception as e:
        log_message(f"Add user subscription failed: {e}")
        raise e

@with_auto_refresh
def user_delete_subscription(*, bse_code: str, access_token: str, refresh_token: Optional[str] = None, **kwargs) -> Optional[bool]:
    client = get_user_client(access_token)
    if not client:
        return None
    try:
        client.table('user_scrip_subscriptions').delete().eq('bse_code', bse_code).execute()
        return True
    except Exception as e:
        log_message(f"Delete user subscription failed: {e}")
        raise e

@with_auto_refresh
def user_list_telegrams(*, user_id: str, access_token: str, refresh_token: Optional[str] = None, **kwargs) -> Optional[list[dict]]:
    client = get_user_client(access_token)
    if not client:
        return None
    try:
        return client.table('user_telegram_ids').select('chat_id, created_at').execute().data or []
    except Exception as e:
        log_message(f"List user telegrams failed: {e}")
        raise e

@with_auto_refresh
def user_add_telegram(*, user_id: str, chat_id: str, access_token: str, refresh_token: Optional[str] = None, **kwargs) -> Optional[bool]:
    client = get_user_client(access_token)
    if not client:
        return None
    try:
        client.table('user_telegram_ids').insert({
            'user_id': user_id, 'chat_id': chat_id
        }).execute()
        return True
    except Exception as e:
        log_message(f"Add user telegram failed: {e}")
        raise e

@with_auto_refresh
def user_delete_telegram(*, user_id: str, chat_id: str, access_token: str, refresh_token: Optional[str] = None, **kwargs) -> Optional[bool]:
    client = get_user_client(access_token)
    if not client:
        return None
    try:
        client.table('user_telegram_ids').delete().eq('chat_id', chat_id).execute()
        return True
    except Exception as e:
        log_message(f"Delete user telegram failed: {e}")
        raise e

@with_auto_refresh
def user_get_profile(*, user_id: str, access_token: str, refresh_token: Optional[str] = None, **kwargs) -> Optional[dict]:
    """Return profile pulling role from app_users.role (source of truth)."""
    client = get_user_client(access_token)
    if not client:
        return None
    try:
        # Basic identity
        user_response = client.auth.get_user()
        if not user_response or not user_response.user:
            return None
        auth_user = user_response.user
        email = auth_user.email or ''
        metadata = auth_user.user_metadata or {}
        full_name = metadata.get('full_name') or metadata.get('name') or ''
        # Role from app_users (use service client to avoid RLS recursion in policies)
        role = 'user'
        sb_service = get_supabase_client()
        if sb_service:
            try:
                role_row = sb_service.table('app_users').select('role').eq('id', auth_user.id).limit(1).execute().data or []
                if role_row:
                    rv = (role_row[0] or {}).get('role')
                    if isinstance(rv, str) and rv.strip():
                        role = rv.strip().lower()
            except Exception as e:
                log_message(f"user_get_profile role lookup failed (service): {e}")
        else:
            # Fallback on user client if service client unavailable
            try:
                role_row = client.table('app_users').select('role').eq('id', auth_user.id).limit(1).execute().data or []
                if role_row:
                    rv = (role_row[0] or {}).get('role')
                    if isinstance(rv, str) and rv.strip():
                        role = rv.strip().lower()
            except Exception as e:
                log_message(f"user_get_profile role lookup failed (scoped): {e}")
        profile = {
            'id': auth_user.id,
            'email': email,
            'full_name': full_name,
            'role': role,
            'created_at': auth_user.created_at,
        }
        return profile
    except Exception as e:
        log_message(f"user_get_profile failed: {e}")
        return None

# --- Admin Functions (Service role) ---
def is_admin_by_user_id(user_id: str) -> bool:
    """Check admin by reading role from app_users with SERVICE client (avoid RLS recursion).
    Treats admin/superuser as admin.
    """
    if not user_id:
        return False
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        # Use a minimal select that cannot recurse via policy functions
        data = sb.table('app_users').select('role').eq('id', user_id).limit(1).execute().data or []
        if data:
            role_value = (data[0] or {}).get('role')
            return isinstance(role_value, str) and role_value.strip().lower() in {"admin", "superuser"}
        return False
    except Exception as e:
        # As a last resort, try user-scoped client (may still be blocked by RLS)
        try:
            # This path requires the caller to pass a valid access token; not available here.
            # So we just log and return False to avoid recursion errors.
            pass
        except Exception:
            pass
        log_message(f"is_admin_by_user_id failed: {e}")
        return False

def admin_list_users() -> list[dict]:
    """List users from app_users (service role)."""
    sb = get_supabase_client()
    if not sb:
        return []
    try:
        resp = sb.table('app_users').select('id, email, full_name, role, created_at').order('created_at').execute()
        return resp.data or []
    except Exception as e:
        log_message(f"Admin list users failed: {e}")
        return []

def admin_list_users_scoped(access_token: str) -> list[dict]:
    """Lists users via user-scoped client so RLS is enforced.
    Admins (via JWT claim/policy) will see all; non-admins will see only themselves.
    """
    client = get_user_client(access_token)
    if not client:
        return []
    try:
        resp = client.table('app_users').select('id, email, full_name, role, created_at').order('created_at').execute()
        return resp.data or []
    except Exception as e:
        log_message(f"Admin list users (scoped) failed: {e}")
        return []


def admin_list_users_for_interface(access_token: str) -> list[dict]:
    """List users for admin UI from app_users using service role."""
    sb = get_supabase_client()
    if not sb:
        return []
    try:
        resp = sb.table('app_users').select('id, email, full_name, role, created_at').order('created_at').execute()
        return resp.data or []
    except Exception as e:
        log_message(f"Admin list users for interface failed: {e}")
        return []


def is_admin(user_id: str, access_token: Optional[str] = None) -> bool:
    """Alias to is_admin_by_user_id (role in app_users)."""
    return is_admin_by_user_id(user_id)


def service_fetch_profile_by_user_id(user_id: str) -> Optional[dict]:
    """Fetch a basic profile using the SERVICE client only (no user token required).
    Returns: { id, email, full_name, role, created_at } or None.
    """
    if not user_id:
        return None
    sb = get_supabase_client()
    if not sb:
        return None
    email = ''
    full_name = ''
    created_at = ''
    # NOTE: Do not call auth.admin here; some environments don't expose admin with provided key
    # Role from app_users
    role = 'user'
    try:
        row = sb.table('app_users').select('email, full_name, role, created_at').eq('id', user_id).limit(1).execute().data or []
        if row:
            r = row[0] or {}
            role_val = r.get('role')
            if isinstance(role_val, str) and role_val.strip():
                role = role_val.strip().lower()
            email = r.get('email') or email
            full_name = r.get('full_name') or full_name
            created_at = r.get('created_at') or created_at
    except Exception as e:
        log_message(f"service_fetch_profile_by_user_id table read failed: {e}")
    return {
        'id': user_id,
        'email': email,
        'full_name': full_name,
        'role': role,
        'created_at': created_at or '1970-01-01T00:00:00Z'
    }


## Removed bootstrap_admin_if_none: revert to table-driven roles only

def admin_list_user_subs(user_id: str) -> list[dict]:
    sb = get_supabase_client()
    if not sb:
        return []
    try:
        return sb.table('user_scrip_subscriptions').select('bse_code, company_name').eq('user_id', user_id).execute().data or []
    except Exception as e:
        log_message(f"Admin list user subs failed: {e}")
        return []

def admin_upsert_user_sub(user_id: str, bse_code: str, company_name: str) -> bool:
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        sb.table('user_scrip_subscriptions').upsert({
            'user_id': user_id, 'bse_code': bse_code, 'company_name': company_name
        }).execute()
        return True
    except Exception as e:
        log_message(f"Admin upsert user sub failed: {e}")
        return False

def admin_delete_user_sub(user_id: str, bse_code: str) -> bool:
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        sb.table('user_scrip_subscriptions').delete().eq('user_id', user_id).eq('bse_code', bse_code).execute()
        return True
    except Exception as e:
        log_message(f"Admin delete user sub failed: {e}")
        return False

def admin_list_user_telegrams(user_id: str) -> list[dict]:
    sb = get_supabase_client()
    if not sb:
        return []
    try:
        return sb.table('user_telegram_ids').select('chat_id, created_at').eq('user_id', user_id).execute().data or []
    except Exception as e:
        log_message(f"Admin list user telegrams failed: {e}")
        return []

def admin_add_user_telegram(user_id: str, chat_id: str) -> bool:
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        # Upsert to avoid duplicates on same (user_id, chat_id)
        sb.table('user_telegram_ids').upsert({
            'user_id': user_id, 'chat_id': chat_id
        }, on_conflict='chat_id').execute()
        return True
    except Exception as e:
        log_message(f"Admin add user telegram failed: {e}")
        return False

def admin_delete_user_telegram(user_id: str, chat_id: str) -> bool:
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        sb.table('user_telegram_ids').delete().eq('user_id', user_id).eq('chat_id', chat_id).execute()
        return True
    except Exception as e:
        log_message(f"Admin delete user telegram failed: {e}")
        return False

# --- Legacy Functions for Scheduler ---
def db_read_all_subscribed_scrips() -> list[dict]:
    """Returns all unique scrip codes across all users (for scheduler)."""
    sb = get_supabase_client()
    if not sb:
        log_message("DB Read all subscribed scrips failed: no client")
        return []
    try:
        data = sb.table('user_scrip_subscriptions').select('bse_code, company_name').execute().data or []
        # Deduplicate by bse_code
        seen = set()
        unique_scrips = []
        for item in data:
            code = item.get('bse_code')
            if code and code not in seen:
                seen.add(code)
                unique_scrips.append(item)
        return unique_scrips
    except Exception as e:
        log_message(f"DB Read all subscribed scrips failed: {e}")
        return []

def db_get_seen_announcements_for_today() -> List[Dict[str, Any]]:
    sb = get_supabase_client()
    if not sb:
        return []
    try:
        today = datetime.now(AppConfig.IST).date().isoformat()
        return sb.table('seen_announcements').select('*').gte('ann_date', today).execute().data or []
    except Exception as e:
        log_message(f"DB Get seen announcements failed: {e}")
        return []

def db_check_if_announcement_seen_by_user(news_id: str, user_id: str) -> bool:
    """Check if a specific user has seen this announcement"""
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        data = sb.table('seen_announcements').select('id').eq('news_id', news_id).eq('user_id', user_id).execute().data
        return len(data) > 0
    except Exception as e:
        log_message(f"DB Check announcement seen by user failed: {e}")
        return False

def db_check_if_pdf_seen_by_user(pdf_name: str, user_id: str) -> bool:
    """Check if a specific user has already received an announcement with the same PDF file name."""
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        data = sb.table('seen_announcements').select('id').eq('pdf_name', pdf_name).eq('user_id', user_id).execute().data
        return len(data) > 0
    except Exception as e:
        log_message(f"DB Check PDF seen by user failed: {e}")
        return False

def db_check_if_announcement_seen(news_id: str) -> bool:
    """Legacy function - checks if announcement exists globally (for backwards compatibility)"""
    sb = get_supabase_client()
    if not sb:
        return False
    try:
        data = sb.table('seen_announcements').select('id').eq('news_id', news_id).execute().data
        return len(data) > 0
    except Exception as e:
        log_message(f"DB Check announcement seen failed: {e}")
        return False

def db_save_announcement_for_user(news_id: str, user_id: str, scrip_code: str, headline: str, pdf_name: str, ann_date_ist, caption: str) -> None:
    """Save announcement as seen for a specific user"""
    from datetime import datetime
    sb = get_supabase_client()
    if not sb:
        return
    try:
        # Ensure ann_date_ist is not None and convert to date string
        if ann_date_ist is None:
            ann_date_ist = datetime.now(AppConfig.IST)
        
        sb.table('seen_announcements').insert({
            'user_id': user_id,
            'news_id': news_id,
            'scrip_code': scrip_code,
            'headline': headline,
            'pdf_name': pdf_name,
            'ann_date': ann_date_ist.date().isoformat(),
            'caption': caption,
            'created_at': datetime.now(AppConfig.IST).isoformat()
        }).execute()
    except Exception as e:
        log_message(f"DB Save announcement for user failed: {e}")

def db_save_announcement(news_id: str, scrip_code: str, headline: str, pdf_name: str, ann_date_ist, caption: str) -> None:
    """Legacy function - saves announcement globally (for backwards compatibility)"""
    from datetime import datetime
    sb = get_supabase_client()
    if not sb:
        return
    try:
        # Ensure ann_date_ist is not None and convert to date string
        if ann_date_ist is None:
            ann_date_ist = datetime.now(AppConfig.IST)
        
        sb.table('seen_announcements').insert({
            'news_id': news_id,
            'scrip_code': scrip_code,
            'headline': headline,
            'pdf_name': pdf_name,
            'ann_date': ann_date_ist.date().isoformat(),  # Use ann_date instead of announcement_date
            'caption': caption,
            'created_at': datetime.now(AppConfig.IST).isoformat()
        }).execute()
    except Exception as e:
        log_message(f"DB Save announcement failed: {e}")

# --- Legacy Functions (Deprecated, kept for compatibility) ---
def db_read_monitored_scrips() -> List[Dict[str, Any]]:
    """Legacy function - use admin functions or user-scoped functions instead."""
    return db_read_all_subscribed_scrips()

def db_read_telegram_recipients() -> List[Dict[str, Any]]:
    """Legacy function - use admin functions or user-scoped functions instead."""
    sb = get_supabase_client()
    if not sb:
        return []
    try:
        return sb.table('user_telegram_ids').select('chat_id').execute().data or []
    except Exception as e:
        log_message(f"DB Read telegram recipients failed: {e}")
        return []