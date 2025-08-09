import os
from dotenv import load_dotenv
from pytz import timezone

load_dotenv()


class AppConfig:
    TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
    TELEGRAM_CAPTION_LIMIT = 4096

    SUPABASE_URL = os.environ.get('SUPABASE_URL')
    SUPABASE_KEY = os.environ.get('SUPABASE_KEY')
    SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY')

    CRON_SECRET_KEY = os.environ.get('CRON_SECRET_KEY', 'local-dev-secret')
    BSE_API_URL = "https://api.bseindia.com/BseIndiaAPI/api/AnnGetData/w"
    PDF_BASE_URL = "https://www.bseindia.com/xml-data/corpfiling/AttachLive/"
    COMPANY_LIST_CSV = 'bse_company_list_cleaned.csv'

    IST = timezone('Asia/Kolkata')

    GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY') or os.environ.get('GOOGLE_API_KEY')
    GEMINI_MODEL = os.environ.get('GEMINI_MODEL', 'gemini-1.5-pro')  # More powerful model for better analysis

    # --- Resource / Memory Guard ---
    # Hard limit target: 512MB box. Keep soft limit lower to leave headroom for OS.
    MEMORY_SOFT_LIMIT_MB = float(os.environ.get('MEMORY_SOFT_LIMIT_MB', 420))
    MEMORY_HARD_LIMIT_MB = float(os.environ.get('MEMORY_HARD_LIMIT_MB', 500))
    MEMORY_CHECK_INTERVAL_SEC = int(os.environ.get('MEMORY_CHECK_INTERVAL_SEC', 5))
    DISABLE_AI_ON_LOW_MEMORY = os.environ.get('DISABLE_AI_ON_LOW_MEMORY', 'true').lower() in ('1', 'true', 'yes')

    # Deprecated: admin allowlists removed in favor of DB roles

    # --- Public Base URL for deep links (prefer provider-provided var) ---
    APP_BASE_URL = (
        os.environ.get('APP_BASE_URL')
        or os.environ.get('RENDER_EXTERNAL_URL')
        or os.environ.get('RAILWAY_PUBLIC_DOMAIN')
        or ''
    ).rstrip('/')


