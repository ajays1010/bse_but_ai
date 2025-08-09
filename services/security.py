from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional

from .config import AppConfig


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode('utf-8').rstrip('=')


def _b64url_decode(s: str) -> bytes:
    pad = '=' * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def sign_token(payload: Dict[str, Any], expires_in_seconds: int = 3 * 24 * 3600) -> str:
    """Create a signed token carrying small payload (e.g., user_id, news_id)."""
    secret = (AppConfig.CRON_SECRET_KEY or "default-secret").encode('utf-8')
    body = dict(payload)
    body['exp'] = int(time.time()) + int(expires_in_seconds)
    encoded = _b64url(json.dumps(body, separators=(',', ':'), ensure_ascii=False).encode('utf-8'))
    sig = hmac.new(secret, encoded.encode('utf-8'), hashlib.sha256).digest()
    return encoded + '.' + _b64url(sig)


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        encoded, sig = token.split('.', 1)
        secret = (AppConfig.CRON_SECRET_KEY or "default-secret").encode('utf-8')
        expected = _b64url(hmac.new(secret, encoded.encode('utf-8'), hashlib.sha256).digest())
        if not hmac.compare_digest(expected, sig):
            return None
        body = json.loads(_b64url_decode(encoded).decode('utf-8'))
        if int(body.get('exp', 0)) < int(time.time()):
            return None
        return body
    except Exception:
        return None


