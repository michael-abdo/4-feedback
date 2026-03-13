#!/usr/bin/env python3
"""
Tradovate Token Refresh Script

Automatically refreshes the bearer token for Tradovate API access.
Stores token in .tradovate_token file.

Usage:
    python3 tradovate_auth.py              # Refresh token
    python3 tradovate_auth.py --check      # Check current token status
    python3 tradovate_auth.py --env live   # Use live environment (default: demo)
"""

import argparse
import base64
import json
import hashlib
import requests
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Tuple

# File paths
SCRIPT_DIR = Path(__file__).parent
TOKEN_FILE = SCRIPT_DIR / ".tradovate_token"
CREDENTIALS_FILE = SCRIPT_DIR / ".tradovate_credentials"

# API endpoints
ENDPOINTS = {
    "demo": {
        "auth": "https://demo.tradovateapi.com/v1/auth/accesstokenrequest",
        "report": "https://rpt-demo.tradovateapi.com/v1/reports/requestreport",
    },
    "live": {
        "auth": "https://live.tradovateapi.com/v1/auth/accesstokenrequest",
        "report": "https://rpt-live.tradovateapi.com/v1/reports/requestreport",
    }
}

# Default credentials (from Tradovate web app)
DEFAULT_APP_ID = "tradovate_trader(web)"
DEFAULT_APP_VERSION = "3.260109.0"
DEFAULT_CID = "1"
DEFAULT_SEC = "43970b00d445313059d4df23f80720f2f1a8965fb58d3906546f0c917f994995"


def encode_password_tradovate(password: str) -> str:
    """
    Encode password using Tradovate's encoding scheme.
    Pattern: reverse all characters except the last, then base64 encode.
    Example: "A#n4uWPJD7eB" -> reverse "A#n4uWPJD7e" -> "e7DJPWu4n#A" + "B" -> base64
    """
    if len(password) <= 1:
        return base64.b64encode(password.encode('utf-8')).decode('utf-8')

    # Reverse all but last character, keep last character at end
    transformed = password[:-1][::-1] + password[-1]
    return base64.b64encode(transformed.encode('utf-8')).decode('utf-8')


def generate_challenge() -> str:
    """Generate a challenge value (timestamp-based like web app)."""
    import time
    return str(int(time.time() * 1000))


def load_credentials() -> Optional[dict]:
    """Load credentials from file."""
    if not CREDENTIALS_FILE.exists():
        return None
    try:
        return json.loads(CREDENTIALS_FILE.read_text())
    except Exception:
        return None


def save_credentials(creds: dict):
    """Save credentials to file."""
    CREDENTIALS_FILE.write_text(json.dumps(creds, indent=2))
    CREDENTIALS_FILE.chmod(0o600)  # Restrict permissions
    print(f"Credentials saved to {CREDENTIALS_FILE}")


def get_access_token(
    username: str,
    password: str,
    environment: str = "demo",
    device_id: Optional[str] = None,
    password_encoded: Optional[str] = None,
    challenge: Optional[str] = None,
) -> Tuple[bool, str, Optional[dict]]:
    """
    Request access token from Tradovate.
    Returns: (success, message, token_data)

    If password_encoded and challenge are provided, uses those directly.
    Otherwise attempts to encode the password.
    """
    # Use provided device_id or generate one
    if not device_id:
        import uuid
        device_id = str(uuid.uuid4())

    # Use pre-encoded password if available, otherwise encode
    if password_encoded and challenge:
        enc_pwd = password_encoded
        chl = challenge
    else:
        enc_pwd = encode_password_tradovate(password)
        chl = generate_challenge()

    payload = {
        "name": username,
        "password": enc_pwd,
        "environment": "demo",  # Always "demo" per captured request
        "appId": DEFAULT_APP_ID,
        "appVersion": DEFAULT_APP_VERSION,
        "cid": DEFAULT_CID,
        "sec": DEFAULT_SEC,
        "deviceId": device_id,
        "chl": chl,
        "enc": True,
    }

    headers = {
        "Content-Type": "application/json",
        "Origin": "https://trader.tradovate.com",
        "Referer": "https://trader.tradovate.com/",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }

    auth_url = ENDPOINTS[environment]["auth"]

    try:
        response = requests.post(auth_url, json=payload, headers=headers)

        if response.status_code != 200:
            return False, f"Auth failed: HTTP {response.status_code} - {response.text}", None

        data = response.json()

        if "accessToken" not in data:
            # Check for error message
            if "errorText" in data:
                return False, f"Auth failed: {data['errorText']}", None
            return False, f"Auth failed: No access token in response: {data}", None

        return True, "Success", data

    except requests.RequestException as e:
        return False, f"Request failed: {e}", None
    except Exception as e:
        return False, f"Error: {e}", None


def decode_jwt(token: str) -> Optional[dict]:
    """Decode JWT payload without verification."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload = parts[1]
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding
        decoded = base64.urlsafe_b64decode(payload)
        return json.loads(decoded)
    except Exception:
        return None


def check_token(token: str) -> Tuple[bool, Optional[datetime], str]:
    """
    Check token validity and expiration.
    Returns: (is_valid, expiration_datetime, status_message)
    """
    payload = decode_jwt(token)
    if not payload:
        return False, None, "Could not decode token"

    exp_timestamp = payload.get("exp")
    if not exp_timestamp:
        return False, None, "Token has no expiration field"

    exp_dt = datetime.fromtimestamp(exp_timestamp)
    now = datetime.now()

    if now >= exp_dt:
        return False, exp_dt, f"EXPIRED at {exp_dt.strftime('%Y-%m-%d %H:%M:%S')}"

    time_remaining = exp_dt - now
    hours = int(time_remaining.total_seconds() / 3600)
    minutes = int((time_remaining.total_seconds() % 3600) / 60)

    if time_remaining < timedelta(hours=1):
        return True, exp_dt, f"EXPIRING SOON ({minutes} minutes remaining)"

    return True, exp_dt, f"Valid ({hours}h {minutes}m remaining, expires {exp_dt.strftime('%Y-%m-%d %H:%M:%S')})"


def load_token() -> Optional[str]:
    """Load token from file."""
    if TOKEN_FILE.exists():
        return TOKEN_FILE.read_text().strip()
    return None


def save_token(token: str):
    """Save token to file."""
    TOKEN_FILE.write_text(token)
    TOKEN_FILE.chmod(0o600)
    print(f"Token saved to {TOKEN_FILE}")


def refresh_token(
    username: str,
    password: str,
    environment: str = "demo",
    device_id: Optional[str] = None,
    password_encoded: Optional[str] = None,
    challenge: Optional[str] = None,
) -> bool:
    """Refresh token and save to file. Returns success status."""
    print(f"Authenticating as {username} ({environment})...")

    success, message, data = get_access_token(
        username, password, environment, device_id, password_encoded, challenge
    )

    if not success:
        print(f"ERROR: {message}")
        return False

    token = data["accessToken"]
    save_token(token)

    # Show token status
    is_valid, exp_dt, status = check_token(token)
    print(f"Token status: {status}")

    # Show additional info if available
    if "userId" in data:
        print(f"User ID: {data['userId']}")
    if "name" in data:
        print(f"Name: {data['name']}")

    return True


def main():
    parser = argparse.ArgumentParser(description="Tradovate token management")
    parser.add_argument("--check", action="store_true", help="Check current token status only")
    parser.add_argument("--env", choices=["demo", "live"], default="demo", help="Environment (default: demo)")
    parser.add_argument("--username", help="Tradovate username (default: from .tradovate_credentials)")
    parser.add_argument("--password", help="Tradovate password (default: from .tradovate_credentials)")
    parser.add_argument("--save-creds", action="store_true", help="Save credentials to file")
    parser.add_argument("--device-id", help="Device ID (default: from credentials or auto-generate)")

    args = parser.parse_args()

    # Check-only mode
    if args.check:
        token = load_token()
        if not token:
            print(f"No token found at {TOKEN_FILE}")
            return 1

        is_valid, exp_dt, status = check_token(token)
        print(f"Token status: {status}")
        return 0 if is_valid else 1

    # Load or prompt for credentials
    creds = load_credentials() or {}

    username = args.username or creds.get("username")
    password = args.password or creds.get("password")
    device_id = args.device_id or creds.get("device_id")
    password_encoded = creds.get("password_encoded")
    challenge = creds.get("challenge")

    if not username or not password:
        print("Credentials required. Provide --username and --password")
        print(f"Or save credentials to {CREDENTIALS_FILE}")
        print("\nExample credentials file:")
        print(json.dumps({
            "username": "YOUR_USERNAME",
            "password": "YOUR_PASSWORD",
            "password_encoded": "BASE64_ENCODED_PASSWORD (from browser capture)",
            "challenge": "CHALLENGE_VALUE (from browser capture)",
            "device_id": "optional-device-id"
        }, indent=2))
        return 1

    # Save credentials if requested
    if args.save_creds:
        save_credentials({
            "username": username,
            "password": password,
            "password_encoded": password_encoded,
            "challenge": challenge,
            "device_id": device_id or creds.get("device_id"),
        })

    # Refresh token
    success = refresh_token(username, password, args.env, device_id, password_encoded, challenge)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
