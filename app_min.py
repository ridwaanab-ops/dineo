# app_min.py — Dineo WA bot (schema-aware DB logging, JHB time, status logs, sentiment,
#                             account_inquiry with personal code + WA fallback)

import asyncio, os, re, json, time, logging, random, tempfile, threading, secrets, io, csv, mimetypes, hashlib, math
from typing import Any, Dict, Optional, Tuple, List
from pathlib import Path
from datetime import datetime, timedelta, date
from urllib.parse import urlencode

import requests
import bcrypt
from fastapi import FastAPI, Request, HTTPException, Form, UploadFile, File
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

import io
from fpdf import FPDF

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("dineo")

# -----------------------------------------------------------------------------
# Config / tokens
# -----------------------------------------------------------------------------
WABA_TOKEN    = os.environ.get("WABA_TOKEN", "")
WABA_PHONE_ID = os.environ.get("WABA_PHONE_ID", "")
VERIFY_TOKEN  = os.environ.get("VERIFY_TOKEN", "my_secret_verify_token")
GRAPH_URL     = f"https://graph.facebook.com/v19.0/{WABA_PHONE_ID}/messages"

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "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")
OPENAI_MODEL   = os.environ.get("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_TRANSCRIBE_MODEL = os.environ.get("OPENAI_TRANSCRIBE_MODEL", "gpt-4o-mini-transcribe")

LOG_DB_INSERTS = os.getenv("LOG_DB_INSERTS", "0") == "1"  # also enables a few debug traces
# Approved WhatsApp templates (expand as needed)
APPROVED_WA_TEMPLATES = [
    {
        "id": "driver_update",
        "name": "Driver Update",
        "language": "en",
        "description": "Quick update with outstanding balance",
        "parameter_format": "NAMED",
        "variables": ["name", "amount"],
    },
    {
        "id": "payment_reminder",
        "name": "Payment Reminder",
        "language": "en",
        "description": "Reminder to settle outstanding",
        "parameter_format": "NAMED",
        "variables": ["name", "amount", "reference"],
        "variants": ["payment_reminder", "payment_reminder_v2"],
    },
    {
        "id": "performance_needs_attention",
        "name": "Performance Needs Attention",
        "language": "en",
        "description": "Encourage drivers who need attention to improve key KPIs",
        "parameter_format": "NAMED",
        "variables": ["name", "online_hours", "acceptance_rate"],
        "variants": ["performance_needs_attention", "performance_needs_attention_v2"],
    },
    {
        "id": "performance_no_trips_yet",
        "name": "Performance No Trips Yet",
        "language": "en",
        "description": "Nudge drivers with no trips logged to start the day",
        "parameter_format": "NAMED",
        "variables": ["name", "online_hours", "trip_count"],
        "variants": ["performance_no_trips_yet", "performance_no_trips_yet_v2"],
    },
    {
        "id": "performance_engagement_boost",
        "name": "Performance Engagement Boost",
        "language": "en",
        "description": "Motivational check-in to increase engagement",
        "parameter_format": "NAMED",
        "variables": ["name"],
        "variants": ["performance_engagement_boost", "performance_engagement_boost_v2"],
    },
    {
        "id": "performance_payment_support",
        "name": "Performance + Payment Support",
        "language": "en",
        "description": "Link performance to payments and ask for a realistic plan",
        "parameter_format": "NAMED",
        "variables": ["name", "xero_balance"],
        "variants": ["performance_payment_support", "performance_payment_support_v2"],
    },
    {
        "id": "performance_followup_24h",
        "name": "Performance Follow-up 24h",
        "language": "en",
        "description": "Gentle follow-up for no response after 24 hours",
        "parameter_format": "NAMED",
        "variables": ["name"],
        "variants": ["performance_followup_24h", "performance_followup_24h_v2"],
    },
    {
        "id": "collections_followup_24h",
        "name": "Collections Follow-up 24h",
        "language": "en",
        "description": "Gentle collections follow-up for no response after 24 hours",
        "parameter_format": "NAMED",
        "variables": ["name"],
        "variants": ["collections_followup_24h", "collections_followup_24h_v2"],
    },
]
ISSABEL_CLICK2CALL_URL = os.getenv("ISSABEL_CLICK2CALL_URL")  # e.g. https://pbx.example.com/click2call
ISSABEL_USERNAME = os.getenv("ISSABEL_USERNAME")
ISSABEL_PASSWORD = os.getenv("ISSABEL_PASSWORD")
ISSABEL_EXTENSION = os.getenv("ISSABEL_EXTENSION")
ISSABEL_VERIFY_TLS = os.getenv("ISSABEL_VERIFY_TLS", "1") != "0"
SAFE_LOG_COLUMNS = {
    "message_id", "wa_message_id", "wa_id", "phone",
    "message_direction", "status", "intent", "timestamp", "created_at"
}

ZERO_TRIP_NUDGES_ENABLED = os.getenv("ZERO_TRIP_NUDGES_ENABLED", "1") == "1"
ZERO_TRIP_NUDGE_INTERVAL_SECONDS = int(os.getenv("ZERO_TRIP_NUDGE_INTERVAL_SECONDS", str(3 * 60 * 60)))
ZERO_TRIP_MAX_NUDGES_PER_DAY = int(os.getenv("ZERO_TRIP_MAX_NUDGES_PER_DAY", "3"))
ZERO_TRIP_NUDGE_INTENT = "zero_trip_nudge"
ENGAGEMENT_FOLLOWUP_ENABLED = os.getenv("ENGAGEMENT_FOLLOWUP_ENABLED", "1") == "1"
ENGAGEMENT_FOLLOWUP_DELAY_HOURS = float(os.getenv("ENGAGEMENT_FOLLOWUP_DELAY_HOURS", "24"))
ENGAGEMENT_FOLLOWUP_INTERVAL_SECONDS = int(os.getenv("ENGAGEMENT_FOLLOWUP_INTERVAL_SECONDS", str(60 * 60)))
ENGAGEMENT_FOLLOWUP_MAX_BATCH = int(os.getenv("ENGAGEMENT_FOLLOWUP_MAX_BATCH", "200"))
NO_VEHICLE_CHECKIN_ENABLED = os.getenv("NO_VEHICLE_CHECKIN_ENABLED", "1") == "1"
NO_VEHICLE_CHECKIN_DELAY_HOURS = float(os.getenv("NO_VEHICLE_CHECKIN_DELAY_HOURS", "24"))
ZERO_TRIP_NUDGE_MESSAGES = [
    "Hey there! I haven’t seen any trips logged yet today. If you’re ready, hop online so we can start tracking your earnings.",
    "Quick check-in: no trips recorded so far. When you’re good to go, head online and I’ll keep cheering you on!",
    "Still seeing zero trips on the dashboard. Let me know once you’re rolling so we can chase those targets together.",
]
ZERO_TRIP_SUPPORT_HINT = "Need a hand with the app or route planning? Just say the word—I'm here for you."
ZERO_TRIP_NUDGE_START_HOUR = int(os.getenv("ZERO_TRIP_NUDGE_START_HOUR", "6"))
ZERO_TRIP_NUDGE_START_MINUTE = int(os.getenv("ZERO_TRIP_NUDGE_START_MINUTE", "30"))
ZERO_TRIP_NUDGE_SKIP_SUNDAYS = os.getenv("ZERO_TRIP_NUDGE_SKIP_SUNDAYS", "1") == "1"
INTRADAY_UPDATES_ENABLED = os.getenv("INTRADAY_UPDATES_ENABLED", "1") == "1"
INTRADAY_UPDATE_INTERVAL_SECONDS = int(os.getenv("INTRADAY_UPDATE_INTERVAL_SECONDS", str(10 * 60)))
INTRADAY_UPDATE_GRACE_MINUTES = int(os.getenv("INTRADAY_UPDATE_GRACE_MINUTES", "50"))
INTRADAY_DAILY_MIN_FINISHED_ORDERS = int(os.getenv("INTRADAY_DAILY_MIN_FINISHED_ORDERS", "22"))
INTRADAY_UPDATE_INTENT = "intraday_update"
INTRADAY_AUTO_ON_RESPONDED = os.getenv("INTRADAY_AUTO_ON_RESPONDED", "1") == "1"
INTRADAY_CHECKPOINT_RATIOS = [
    (10, 8 / 22),
    (12, 12 / 22),
    (14, 16 / 22),
    (16, 20 / 22),
    (18, 1.0),
]

BASE_DIR = Path(__file__).resolve().parent
_templates_override = os.getenv("TEMPLATES_DIR")
_template_dirs: list[Path] = []
if _templates_override:
    override_path = Path(_templates_override).expanduser()
    if override_path.exists():
        _template_dirs.append(override_path)
    else:
        log.warning("TEMPLATES_DIR=%s not found, skipping override.", override_path)
default_templates = BASE_DIR / "templates"
if not default_templates.exists():
    try:
        default_templates.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        log.warning("Failed to create templates dir %s: %s", default_templates, exc)
if default_templates.exists():
    _template_dirs.append(default_templates)
if BASE_DIR not in _template_dirs:
    _template_dirs.append(BASE_DIR)
if not _template_dirs:
    _template_dirs.append(BASE_DIR)
TEMPLATES_DIR = _template_dirs[0]
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
try:
    from jinja2 import FileSystemLoader

    loader_paths = [str(p) for p in _template_dirs if p.exists()]
    if loader_paths:
        templates.env.loader = FileSystemLoader(loader_paths)
except Exception as exc:
    log.warning("Falling back to single template directory due to loader error: %s", exc)


def _jinja_meta_value(value, key=""):
    return _format_metadata_value(key or "", value)


templates.env.filters["meta_value"] = _jinja_meta_value

# -----------------------------------------------------------------------------
# Timezone helpers (Africa/Johannesburg)
# -----------------------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def jhb_now() -> datetime:
    try:
        return datetime.now(ZoneInfo("Africa/Johannesburg")) if ZoneInfo else datetime.utcnow() + timedelta(hours=2)
    except Exception:
        return datetime.utcnow() + timedelta(hours=2)

def unix_to_jhb_str(ts_unix: Optional[str|int]) -> Optional[str]:
    if not ts_unix:
        return None
    try:
        ts = int(ts_unix)
        if ZoneInfo:
            return datetime.fromtimestamp(ts, ZoneInfo("Africa/Johannesburg")).strftime("%Y-%m-%d %H:%M:%S")
        return (datetime.utcfromtimestamp(ts) + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def fmt_jhb_date() -> str:
    d = jhb_now()
    return d.strftime("%A, %d %B %Y").replace(" 0", " ")

def fmt_jhb_time() -> str:
    return jhb_now().strftime("%H:%M")


def _format_timestamp_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        ts = float(value)
    except Exception:
        return None
    if ts > 9999999999:  # likely milliseconds
        ts = ts / 1000.0
    try:
        dt = datetime.fromtimestamp(ts, ZoneInfo("Africa/Johannesburg")) if ZoneInfo else datetime.utcfromtimestamp(ts) + timedelta(hours=2)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _format_metadata_value(key: str, value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    if isinstance(value, bool):
        return "True" if value else "False"
    key_lower = (key or "").lower()
    ts_keys = ("_ts", "_timestamp", "_time", "_at")
    if key_lower.endswith(ts_keys):
        formatted = _format_timestamp_value(value)
        if formatted:
            return formatted
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)

# -----------------------------------------------------------------------------
# MySQL config & utils
# -----------------------------------------------------------------------------
def _env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v.strip() if isinstance(v, str) else v

_bcrypt_truncation_warned = False
BCRYPT_ROUNDS = int(os.getenv("ADMIN_BCRYPT_ROUNDS", "12"))
BCRYPT_MAX_PASSWORD_BYTES = 72

ADMIN_SESSION_SECRET = os.getenv("ADMIN_SESSION_SECRET")
if not ADMIN_SESSION_SECRET:
    fallback_secret = os.getenv("VERIFY_TOKEN") or os.getenv("WABA_TOKEN")
    if fallback_secret:
        ADMIN_SESSION_SECRET = f"{fallback_secret}-admin"
    else:
        ADMIN_SESSION_SECRET = secrets.token_hex(32)

ADMIN_COOKIE_NAME = os.getenv("ADMIN_COOKIE_NAME", "dineo_admin")
ADMIN_COOKIE_SECURE = os.getenv("ADMIN_COOKIE_SECURE", "1") == "1"
ADMIN_COOKIE_SAMESITE = os.getenv("ADMIN_COOKIE_SAMESITE", "lax").lower()
ADMIN_SESSION_MAX_AGE = int(os.getenv("ADMIN_SESSION_MAX_AGE", str(7 * 24 * 60 * 60)))
ADMIN_BOOTSTRAP_EMAIL = _env("ADMIN_BOOTSTRAP_EMAIL")
ADMIN_BOOTSTRAP_PASSWORD = _env("ADMIN_BOOTSTRAP_PASSWORD")

SMTP_HOST = _env("SMTP_HOST")
SMTP_PORT = int(_env("SMTP_PORT", "587"))
SMTP_USERNAME = _env("SMTP_USERNAME") or _env("SMTP_USER")
SMTP_PASSWORD = _env("SMTP_PASSWORD")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "1") == "1"
RESET_TOKEN_MAX_AGE = int(os.getenv("ADMIN_RESET_TOKEN_MAX_AGE", str(60 * 60)))

_admin_serializer = URLSafeTimedSerializer(ADMIN_SESSION_SECRET, salt="dineo-admin")
_reset_serializer = URLSafeTimedSerializer(ADMIN_SESSION_SECRET, salt="dineo-admin-reset")

# Driver portal session config
DRIVER_SESSION_SECRET = os.getenv("DRIVER_SESSION_SECRET") or ADMIN_SESSION_SECRET
DRIVER_COOKIE_NAME = os.getenv("DRIVER_COOKIE_NAME", "driver_portal")
DRIVER_COOKIE_SECURE = os.getenv("DRIVER_COOKIE_SECURE", "1") == "1"
DRIVER_COOKIE_SAMESITE = os.getenv("DRIVER_COOKIE_SAMESITE", "lax").lower()
DRIVER_SESSION_MAX_AGE = int(os.getenv("DRIVER_SESSION_MAX_AGE", str(2 * 24 * 60 * 60)))
_driver_serializer = URLSafeTimedSerializer(DRIVER_SESSION_SECRET, salt="driver-portal")
MNC_LOGO_PATH = os.getenv("MNC_LOGO_PATH", "./mnc_logo.png")
# Optional: absolute/served URL. If not provided, we'll embed a data URL from MNC_LOGO_PATH if available.
MNC_LOGO_URL = os.getenv("MNC_LOGO_URL", "").strip()
# Default inline SVG (matches MNC lockup)
DEFAULT_LOGO_DATA_URL = (
    "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='512' height='164' viewBox='0 0 512 164'%3E"
    "%3Cpath fill='%23282828' d='M120 128c-22 0-39.3-8-52.3-24.6V128H0V32h43.7v44.9C43.7 100.7 52.7 109 69 109c16 0 25-8.3 25-32.1V32H137v64c0 30-23.5 32-44.3 32Z'/%3E"
    "%3Ccircle cx='263' cy='28' r='14' fill='%2352c41a'/%3E"
    "%3Cpath fill='%23282828' d='M214.4 128c-22 0-39.3-8-52.3-24.6V128H94.4V32h43.7v44.9C138 100.7 147 109 163.3 109c16 0 25-8.3 25-32.1V32h43.7v64c0 30-23.5 32-44.3 32Z'/%3E"
    "%3Cpath fill='%23282828' d='M412.5 128c-35.2 0-62.3-26.4-62.3-56 0-29.8 27-56.2 62.3-56.2 35 0 62.1 26.9 62.1 56.6 0 4-0.5 7.8-1.2 11.2h-82.4c4.2 8.9 13.7 15.5 26.8 15.5 9.3 0 19-3.6 24-9.8l28.3 17c-12 13-31 21.7-51.9 21.7Zm-24.2-66.4h47.6c-3.2-10-14-17.2-24-17.2-10.9 0-20.4 6.8-23.6 17.2Z'/%3E"
    "%3Crect x='242' y='134' width='88' height='18' fill='%2352c41a' rx='4'/%3E%3C/svg%3E"
)
MYSQL_HOST = _env("MYSQL_HOST")
MYSQL_PORT = int(_env("MYSQL_PORT", "3306"))
MYSQL_DB   = _env("MYSQL_DB") or _env("MYSQL_SCHEMA") or "mnc_report"
MYSQL_USER = _env("MYSQL_USER")
MYSQL_PASS = _env("MYSQL_PASSWORD") or _env("MYSQL_PASS") or _env("MYSQL_PWD")

try:
    import pymysql
except Exception:
    pymysql = None

def _missing_db_envs() -> List[str]:
    missing = []
    if not MYSQL_HOST: missing.append("MYSQL_HOST")
    if not MYSQL_DB:   missing.append("MYSQL_DB")
    if not MYSQL_USER: missing.append("MYSQL_USER")
    if not MYSQL_PASS: missing.append("MYSQL_PASSWORD (or MYSQL_PASS/MYSQL_PWD)")
    return missing

def mysql_available() -> bool:
    return bool(pymysql and MYSQL_HOST and MYSQL_DB and MYSQL_USER and MYSQL_PASS)


def require_mysql() -> None:
    if not mysql_available():
        raise RuntimeError("MySQL not configured. Missing: " + ", ".join(_missing_db_envs()))


_mysql_thread_local = threading.local()


def _create_mysql_connection():
    conn = pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS,
        db=MYSQL_DB, charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor, autocommit=True
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SET time_zone = '+02:00'")
    except Exception:
        pass
    return conn


def get_mysql():
    require_mysql()
    conn = getattr(_mysql_thread_local, "connection", None)
    if conn:
        try:
            conn.ping(reconnect=True)
            return conn
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
    conn = _create_mysql_connection()
    _mysql_thread_local.connection = conn
    return conn

def _split_schema_table(fqtn: str):
    if "." in fqtn:
        sch, tbl = fqtn.split(".", 1)
    else:
        sch, tbl = MYSQL_DB, fqtn
    return sch, tbl

def _table_exists(conn, fqtn: str) -> bool:
    sch, tbl = _split_schema_table(fqtn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (sch, tbl)
        )
        return cur.fetchone() is not None

def _get_table_columns(conn, fqtn: str) -> set[str]:
    if not hasattr(_get_table_columns, "_cache"):
        _get_table_columns._cache = {}
    cache = _get_table_columns._cache
    if fqtn in cache:
        return cache[fqtn]
    sch, tbl = _split_schema_table(fqtn)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (sch, tbl)
        )
        cols = {row["COLUMN_NAME"] for row in (cur.fetchall() or [])}
    cache[fqtn] = cols
    return cols

def _pick_col_exists(conn, table_fq: str, candidates: List[str]) -> Optional[str]:
    sch, tbl = _split_schema_table(table_fq)
    with conn.cursor() as cur:
        for c in candidates:
            cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s",
                (sch, tbl, c)
            )
            if cur.fetchone():
                return c
    return None


class PDFStatement(FPDF):
    def __init__(self, wa_id, display_name, *args, model=None, vehicle=None, bank=None, reference=None, **kwargs):
        super().__init__(*args, **kwargs)
        # Store data as instance attributes
        self.doc_title = display_name
        self.doc_wa_id = wa_id
        self.doc_model = model
        self.doc_vehicle = vehicle
        self.doc_bank = bank or {}
        self.doc_reference = reference

    def header(self):
        # Access stored data from 'self'
        self.set_font('Arial', 'B', 15)
        self.cell(0, 10, 'My Next Car Account Statement', 0, 1, 'C')
        self.set_font('Arial', '', 10)
        self.cell(0, 5, f'Driver: {self.doc_title} (WA: {self.doc_wa_id})', 0, 1, 'C')
        self.cell(0, 5, f'Report Date: {fmt_jhb_date()}', 0, 1, 'C')
        # Extra metadata
        meta_lines = []
        if self.doc_model or self.doc_vehicle:
            parts = []
            if self.doc_model:
                parts.append(f"Model: {self.doc_model}")
            if self.doc_vehicle:
                parts.append(f"Vehicle: {self.doc_vehicle}")
            meta_lines.append(" · ".join(parts))
        if self.doc_bank:
            bank_parts = []
            if self.doc_bank.get("bank_name"):
                bank_parts.append(f"Bank: {self.doc_bank.get('bank_name')}")
            if self.doc_bank.get("account_number"):
                bank_parts.append(f"Account: {self.doc_bank.get('account_number')}")
            if self.doc_bank.get("branch_code"):
                bank_parts.append(f"Branch: {self.doc_bank.get('branch_code')}")
            if bank_parts:
                meta_lines.append(" · ".join(bank_parts))
        if self.doc_reference:
            meta_lines.append(f"Reference: {self.doc_reference}")
        if meta_lines:
            self.set_font('Arial', '', 9)
            for line in meta_lines:
                self.cell(0, 5, line, 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('Arial', 'I', 8)
        self.cell(0, 10, 'Page %s' % self.page_no(), 0, 0, 'C')

    def statement_table(self, data):
        self.set_fill_color(220, 220, 220)
        self.set_draw_color(180, 180, 180)
        self.set_line_width(0.3)
        self.set_font('Arial', 'B', 9)
        
        # Column widths (in mm)
        w = [18, 55, 30, 25, 25, 35] 
        
        headers = ['Date', 'Reference', 'Type', 'Debit', 'Credit', 'Outstanding']
        
        # Print header
        for i in range(len(headers)):
            align = 'C' if i < 3 else 'R'
            self.cell(w[i], 7, headers[i], 1, 0, align, 1)
        self.ln()

        self.set_font('Arial', '', 8)
        
        for row in data:
            if not isinstance(row, dict): continue # Safety check
            
            # Date
            date_str = (str(row.get("date") or "")).replace(" 00:00:00", "")
            self.cell(w[0], 6, date_str, 'LR')

            # Reference (Ensure it's a string)
            reference = str(row.get("reference") or "")
            self.cell(w[1], 6, reference, 'LR')

            # Type (Ensure it's a string)
            source = str(row.get("source") or "")
            self.cell(w[2], 6, source, 'LR')

            # Debit
            debit_val = fmt_rands(row.get("debit")) if row.get("debit") else '-'
            self.cell(w[3], 6, debit_val, 'LR', 0, 'R')

            # Credit
            credit_val = fmt_rands(row.get("credit")) if row.get("credit") else '-'
            self.cell(w[4], 6, credit_val, 'LR', 0, 'R')

            # Outstanding
            outstanding_val = fmt_rands(row.get("outstanding"))
            self.cell(w[5], 6, outstanding_val, 'LR', 0, 'R')

            self.ln()

        # Closing line
        self.cell(sum(w), 0, '', 'T', 1)

def generate_statement_pdf(wa_id: str, display_name: str, statement_data: List[Dict[str, Any]], *, meta: Optional[Dict[str, Any]] = None) -> bytes:
    meta = meta or {}
    # Pass wa_id and display_name to the PDFStatement constructor
    pdf = PDFStatement(
        wa_id=wa_id,
        display_name=display_name,
        model=meta.get("model"),
        vehicle=meta.get("vehicle"),
        bank=meta.get("bank"),
        reference=meta.get("reference"),
        orientation='P', 
        unit='mm', 
        format='A4'
    )
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page() # This is where PDF generation starts
    pdf.statement_table(statement_data)
    
    # --- FIX: Convert the bytearray output (dest='S') to an immutable bytes object ---
    pdf_output_bytearray = pdf.output(dest='S')

    # Convert to bytes (PyFPDF may return str in some environments)
    if isinstance(pdf_output_bytearray, str):
        pdf_output = pdf_output_bytearray.encode("latin-1")
    else:
        pdf_output = bytes(pdf_output_bytearray)
    return pdf_output

# -----------------------------------------------------------------------------
# Ensure context table
# -----------------------------------------------------------------------------
def ensure_schema():
    if not mysql_available():
        log.error("MySQL not configured at startup. Missing: %s", ", ".join(_missing_db_envs()))
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_DB}.whatsapp_context_memory (
              wa_id VARCHAR(32) PRIMARY KEY,
              last_intent VARCHAR(64) NULL,
              last_reply LONGTEXT NULL,
              prefs_json JSON NULL,
              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_DB}.driver_zero_trip_nudges (
              wa_id VARCHAR(32) NOT NULL,
              nudge_date DATE NOT NULL,
              nudge_count INT NOT NULL DEFAULT 0,
              last_nudge_at TIMESTAMP NULL DEFAULT NULL,
              PRIMARY KEY (wa_id, nudge_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_DB}.driver_nudge_events (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              wa_id VARCHAR(32) NOT NULL,
              nudge_date DATE NOT NULL,
              nudge_number INT NOT NULL,
              template_index INT NOT NULL,
              template_message TEXT NULL,
              send_status VARCHAR(32) NOT NULL,
              whatsapp_message_id VARCHAR(128) NULL,
              send_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              status_code VARCHAR(64) NULL,
              status_detail TEXT NULL,
              response_message_id VARCHAR(128) NULL,
              response_ts TIMESTAMP NULL,
              response_latency_sec DECIMAL(10,2) NULL,
              response_intent VARCHAR(64) NULL,
              metadata JSON NULL,
              PRIMARY KEY (id),
              KEY idx_driver_nudge (wa_id, nudge_date),
              KEY idx_driver_nudge_msg (whatsapp_message_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            try:
                cols = _get_table_columns(conn, f"{MYSQL_DB}.driver_nudge_events")
            except Exception:
                cols = set()
            if "last_update_at" not in cols:
                try:
                    cur.execute(
                        f"""
                        ALTER TABLE {MYSQL_DB}.driver_nudge_events
                        ADD COLUMN last_update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                          ON UPDATE CURRENT_TIMESTAMP
                          AFTER send_ts
                        """
                    )
                except Exception as alter_exc:
                    log.warning("driver_nudge_events alter failed: %s", alter_exc)
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_DB}.driver_intraday_updates (
              wa_id VARCHAR(32) NOT NULL,
              update_date DATE NOT NULL,
              slot_hour TINYINT NOT NULL,
              target_trips INT NULL,
              finished_trips INT NULL,
              acceptance_rate DECIMAL(6,2) NULL,
              send_status VARCHAR(32) NOT NULL,
              whatsapp_message_id VARCHAR(128) NULL,
              sent_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (wa_id, update_date, slot_hour),
              KEY idx_intraday_status (send_status),
              KEY idx_intraday_msg (whatsapp_message_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_DB}.driver_issue_tickets (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              wa_id VARCHAR(32) NOT NULL,
              issue_type VARCHAR(32) NOT NULL,
              status VARCHAR(32) NOT NULL DEFAULT 'open',
              initial_message LONGTEXT NULL,
              media_urls JSON NULL,
              location_lat DECIMAL(10,7) NULL,
              location_lng DECIMAL(10,7) NULL,
              location_desc VARCHAR(255) NULL,
              metadata JSON NULL,
              last_update_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              KEY idx_issue_status (status),
              KEY idx_issue_wa (wa_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_DB}.driver_issue_admins (
              id INT UNSIGNED NOT NULL AUTO_INCREMENT,
              email VARCHAR(255) NOT NULL,
              password_hash VARCHAR(255) NOT NULL,
              is_active TINYINT(1) NOT NULL DEFAULT 1,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              UNIQUE KEY uniq_driver_issue_admin_email (email)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
            cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {MYSQL_DB}.driver_issue_ticket_logs (
              id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
              ticket_id BIGINT UNSIGNED NOT NULL,
              admin_email VARCHAR(255) NULL,
              action_type VARCHAR(32) NOT NULL,
              from_status VARCHAR(32) NULL,
              to_status VARCHAR(32) NULL,
              note TEXT NULL,
              created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (id),
              KEY idx_ticket_logs_ticket (ticket_id),
              KEY idx_ticket_logs_created (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""")
    except Exception as e:
        log.warning("ensure_schema skipped: %s", e)

# -----------------------------------------------------------------------------
# OpenAI helpers (optional)
# -----------------------------------------------------------------------------
def nlg_with_openai(system: str, prompt: str) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    try:
        from openai import OpenAI
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role":"system","content":system},{"role":"user","content":prompt}],
            temperature=0.5, max_tokens=300,
        )
        return (resp.choices[0].message.content or "").strip()
    except Exception as e:
        log.warning("OpenAI NLG failed: %s", e)
        return None

def analyze_sentiment(text: str) -> Tuple[str, float, Dict[str, Any]]:
    toks = re.findall(r"[a-z]+", (text or "").lower())
    pos = {"great","good","awesome","love","excellent","amazing","nice","thanks","thank","happy","well","cool"}
    neg = {"bad","terrible","hate","awful","angry","upset","sad","poor","problem","issue","slow"}
    pc = sum(t in pos for t in toks); nc = sum(t in neg for t in toks)
    score = (pc - nc) / max(1, len(toks))
    label = "positive" if score > 0.15 else ("negative" if score < -0.15 else "neutral")
    raw = {"method":"rule","pos":pc,"neg":nc,"len":len(toks)}
    return label, float(round(score, 3)), raw

# -----------------------------------------------------------------------------
# Context persistence (file + DB)
# -----------------------------------------------------------------------------
CTX_DIR = Path("./context"); CTX_DIR.mkdir(exist_ok=True)
def _ctx_path(wa_id: str) -> Path: return CTX_DIR / f"{wa_id}.json"
def load_context_file(wa_id: str) -> Dict[str, Any]:
    p = _ctx_path(wa_id)
    if p.exists():
        try: return json.loads(p.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}
def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)
def _json_dumps(value: Any, *, pretty: bool = False) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2 if pretty else None, default=_json_default)
def save_context_file(wa_id: str, ctx: Dict[str, Any]) -> None:
    _ctx_path(wa_id).write_text(_json_dumps(ctx, pretty=True), encoding="utf-8")

def _record_outbound_template_context(
    wa_id: str,
    template_id: str,
    params: Any,
    *,
    param_names: Optional[List[str]] = None,
    parameter_format: Optional[str] = None,
) -> None:
    ctx = load_context_file(wa_id)
    named: Dict[str, Any] = {}
    if isinstance(params, dict):
        named = {str(k): v for k, v in params.items()}
    elif isinstance(params, (list, tuple)) and param_names:
        for idx, name in enumerate(param_names):
            if idx >= len(params):
                break
            key = str(name or "").strip()
            if key:
                named[key] = params[idx]
    ctx["_last_outbound_template"] = {
        "id": template_id,
        "sent_at": time.time(),
        "params": params,
        "params_named": named,
        "parameter_format": parameter_format,
    }
    if str(template_id or "").startswith("performance_no_trips_yet"):
        ctx["_awaiting_goal_confirm"] = True
        ctx["_awaiting_goal_set_at"] = time.time()
    if str(template_id or "").startswith("performance_needs_attention"):
        ctx["_awaiting_performance_tips"] = True
        ctx["_performance_tips_set_at"] = time.time()
    save_context_file(wa_id, ctx)

def _recent_outbound_template(ctx: Dict[str, Any], template_id: str, *, max_age_hours: int = 72) -> Optional[Dict[str, Any]]:
    payload = ctx.get("_last_outbound_template")
    if not isinstance(payload, dict):
        return None
    if payload.get("id") != template_id:
        return None
    sent_at = payload.get("sent_at")
    try:
        if sent_at and (time.time() - float(sent_at)) > max_age_hours * 3600:
            return None
    except Exception:
        return None
    return payload

def _recent_outbound_template_for_group(
    ctx: Dict[str, Any],
    template_group: str,
    *,
    max_age_hours: int = 72,
) -> Optional[Dict[str, Any]]:
    payload = ctx.get("_last_outbound_template")
    if not isinstance(payload, dict):
        return None
    template_id = payload.get("id")
    if not template_id:
        return None
    if template_id != template_group:
        variants: List[str] = []
        for tmpl in APPROVED_WA_TEMPLATES:
            if tmpl.get("id") == template_group:
                variants = tmpl.get("variants") or []
                break
        if template_id not in variants:
            return None
    sent_at = payload.get("sent_at")
    try:
        if sent_at and (time.time() - float(sent_at)) > max_age_hours * 3600:
            return None
    except Exception:
        return None
    return payload

def _coerce_template_amount(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return fmt_rands(float(value))
    text = str(value).strip()
    if not text:
        return None
    if re.search(r"[Rr]\s*\d", text) or "," in text:
        return text
    try:
        numeric = float(re.sub(r"[^\d.\-]", "", text))
    except Exception:
        return text
    return fmt_rands(numeric)

def _extract_driver_update_amount(ctx: Dict[str, Any]) -> Optional[str]:
    tmpl_ctx = _recent_outbound_template(ctx, "driver_update")
    if not tmpl_ctx:
        return None
    params_named = tmpl_ctx.get("params_named") or {}
    amount = params_named.get("amount") or params_named.get("outstanding") or params_named.get("balance")
    if amount is None:
        params = tmpl_ctx.get("params")
        if isinstance(params, (list, tuple)) and len(params) >= 2:
            amount = params[1]
    return _coerce_template_amount(amount)

def _coerce_goal_value(value: Any, *, allow_zero: bool = False) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        num = float(value)
        if not allow_zero and num <= 0:
            return None
        if num.is_integer():
            return str(int(num))
        return f"{num:.1f}".rstrip("0").rstrip(".")
    text = str(value).strip()
    if not text:
        return None
    try:
        numeric = float(re.sub(r"[^\d.\-]", "", text))
    except Exception:
        return text
    if not allow_zero and numeric <= 0:
        return None
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:.1f}".rstrip("0").rstrip(".")

def _extract_no_trips_goals(ctx: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    tmpl_ctx = _recent_outbound_template_for_group(ctx, "performance_no_trips_yet")
    if not tmpl_ctx:
        return None, None
    params_named = tmpl_ctx.get("params_named") or {}
    hours = params_named.get("online_hours")
    trips = params_named.get("trip_count")
    if hours is None or trips is None:
        params = tmpl_ctx.get("params")
        if isinstance(params, (list, tuple)):
            if hours is None and len(params) >= 2:
                hours = params[1]
            if trips is None and len(params) >= 3:
                trips = params[2]
    return _coerce_goal_value(hours), _coerce_goal_value(trips)

def _recent_goal_targets(ctx: Dict[str, Any], *, max_age_days: Optional[int] = None) -> Dict[str, float]:
    if not ctx:
        return {}
    if max_age_days is None:
        max_age_days = GOAL_TARGET_TTL_DAYS
    ts = ctx.get("_goal_set_at")
    if ts:
        try:
            if (time.time() - float(ts)) > max_age_days * 86400:
                return {}
        except Exception:
            return {}
    hours = _coerce_float(ctx.get("_goal_online_hours"))
    trips = _coerce_float(ctx.get("_goal_trip_count"))
    targets: Dict[str, float] = {}
    if hours and hours > 0:
        targets["online_hours"] = hours
    if trips and trips > 0:
        targets["trip_count"] = trips
    return targets

COMMITMENT_ADJUST_KEYWORDS = {
    "adjust", "change", "lower", "less", "different", "later", "not sure", "unsure", "maybe",
    "cannot", "can't", "cant", "no", "not now",
}

COMMITMENT_SKIP_KEYWORDS = {"skip", "later", "not now", "no thanks", "no thank you"}

DAY_ALIASES = {
    "mon": "Mon", "monday": "Mon",
    "tue": "Tue", "tues": "Tue", "tuesday": "Tue",
    "wed": "Wed", "weds": "Wed", "wednesday": "Wed",
    "thu": "Thu", "thur": "Thu", "thurs": "Thu", "thursday": "Thu",
    "fri": "Fri", "friday": "Fri",
    "sat": "Sat", "saturday": "Sat",
    "sun": "Sun", "sunday": "Sun",
}

DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

def _is_commitment_adjust_request(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    if any(token in lowered for token in COMMITMENT_ADJUST_KEYWORDS):
        return True
    return _is_negative_confirmation(lowered)

def _should_prompt_commitment(ctx: Dict[str, Any]) -> bool:
    if not ctx:
        return True
    if ctx.get("_awaiting_commitment_choice") or ctx.get("_awaiting_target_update") or ctx.get("_pending_goal"):
        return False
    last_prompt = ctx.get("_commitment_prompt_at")
    if last_prompt:
        try:
            if (time.time() - float(last_prompt)) < COMMITMENT_PROMPT_COOLDOWN_HOURS * 3600:
                return False
        except Exception:
            pass
    if _recent_goal_targets(ctx):
        return False
    return True

def _commitment_prompt_active(ctx: Dict[str, Any], *, max_age_hours: Optional[int] = None) -> bool:
    if not ctx:
        return False
    ts = ctx.get("_commitment_prompt_at")
    if not ts:
        return False
    if max_age_hours is None:
        max_age_hours = COMMITMENT_PROMPT_COOLDOWN_HOURS
    try:
        if (time.time() - float(ts)) > max_age_hours * 3600:
            ctx.pop("_commitment_prompt_at", None)
            ctx.pop("_commitment_prompt_targets", None)
            return False
    except Exception:
        return False
    return True

def _commitment_prompt_line(targets: Dict[str, float], *, include_update_hint: bool = False) -> str:
    hours = _fmt_hours(targets.get("online_hours"))
    trips = _fmt_trips(targets.get("trip_count"))
    target_bits = [bit for bit in [hours, trips] if bit]
    target_line = " / ".join(target_bits)
    if target_line:
        prompt = f"Reply YES to lock {target_line} or send your target."
    else:
        prompt = "Reply YES to lock your target or send your target."
    if include_update_hint:
        prompt += f" {_daily_update_hint()}"
    return prompt

def _extract_schedule_days(text: str) -> List[str]:
    if not text:
        return []
    lowered = text.lower()
    if "weekday" in lowered:
        return ["Mon", "Tue", "Wed", "Thu", "Fri"]
    if "weekend" in lowered:
        return ["Sat", "Sun"]
    found = []
    for token in re.findall(r"[a-z]{3,9}", lowered):
        mapped = DAY_ALIASES.get(token)
        if mapped and mapped not in found:
            found.append(mapped)
    if "mon" in lowered and "fri" in lowered and "mon-fri" in lowered:
        return ["Mon", "Tue", "Wed", "Thu", "Fri"]
    return sorted(found, key=lambda d: DAY_ORDER.index(d)) if found else []

def _format_schedule_days(days: List[str]) -> str:
    if not days:
        return ""
    if days == ["Sat", "Sun"]:
        return "weekends"
    if days == ["Mon", "Tue", "Wed", "Thu", "Fri"]:
        return "Mon–Fri"
    if len(days) == 1:
        return days[0]
    return ", ".join(days)

def _extract_schedule_hint(text: str) -> str:
    if not text:
        return "soon"
    lowered = _normalize_text(text).lower()
    if "tomorrow" in lowered or "tmr" in lowered:
        return "tomorrow"
    if "next week" in lowered:
        return "next week"
    if "this weekend" in lowered:
        return "this weekend"
    days = _extract_schedule_days(lowered)
    if days:
        return _format_schedule_days(days)
    if "today" in lowered:
        return "today"
    return "soon"

def _is_schedule_update(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if "tomorrow" in lowered or "tmr" in lowered or "next week" in lowered:
        return True
    if re.search(r"\b(back|online|return|resume|start)\b.*\b(weekend|monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b", lowered):
        return True
    if re.search(r"\b(off|offline|resting|break|leave)\b.*\b(today|tomorrow|this week|this weekend)\b", lowered):
        return True
    if "back online" in lowered or "online tomorrow" in lowered:
        return True
    return False

def _clear_stale_performance_followups(ctx: Dict[str, Any]) -> None:
    if not ctx:
        return
    now_ts = time.time()
    tips_at = ctx.get("_performance_tips_set_at")
    if tips_at:
        try:
            if (now_ts - float(tips_at)) > PERFORMANCE_FOLLOWUP_TTL_HOURS * 3600:
                ctx.pop("_awaiting_performance_tips", None)
                ctx.pop("_performance_tips_set_at", None)
        except Exception:
            pass

def _pick_phrase(options: List[str]) -> str:
    if not options:
        return ""
    try:
        return random.choice(options)
    except Exception:
        return options[0]

def _build_clarify_reply(msg: str, ctx: Dict[str, Any], driver: Dict[str, Any]) -> str:
    raw = _normalize_text(msg or "")
    lowered = raw.lower()
    choice_match = re.match(r"^\s*([1-4])(?:[.)])?\s*$", raw)
    if choice_match:
        choice = choice_match.group(1)
        if choice == "1":
            return "Got it — vehicle issue. Is it in the workshop/maintenance, no car/replacement, accident, or something else?"
        if choice == "2":
            login_url = "https://60b9b868ac0b.ngrok-free.app/driver/login"
            return (
                f"You can check your balance here: {login_url}. "
                "Use your personal code (South African ID number, or Traffic Register Number (TRN) for foreign nationals from your PrDP)."
            )
        if choice == "3":
            return "Got it — performance targets. Do you want today’s trips, your weekly target, or tips on times/areas?"
        if choice == "4":
            return "Got it — account/block or app issue. Are you seeing a suspension message, or is the app/login not working?"
    def _maybe_add_commitment_probe(text: str) -> str:
        if not text:
            return text
        if _has_goal_update_values(raw, allow_plain=True):
            return text
        if _is_medical_issue(raw) or _is_no_vehicle(raw) or _is_vehicle_repossession(raw) or _is_car_problem(raw):
            return text
        if ctx.get("_pending_intent") in {PENDING_NO_VEHICLE_REASON}:
            return text
        if ctx.get("_awaiting_target_update") or ctx.get("_pending_goal") or ctx.get("_awaiting_goal_confirm"):
            return text
        if not _should_prompt_commitment(ctx):
            return text
        targets = _recent_goal_targets(ctx) or get_model_targets(driver.get("asset_model"))
        ctx["_commitment_prompt_at"] = time.time()
        ctx["_commitment_prompt_targets"] = {
            "online_hours": targets.get("online_hours"),
            "trip_count": targets.get("trip_count"),
        }
        prompt = _commitment_prompt_line(targets)
        return f"{text} {prompt}".strip()
    if ctx.get("_pending_intent") == PENDING_NO_VEHICLE_REASON:
        last_prompt_at = ctx.get("_no_vehicle_prompt_at")
        if last_prompt_at:
            try:
                if (time.time() - float(last_prompt_at)) < NO_VEHICLE_PROMPT_COOLDOWN_SECONDS:
                    if _is_vehicle_back(raw) and not _is_no_vehicle(raw):
                        return (
                            "Glad you have the car back. Are you going online this week? "
                            "If yes, about how many trips can you commit to?"
                        )
                    return _pick_phrase(
                        [
                            "No rush. When you can, reply with one word: workshop, replacement, balance, or other.",
                            "When you have a moment, reply with workshop, replacement, balance, or other. If the car is back, just say 'car back'.",
                        ]
                    )
            except Exception:
                pass
        ctx["_no_vehicle_prompt_at"] = time.time()
        return _pick_phrase(
            [
                "Quick check - is the car in workshop/maintenance, waiting for a replacement, held for an outstanding balance, or something else?",
                "Got it. Is it workshop/maintenance, waiting for a replacement, outstanding balance, or something else?",
            ]
        )
    if "pick up" in lowered or "pickup" in lowered or "collect" in lowered:
        return _pick_phrase(
            [
                "Are you asking about vehicle pickup? Which branch are you closest to, or where is the car right now?",
                "Do you need pickup details? Tell me your closest branch and I'll guide you.",
            ]
        )
    if ctx.get("_awaiting_target_update") or ctx.get("_pending_goal") or ctx.get("_awaiting_goal_confirm"):
        return _pick_phrase(
            [
                "Got it. What hours or trips can you commit to this week? (e.g., '45 hours' or '100 trips').",
                "Cool - what can you commit to this week: hours or trips? (e.g., '45 hours' or '100 trips').",
                "No stress. What hours or trips feel realistic for you this week? (e.g., '45 hours' or '100 trips').",
            ]
        )
    last_intent = ctx.get("_last_kpi_intent")
    last_at = ctx.get("_last_kpi_intent_at") or 0
    if last_intent in KPI_INTENTS:
        try:
            recent = (time.time() - float(last_at)) < 6 * 3600
        except Exception:
            recent = False
        if recent:
            targets = _recent_goal_targets(ctx) or get_model_targets(driver.get("asset_model"))
            hours = _fmt_hours(targets.get("online_hours"))
            trips = _fmt_trips(targets.get("trip_count"))
            target_bits = [bit for bit in [hours, trips] if bit]
            target_line = f"Target right now: {' / '.join(target_bits)}. " if target_bits else ""
            prompt = _pick_phrase(
                [
                    "Do you want today's trips, your weekly target, or tips on times/areas?",
                    "Are you asking about today's trips, your weekly target, or tips on times/areas?",
                    "Do you want today's trips, the weekly target, or tips on where/when to drive?",
                ]
            )
            return f"{target_line}{prompt}".strip()
    if "balance" in lowered or "outstanding" in lowered or "owe" in lowered:
        login_url = "https://60b9b868ac0b.ngrok-free.app/driver/login"
        return _pick_phrase(
            [
                f"You can check your balance here: {login_url}. Use your personal code (South African ID number, or Traffic Register Number (TRN) for foreign nationals from your PrDP).",
                f"To view your balance, log in at {login_url}. Use your personal code (SA ID number, or TRN for foreign nationals from your PrDP).",
                f"Please check your balance at {login_url} and log in with your personal code (SA ID number, or TRN for foreign nationals from your PrDP).",
            ]
        )
    stage = ctx.get("_clarify_stage")
    if stage != "detail":
        ctx["_clarify_stage"] = "detail"
        return _maybe_add_commitment_probe(
            _pick_phrase(
            [
                "Quick check - is this about 1) a vehicle issue, 2) balance/payment, 3) performance targets, or 4) account/block or app/login issue? Reply with the number or a short line.",
                "Can you point me in the right direction: vehicle issue, balance/payment, performance targets, or account/block/app issue?",
            ]
            )
        )
    ctx["_clarify_stage"] = "broad"
    return _maybe_add_commitment_probe(
        _pick_phrase(
        [
            "Share a short sentence on what's happening (e.g., 'car in workshop', 'balance high', 'acceptance low') and I'll jump in.",
            "Give me one line about the issue so I can help (e.g., 'car in workshop', 'balance high', 'acceptance low').",
        ]
        )
    )

def _should_send_repossession_prompt(ctx: Dict[str, Any]) -> bool:
    if not ctx:
        return True
    ts = ctx.get("_repossession_prompted_at")
    if not ts:
        return True
    try:
        if (time.time() - float(ts)) < REPOSSESSION_PROMPT_COOLDOWN_HOURS * 3600:
            return False
    except Exception:
        return True
    return True
    plan_at = ctx.get("_performance_tips_sent_at")
    if plan_at:
        try:
            if (now_ts - float(plan_at)) > PERFORMANCE_FOLLOWUP_TTL_HOURS * 3600:
                ctx.pop("_awaiting_performance_plan", None)
                ctx.pop("_performance_tips_sent_at", None)
        except Exception:
            pass

def save_context_db(wa_id: str, last_intent: str, last_reply: str, prefs: Dict[str, Any]):
    if not mysql_available():
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {MYSQL_DB}.whatsapp_context_memory (wa_id, last_intent, last_reply, prefs_json)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  last_intent=VALUES(last_intent),
                  last_reply=VALUES(last_reply),
                  prefs_json=VALUES(prefs_json),
                  updated_at=CURRENT_TIMESTAMP
            """, (wa_id, last_intent, last_reply, _json_dumps(prefs)))
    except Exception as e:
        log.error("Context memory upsert failed: %s", e)

# -----------------------------------------------------------------------------
# Driver issue ticket helpers (car problems etc.)
# -----------------------------------------------------------------------------
ISSUE_TICKET_TABLE = f"{MYSQL_DB}.driver_issue_tickets"
ADMIN_TABLE = f"{MYSQL_DB}.driver_issue_admins"
_admin_manage_flag_supported: Optional[bool] = None
INTERACTION_TABLE = f"{MYSQL_DB}.driver_interactions"
ENGAGEMENT_CAMPAIGN_TABLE = f"{MYSQL_DB}.driver_engagement_campaigns"
ENGAGEMENT_ROW_TABLE = f"{MYSQL_DB}.driver_engagement_rows"
ENGAGEMENT_RESPONSE_WINDOW_DAYS = int(os.getenv("ENGAGEMENT_RESPONSE_WINDOW_DAYS", "3"))
ENGAGEMENT_PREVIEW_TTL_SECONDS = int(os.getenv("ENGAGEMENT_PREVIEW_TTL_SECONDS", str(30 * 60)))
ENGAGEMENT_MAX_ROWS = int(os.getenv("ENGAGEMENT_MAX_ROWS", "5000"))
ENGAGEMENT_TARGET_ONLINE_HOURS_MIN = float(os.getenv("ENGAGEMENT_TARGET_ONLINE_HOURS_MIN", "55"))
ENGAGEMENT_TARGET_ONLINE_HOURS_MAX = float(os.getenv("ENGAGEMENT_TARGET_ONLINE_HOURS_MAX", "60"))
ENGAGEMENT_TARGET_EPH = float(os.getenv("ENGAGEMENT_TARGET_EPH", "164"))
ENGAGEMENT_TARGET_TRIPS = float(os.getenv("ENGAGEMENT_TARGET_TRIPS", "110"))
ENGAGEMENT_TARGET_ACCEPTANCE = float(os.getenv("ENGAGEMENT_TARGET_ACCEPTANCE", "65"))
GOAL_TARGET_MIN_RATIO = float(os.getenv("GOAL_TARGET_MIN_RATIO", "0.9"))
GOAL_TARGET_TTL_DAYS = int(os.getenv("GOAL_TARGET_TTL_DAYS", "14"))
PERFORMANCE_FOLLOWUP_TTL_HOURS = int(os.getenv("PERFORMANCE_FOLLOWUP_TTL_HOURS", "24"))
REPOSSESSION_PROMPT_COOLDOWN_HOURS = int(os.getenv("REPOSSESSION_PROMPT_COOLDOWN_HOURS", "6"))
COMMITMENT_PROMPT_COOLDOWN_HOURS = int(os.getenv("COMMITMENT_PROMPT_COOLDOWN_HOURS", "24"))


def create_driver_issue_ticket(
    wa_id: str,
    initial_message: str,
    driver: Dict[str, Any],
    *,
    issue_type: str = "car_problem",
) -> Optional[int]:
    if not mysql_available():
        return None
    metadata = {
        "driver_display_name": driver.get("display_name"),
        "asset_model": driver.get("asset_model"),
        "created_at_ts": time.time(),
    }
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {ISSUE_TICKET_TABLE}
                  (wa_id, issue_type, status, initial_message, media_urls, metadata)
                VALUES (%s, %s, %s, %s, JSON_ARRAY(), %s)
                """,
                (
                    wa_id,
                    issue_type,
                    "collecting",
                    initial_message,
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            ticket_id = cur.lastrowid
        return int(ticket_id) if ticket_id else None
    except Exception as exc:
        log.error("create_driver_issue_ticket failed: %s", exc)
        return None


def append_driver_issue_media(ticket_id: int, media: Dict[str, Any]) -> bool:
    if not (mysql_available() and ticket_id and media):
        return False
    payload = json.dumps(media, ensure_ascii=False)
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ISSUE_TICKET_TABLE}
                SET media_urls = CASE
                        WHEN media_urls IS NULL THEN JSON_ARRAY(CAST(%s AS JSON))
                        ELSE JSON_ARRAY_APPEND(media_urls, '$', CAST(%s AS JSON))
                    END,
                    metadata = JSON_SET(IFNULL(metadata, JSON_OBJECT()), '$.photos_received', TRUE),
                    last_update_at = CURRENT_TIMESTAMP
                WHERE id=%s
                """,
                (payload, payload, ticket_id),
            )
            updated = cur.rowcount > 0
        return updated
    except Exception as exc:
        log.error("append_driver_issue_media failed: %s", exc)
        return False


def update_driver_issue_location(
    ticket_id: int,
    *,
    latitude: Optional[float],
    longitude: Optional[float],
    description: Optional[str] = None,
    raw: Optional[Dict[str, Any]] = None,
) -> bool:
    if not (mysql_available() and ticket_id):
        return False
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ISSUE_TICKET_TABLE}
                SET location_lat=%s,
                    location_lng=%s,
                    location_desc=%s,
                    metadata = JSON_SET(
                        IFNULL(metadata, JSON_OBJECT()),
                        '$.location_received', TRUE,
                        '$.location_raw', CAST(%s AS JSON)
                    ),
                    last_update_at = CURRENT_TIMESTAMP
                WHERE id=%s
                """,
                (
                    latitude,
                    longitude,
                    description,
                    json.dumps(raw or {}, ensure_ascii=False),
                    ticket_id,
                ),
            )
            updated = cur.rowcount > 0
        return updated
    except Exception as exc:
        log.error("update_driver_issue_location failed: %s", exc)
        return False


def update_driver_issue_status(ticket_id: int, status: str) -> bool:
    if not (mysql_available() and ticket_id):
        return False
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ISSUE_TICKET_TABLE}
                SET status=%s,
                    metadata = JSON_SET(IFNULL(metadata, JSON_OBJECT()), '$.last_status', %s),
                    last_update_at = CURRENT_TIMESTAMP
                WHERE id=%s
                """,
                (status, status, ticket_id),
            )
            updated = cur.rowcount > 0
        return updated
    except Exception as exc:
        log.error("update_driver_issue_status failed: %s", exc)
        return False


def update_driver_issue_metadata(ticket_id: int, patch: Dict[str, Any]) -> bool:
    if not (mysql_available() and ticket_id and patch):
        return False
    payload = json.dumps(patch, ensure_ascii=False)
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ISSUE_TICKET_TABLE}
                SET metadata = JSON_MERGE_PATCH(IFNULL(metadata, JSON_OBJECT()), CAST(%s AS JSON)),
                    last_update_at = CURRENT_TIMESTAMP
                WHERE id=%s
                """,
                (payload, ticket_id),
            )
            updated = cur.rowcount > 0
        return updated
    except Exception as exc:
        log.error("update_driver_issue_metadata failed: %s", exc)
        return False


def log_driver_issue_ticket_event(
    ticket_id: int,
    *,
    admin_email: Optional[str],
    action_type: str,
    from_status: Optional[str] = None,
    to_status: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    if not (mysql_available() and ticket_id and action_type):
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {MYSQL_DB}.driver_issue_ticket_logs
                  (ticket_id, admin_email, action_type, from_status, to_status, note)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (ticket_id, admin_email, action_type, from_status, to_status, note),
            )
    except Exception as exc:
        log.warning("log_driver_issue_ticket_event failed: %s", exc)


def _build_ticket_closed_message(ticket: Dict[str, Any], reason: Optional[str]) -> str:
    issue_type = (ticket.get("issue_type") or "").strip().lower()
    reason_clean = (reason or "").strip()
    if issue_type == "pop_submission":
        body = (
            "Payment update: your POP has been validated and the payment allocated to your account. "
            "This ticket is now closed."
        )
    else:
        body = "Update: your ticket has been closed."
    if reason_clean:
        body = f"{body} Reason: {reason_clean}"
    return body


def _notify_driver_ticket_closed(
    *,
    ticket: Dict[str, Any],
    reason: Optional[str],
    admin_email: Optional[str],
) -> None:
    wa_id = ticket.get("wa_id")
    if not wa_id:
        return
    message = _build_ticket_closed_message(ticket, reason)
    outbound_id = send_whatsapp_text(wa_id, message)
    send_status = "sent" if outbound_id else "send_failed"
    timestamp_unix = str(int(time.time()))
    s_label, s_score, s_raw = analyze_sentiment(message)
    log_message(
        direction="OUTBOUND",
        wa_id=wa_id,
        text=message,
        intent=None,
        status=send_status,
        wa_message_id=outbound_id,
        message_id=outbound_id,
        business_number=None,
        phone_number_id=None,
        origin_type="ticket_status",
        raw_json={"ticket_id": ticket.get("id"), "status": "closed", "admin": admin_email},
        timestamp_unix=timestamp_unix,
        sentiment=s_label,
        sentiment_score=s_score,
        intent_label=None,
        ai_raw=s_raw,
        conversation_id=f"ticket-closed-{ticket.get('id')}-{timestamp_unix}",
    )


def fetch_driver_issue_logs(ticket_id: int, limit: int = 50) -> List[Dict[str, Any]]:
    if not (mysql_available() and ticket_id):
        return []
    limit = max(1, min(limit, 200))
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, admin_email, action_type, from_status, to_status, note, created_at
                FROM {MYSQL_DB}.driver_issue_ticket_logs
                WHERE ticket_id=%s
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (ticket_id, limit),
            )
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("fetch_driver_issue_logs failed: %s", exc)
        return []
    return rows


# -----------------------------------------------------------------------------
# Admin auth helpers
# -----------------------------------------------------------------------------
def _normalize_email(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    cleaned = value.strip().lower()
    return cleaned or None


def _admin_manage_flag_available() -> bool:
    global _admin_manage_flag_supported
    if _admin_manage_flag_supported is not None:
        return _admin_manage_flag_supported
    if not mysql_available():
        _admin_manage_flag_supported = False
        return False
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND COLUMN_NAME = 'can_manage_users'
                """,
                (MYSQL_DB, "driver_issue_admins"),
            )
            row = cur.fetchone()
            _admin_manage_flag_supported = bool(row and row.get("cnt"))
    except Exception as exc:
        log.debug("could not detect can_manage_users column: %s", exc)
        _admin_manage_flag_supported = False
    return _admin_manage_flag_supported


def _prepare_password_bytes(password: Optional[str]) -> bytes:
    raw_value = password or ""
    password_bytes = raw_value.encode("utf-8")
    if len(password_bytes) > BCRYPT_MAX_PASSWORD_BYTES:
        global _bcrypt_truncation_warned
        if not _bcrypt_truncation_warned:
            log.warning(
                "bcrypt password exceeds %d bytes and will be truncated before hashing",
                BCRYPT_MAX_PASSWORD_BYTES,
            )
            _bcrypt_truncation_warned = True
        password_bytes = password_bytes[:BCRYPT_MAX_PASSWORD_BYTES]
    return password_bytes


def _hash_password(password: str) -> str:
    password_bytes = _prepare_password_bytes(password)
    salt = bcrypt.gensalt(rounds=BCRYPT_ROUNDS)
    return bcrypt.hashpw(password_bytes, salt).decode("utf-8")


def _verify_password(plain_password: str, hashed_password: str) -> bool:
    if not hashed_password:
        return False
    try:
        password_bytes = _prepare_password_bytes(plain_password)
        hashed_bytes = hashed_password if isinstance(hashed_password, bytes) else hashed_password.encode("utf-8")
        return bcrypt.checkpw(password_bytes, hashed_bytes)
    except Exception as exc:
        log.debug("bcrypt verify failed: %s", exc)
        return False


def _encode_admin_session(admin_id: int) -> str:
    payload = {"admin_id": int(admin_id), "ts": int(time.time())}
    return _admin_serializer.dumps(payload)


def _decode_admin_session(token: str) -> Optional[int]:
    try:
        data = _admin_serializer.loads(token, max_age=ADMIN_SESSION_MAX_AGE)
    except SignatureExpired:
        return None
    except BadSignature:
        return None
    try:
        return int(data.get("admin_id"))
    except (TypeError, ValueError):
        return None


def _set_admin_session_cookie(response, admin_id: int) -> None:
    token = _encode_admin_session(admin_id)
    response.set_cookie(
        ADMIN_COOKIE_NAME,
        token,
        max_age=ADMIN_SESSION_MAX_AGE,
        expires=ADMIN_SESSION_MAX_AGE,
        httponly=True,
        secure=ADMIN_COOKIE_SECURE,
        samesite=ADMIN_COOKIE_SAMESITE,
    )


def _clear_admin_session_cookie(response) -> None:
    response.delete_cookie(ADMIN_COOKIE_NAME)


def _fetch_admin_by(where_clause: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    if not mysql_available():
        return None
    manage_supported = _admin_manage_flag_available()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            extra = ", IFNULL(can_manage_users,1) AS can_manage_users" if manage_supported else ""
            cur.execute(
                f"SELECT id, email, password_hash, is_active, created_at, updated_at{extra} FROM {ADMIN_TABLE} WHERE {where_clause} LIMIT 1",
                params,
            )
            row = cur.fetchone()
        if row and "can_manage_users" not in row:
            row["can_manage_users"] = True
        return row
    except Exception as exc:
        log.error("fetch admin failed: %s", exc)
        return None


def get_admin_user_by_email(email: Optional[str]) -> Optional[Dict[str, Any]]:
    normalized = _normalize_email(email)
    if not normalized:
        return None
    return _fetch_admin_by("email=%s", (normalized,))


def get_admin_user_by_id(admin_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if not admin_id:
        return None
    return _fetch_admin_by("id=%s", (int(admin_id),))


def ensure_admin_bootstrap_user() -> None:
    if not (ADMIN_BOOTSTRAP_EMAIL and ADMIN_BOOTSTRAP_PASSWORD):
        return
    email = _normalize_email(ADMIN_BOOTSTRAP_EMAIL)
    if not email:
        return
    if not mysql_available():
        log.warning("Admin bootstrap skipped: MySQL unavailable")
        return
    existing = get_admin_user_by_email(email)
    if existing:
        return
    password_hash = _hash_password(ADMIN_BOOTSTRAP_PASSWORD)
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {ADMIN_TABLE} (email, password_hash, is_active) VALUES (%s, %s, 1)",
                (email, password_hash),
            )
        log.info("Bootstrap admin user created for %s", email)
    except Exception as exc:
        log.error("Failed to create bootstrap admin user: %s", exc)


def fetch_admin_users() -> List[Dict[str, Any]]:
    if not mysql_available():
        return []
    manage_supported = _admin_manage_flag_available()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            extra = ", IFNULL(can_manage_users,1) AS can_manage_users" if manage_supported else ""
            cur.execute(
                f"SELECT id, email, is_active, created_at, updated_at{extra} FROM {ADMIN_TABLE} ORDER BY created_at DESC, id DESC"
            )
            rows = cur.fetchall() or []
        for r in rows:
            if "can_manage_users" not in r:
                r["can_manage_users"] = True
        return rows
    except Exception as exc:
        log.error("fetch admin users failed: %s", exc)
        return []


def create_admin_user(email: str, password: str) -> Tuple[bool, str]:
    normalized = _normalize_email(email)
    if not normalized:
        return False, "Please enter a valid email."
    if not password or len(password) < 6:
        return False, "Password must be at least 6 characters."
    if not mysql_available():
        return False, "Database unavailable."
    existing = get_admin_user_by_email(normalized)
    if existing:
        return False, "An account with that email already exists."
    password_hash = _hash_password(password)
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {ADMIN_TABLE} (email, password_hash, is_active) VALUES (%s, %s, 1)",
                (normalized, password_hash),
            )
        return True, ""
    except Exception as exc:
        log.error("create admin user failed: %s", exc)
        return False, "Could not create user. Please try again."


def change_admin_password(email: str, password: str) -> Tuple[bool, str]:
    normalized = _normalize_email(email)
    if not normalized:
        return False, "Please enter a valid email."
    if not password or len(password) < 6:
        return False, "Password must be at least 6 characters."
    user = get_admin_user_by_email(normalized)
    if not user:
        return False, "No admin found with that email."
    if not user.get("is_active"):
        return False, "That admin is disabled."
    ok = update_admin_password(user["id"], password)
    if not ok:
        return False, "Could not update the password. Please try again."
    return True, ""


def update_admin_manage_flag(email: str, can_manage: bool) -> Tuple[bool, str]:
    if not _admin_manage_flag_available():
        return False, "Permission flag not available in the database. Please add the 'can_manage_users' column."
    normalized = _normalize_email(email)
    if not normalized:
        return False, "Please enter a valid email."
    user = get_admin_user_by_email(normalized)
    if not user:
        return False, "No admin found with that email."
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {ADMIN_TABLE} SET can_manage_users=%s, updated_at=CURRENT_TIMESTAMP WHERE id=%s",
                (1 if can_manage else 0, user["id"]),
            )
        return True, ""
    except Exception as exc:
        log.error("Failed to update admin manage flag: %s", exc)
        return False, "Could not update admin permissions. Please try again."


def get_whatsapp_templates() -> List[Dict[str, Any]]:
    return APPROVED_WA_TEMPLATES


def email_delivery_available() -> bool:
    return bool(SMTP_HOST and SMTP_USERNAME and SMTP_PASSWORD)


def send_password_reset_email(recipient: str, reset_link: str) -> bool:
    if not email_delivery_available():
        log.warning(
            "Password reset email not sent (SMTP not configured) for %s. Link=%s",
            recipient,
            reset_link,
        )
        return False
    try:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["Subject"] = "Dineo Ops Portal password reset"
        msg["From"] = SMTP_USERNAME
        msg["To"] = recipient
        msg.set_content(
            f"Hi,\n\nUse the link below to reset your Dineo Ops Portal password. "
            f"The link expires in {RESET_TOKEN_MAX_AGE // 60} minutes.\n\n{reset_link}\n\n"
            "If you didn’t request this, you can ignore the email.\n"
        )

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        try:
            if SMTP_USE_TLS:
                server.starttls()
            if SMTP_USERNAME and SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                pass
        return True
    except Exception as exc:
        log.error("Failed to send password reset email to %s: %s", recipient, exc)
        log.info("Reset link for troubleshooting: %s", reset_link)
        return False


def create_password_reset_token(admin_id: int, email: str) -> str:
    payload = {"admin_id": int(admin_id), "email": _normalize_email(email), "ts": int(time.time())}
    return _reset_serializer.dumps(payload)


def resolve_password_reset_token(token: str) -> Optional[Dict[str, Any]]:
    if not token:
        return None
    try:
        data = _reset_serializer.loads(token, max_age=RESET_TOKEN_MAX_AGE)
    except SignatureExpired:
        return None
    except BadSignature:
        return None
    admin_id = data.get("admin_id")
    email = data.get("email")
    if not admin_id or not email:
        return None
    user = get_admin_user_by_id(admin_id)
    if not user:
        return None
    if _normalize_email(user.get("email")) != _normalize_email(email):
        return None
    if not user.get("is_active"):
        return None
    return user


def update_admin_password(admin_id: int, new_password: str) -> bool:
    if not (mysql_available() and admin_id and new_password):
        return False
    password_hash = _hash_password(new_password)
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {ADMIN_TABLE} SET password_hash=%s, updated_at=CURRENT_TIMESTAMP WHERE id=%s",
                (password_hash, int(admin_id)),
            )
            return cur.rowcount > 0
    except Exception as exc:
        log.error("Failed to update admin password: %s", exc)
        return False


def get_authenticated_admin(request: Request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get(ADMIN_COOKIE_NAME)
    if not token:
        return None
    admin_id = _decode_admin_session(token)
    if not admin_id:
        return None
    user = get_admin_user_by_id(admin_id)
    if not user or not user.get("is_active"):
        return None
    return user


def send_generic_email(recipient: str, subject: str, body: str) -> bool:
    if not email_delivery_available():
        log.warning("Email not sent (SMTP not configured) for %s. Subject=%s", recipient, subject)
        return False
    try:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = SMTP_USERNAME
        msg["To"] = recipient
        msg.set_content(body or "")

        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        try:
            if SMTP_USE_TLS:
                server.starttls()
            if SMTP_USERNAME and SMTP_PASSWORD:
                server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)
        finally:
            try:
                server.quit()
            except Exception:
                pass
        return True
    except Exception as exc:
        log.error("Failed to send email to %s: %s", recipient, exc)
        return False


def _ensure_interaction_table():
    if not mysql_available():
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {INTERACTION_TABLE} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    wa_id VARCHAR(64) NOT NULL,
                    channel VARCHAR(32) NOT NULL,
                    template_id VARCHAR(128) NULL,
                    variables_json TEXT NULL,
                    amount DECIMAL(18,2) NULL,
                    ptp_date DATE NULL,
                    ptp_payment DECIMAL(18,2) NULL,
                    admin_email VARCHAR(255) NULL,
                    status VARCHAR(32) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_wa_created (wa_id, created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # ensure ptp_payment exists
            cur.execute(
                f"""
                SELECT COUNT(*) AS cnt FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME='ptp_payment'
                """,
                (MYSQL_DB, INTERACTION_TABLE.split(".")[1]),
            )
            row = cur.fetchone()
            if not row or not row.get("cnt"):
                cur.execute(f"ALTER TABLE {INTERACTION_TABLE} ADD COLUMN ptp_payment DECIMAL(18,2) NULL AFTER ptp_date")
    except Exception as exc:
        log.debug("Could not ensure interaction table: %s", exc)


def log_interaction(
    wa_id: str,
    channel: str,
    *,
    template_id: Optional[str] = None,
    variables_json: Optional[str] = None,
    amount: Optional[float] = None,
    ptp_date: Optional[str] = None,
    ptp_payment: Optional[float] = None,
    admin_email: Optional[str] = None,
    status: str = "sent",
):
    if not mysql_available():
        return
    _ensure_interaction_table()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {INTERACTION_TABLE} (wa_id, channel, template_id, variables_json, amount, ptp_date, ptp_payment, admin_email, status) "
                f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (wa_id, channel, template_id, variables_json, amount, ptp_date, ptp_payment, admin_email, status),
            )
    except Exception as exc:
        log.debug("log_interaction failed: %s", exc)


def get_interaction_history(wa_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    if not mysql_available():
        return []
    _ensure_interaction_table()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT wa_id, channel, template_id, variables_json, amount, ptp_date, ptp_payment, admin_email, status, created_at "
                f"FROM {INTERACTION_TABLE} WHERE wa_id=%s ORDER BY created_at DESC LIMIT %s",
                (wa_id, limit),
            )
            rows = cur.fetchall() or []
        return rows
    except Exception as exc:
        log.debug("get_interaction_history failed: %s", exc)
        return []


def get_message_history(wa_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    if not mysql_available():
        return []
    variants = _wa_number_variants(wa_id)
    if not variants:
        return []
    try:
        conn = get_mysql()
        table = _detect_logs_table(conn)
        available = _get_table_columns(conn, table)
        pick = lambda candidates: next((c for c in candidates if c in available), None)

        wa_col = pick(["wa_id", "phone", "whatsapp_number", "whatsapp", "wa_number", "phone_number", "contact_number"])
        msg_col = pick(SYNONYMS["message_text"] + ["text", "body"])
        dir_col = pick(SYNONYMS["message_direction"])
        ts_col = pick(["created_at", "timestamp", "logged_at", "message_timestamp"])
        status_col = pick(["status", "send_status"])
        origin_col = pick(["origin_type", "origin", "source"])
        msg_id_col = pick(SYNONYMS["wa_message_id"] + ["message_id"])
        order_col = ts_col or pick(["id"])

        if not wa_col or not msg_col:
            return []

        select_cols: List[str] = [f"{wa_col} AS wa_id", f"{msg_col} AS message_text"]
        if dir_col:
            select_cols.append(f"{dir_col} AS message_direction")
        if status_col:
            select_cols.append(f"{status_col} AS status")
        if origin_col:
            select_cols.append(f"{origin_col} AS origin_type")
        if ts_col:
            select_cols.append(f"{ts_col} AS logged_at")
        if msg_id_col:
            select_cols.append(f"{msg_id_col} AS message_id")

        placeholders = ", ".join(["%s"] * len(variants))
        where_clauses = [
            f"{wa_col} IN ({placeholders})",
            f"{msg_col} IS NOT NULL",
            f"TRIM({msg_col}) <> ''",
        ]
        if dir_col:
            where_clauses.append(f"(UPPER({dir_col}) <> 'STATUS' OR {dir_col} IS NULL)")
        sql = f"SELECT {', '.join(select_cols)} FROM {table} WHERE " + " AND ".join(where_clauses)
        if order_col:
            sql += f" ORDER BY {order_col} DESC"
        sql += " LIMIT %s"

        with conn.cursor() as cur:
            cur.execute(sql, (*variants, limit))
            rows = cur.fetchall() or []
        if not rows:
            return []
        if msg_id_col and status_col:
            message_ids = [row.get("message_id") for row in rows if row.get("message_id")]
            if message_ids:
                status_cols = [f"{msg_id_col} AS message_id", f"{status_col} AS status"]
                if ts_col:
                    status_cols.append(f"{ts_col} AS logged_at")
                status_placeholders = ", ".join(["%s"] * len(message_ids))
                status_sql = (
                    f"SELECT {', '.join(status_cols)} FROM {table} "
                    f"WHERE {msg_id_col} IN ({status_placeholders}) AND {status_col} IS NOT NULL"
                )
                if ts_col:
                    status_sql += f" ORDER BY {ts_col} DESC"
                try:
                    with conn.cursor() as cur:
                        cur.execute(status_sql, (*message_ids,))
                        status_rows = cur.fetchall() or []
                except Exception:
                    status_rows = []

                status_map: Dict[str, Dict[str, Any]] = {}
                for row in status_rows:
                    message_id = row.get("message_id")
                    status = row.get("status")
                    if not message_id or not status:
                        continue
                    key = str(message_id)
                    status_value = str(status).strip().lower()
                    entry = status_map.setdefault(key, {"badges": [], "latest": None})
                    if entry["latest"] is None:
                        entry["latest"] = status_value
                    if status_value and status_value not in entry["badges"]:
                        entry["badges"].append(status_value)

                for row in rows:
                    message_id = row.get("message_id")
                    key = str(message_id) if message_id else None
                    if key and key in status_map:
                        badges = status_map[key].get("badges") or []
                        if badges:
                            row["status_badges"] = badges
                        if not row.get("status") and status_map[key].get("latest"):
                            row["status"] = status_map[key]["latest"]
                    elif row.get("status"):
                        row["status_badges"] = [str(row.get("status")).strip().lower()]
        else:
            for row in rows:
                if row.get("status"):
                    row["status_badges"] = [str(row.get("status")).strip().lower()]
        return rows
    except Exception as exc:
        log.debug("get_message_history failed: %s", exc)
        return []


def get_all_interactions(limit: int = 200) -> List[Dict[str, Any]]:
    if not mysql_available():
        return []
    _ensure_interaction_table()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT wa_id, channel, template_id, variables_json, amount, ptp_date, ptp_payment, admin_email, status, created_at
                FROM {INTERACTION_TABLE}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall() or []
        return rows
    except Exception as exc:
        log.debug("get_all_interactions failed: %s", exc)
        return []


ENGAGEMENT_DRIVER_TYPE_TEMPLATES = {
    "needs attention": "performance_needs_attention",
    "no trips yet": "performance_no_trips_yet",
}
ENGAGEMENT_DEFAULT_TEMPLATE = "performance_engagement_boost"
ENGAGEMENT_VARIABLE_SOURCES = {
    "name": ["display_name"],
    "driver_type": ["driver_type"],
    "status": ["status"],
    "model": ["model"],
    "car_reg": ["car_reg"],
    "contract_start_date": ["contract_start_date"],
    "online_hours": ["online_hours"],
    "acceptance_rate": ["acceptance_rate"],
    "gross_earnings": ["gross_earnings"],
    "earnings_per_hour": ["earnings_per_hour"],
    "amount": ["amount", "xero_balance", "outstanding", "balance"],
    "xero_balance": ["xero_balance"],
    "payments": ["payments"],
    "trip_count": ["trip_count"],
    "collections_agent": ["collections_agent"],
}
ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE = "performance"
ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS = "collections"
ENGAGEMENT_PAGE_CONFIGS = {
    ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE: {
        "campaign_type": ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE,
        "base_path": "/admin/engagements",
        "nav_active": "performance_ai",
        "page_title": "Performance AI",
        "page_upload_title": "Upload underperforming driver list",
        "page_description": (
            "Upload the CSV export (with columns like Display Name, WhatsApp, Driver Type, Online Hours). "
            "We will preview the list, map templates by driver type, and confirm before sending."
        ),
        "commitment_mode": "kpi",
        "commitment_label": "Committed (KPI)",
        "commitment_rate_label": "KPI commitment rate",
        "commitment_column_label": "Committed (KPI)",
        "kpi_mode": "performance",
        "followup_template_id": "performance_followup_24h",
        "show_ptp": False,
        "show_uplift": True,
    },
    ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS: {
        "campaign_type": ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS,
        "base_path": "/admin/collections-ai",
        "nav_active": "collections_ai",
        "page_title": "Collections AI",
        "page_upload_title": "Upload collections driver list",
        "page_description": (
            "Upload the CSV export for collections outreach. We will preview the list, map templates, "
            "and confirm before sending."
        ),
        "commitment_mode": "payment",
        "commitment_label": "PTP commitments",
        "commitment_rate_label": "PTP rate",
        "commitment_column_label": "PTP",
        "kpi_mode": "collections",
        "followup_template_id": "collections_followup_24h",
        "show_ptp": False,
        "show_uplift": False,
    },
}

SOFTPHONE_URL = os.getenv(
    "SOFTPHONE_URL",
    "https://oaksurefinancialservices-cxm.cnx1.cloud/work",
)


def _get_engagement_page_config(campaign_type: str) -> Dict[str, Any]:
    if campaign_type in ENGAGEMENT_PAGE_CONFIGS:
        return ENGAGEMENT_PAGE_CONFIGS[campaign_type]
    return ENGAGEMENT_PAGE_CONFIGS[ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE]

def _parse_metric_value(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"n/a", "na", "null"}:
        return None
    cleaned = re.sub(r"[^0-9.\-]+", "", text)
    if cleaned in {"", "-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None

def _parse_metric_percent(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace("%", "")
    return _parse_metric_value(text)

def _baseline_metrics_from_row(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    return {
        "online_hours": _parse_metric_value(row.get("online_hours")),
        "acceptance_rate": _parse_metric_percent(row.get("acceptance_rate")),
        "earnings_per_hour": _parse_metric_value(row.get("earnings_per_hour")),
        "trip_count": _parse_metric_value(row.get("trip_count")),
    }


def _baseline_collections_metrics_from_row(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    return {
        "balance": _parse_metric_value(row.get("xero_balance")),
        "payments_7d": _parse_metric_value(row.get("payments")),
    }

def _format_metric_value(
    value: Optional[float],
    *,
    decimals: int = 1,
    suffix: str = "",
    money: bool = False,
    integer: bool = False,
) -> str:
    if value is None:
        return "-"
    if money:
        return fmt_rands(value)
    if integer:
        return str(int(round(value)))
    return f"{value:.{decimals}f}{suffix}"

def _format_delta_value(
    value: Optional[float],
    *,
    decimals: int = 1,
    suffix: str = "",
    money: bool = False,
    integer: bool = False,
) -> str:
    if value is None:
        return "-"
    sign = "+" if value > 0 else "-" if value < 0 else ""
    abs_val = abs(value)
    if money:
        text = fmt_rands(abs_val)
    elif integer:
        text = str(int(round(abs_val)))
    else:
        text = f"{abs_val:.{decimals}f}{suffix}"
    return f"{sign}{text}" if sign else text

def _delta_class(value: Optional[float]) -> str:
    if value is None:
        return "flat"
    if value > 0:
        return "up"
    if value < 0:
        return "down"
    return "flat"

def _build_engagement_kpi_compare(
    baseline: Dict[str, Any],
    current: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    compare: Dict[str, Dict[str, Any]] = {}
    display: List[Dict[str, Any]] = []

    def add_metric(
        key: str,
        label: str,
        base_val: Optional[float],
        cur_val: Optional[float],
        *,
        decimals: int = 1,
        suffix: str = "",
        money: bool = False,
        integer: bool = False,
        target_min: Optional[float] = None,
        target_max: Optional[float] = None,
        target: Optional[float] = None,
        target_label: str = "",
    ) -> None:
        delta = None
        if base_val is not None and cur_val is not None:
            delta = cur_val - base_val
        met_target = None
        if cur_val is not None:
            if target_min is not None or target_max is not None:
                met_target = True
                if target_min is not None and cur_val < target_min:
                    met_target = False
                if target_max is not None and cur_val > target_max:
                    met_target = False
            elif target is not None:
                met_target = cur_val >= target
        compare[key] = {
            "label": label,
            "baseline": base_val,
            "current": cur_val,
            "delta": delta,
            "target_met": met_target,
            "target_label": target_label,
        }
        display.append(
            {
                "label": label,
                "baseline": _format_metric_value(base_val, decimals=decimals, suffix=suffix, money=money, integer=integer),
                "current": _format_metric_value(cur_val, decimals=decimals, suffix=suffix, money=money, integer=integer),
                "delta": _format_delta_value(delta, decimals=decimals, suffix=suffix, money=money, integer=integer),
                "delta_class": _delta_class(delta),
                "target_met": met_target,
                "target_label": target_label,
            }
        )

    base_hours = _parse_metric_value(baseline.get("online_hours")) if baseline else None
    cur_hours = _parse_metric_value(current.get("online_hours")) if current else None
    add_metric(
        "online_hours",
        "Online hours",
        base_hours,
        cur_hours,
        decimals=1,
        target_min=ENGAGEMENT_TARGET_ONLINE_HOURS_MIN,
        target_max=ENGAGEMENT_TARGET_ONLINE_HOURS_MAX,
        target_label=f"{ENGAGEMENT_TARGET_ONLINE_HOURS_MIN:.0f}-{ENGAGEMENT_TARGET_ONLINE_HOURS_MAX:.0f}h",
    )

    base_acc = _parse_metric_percent(baseline.get("acceptance_rate")) if baseline else None
    cur_acc = _parse_metric_percent(current.get("acceptance_rate")) if current else None
    add_metric(
        "acceptance_rate",
        "Acceptance rate",
        base_acc,
        cur_acc,
        decimals=1,
        suffix="%",
        target=ENGAGEMENT_TARGET_ACCEPTANCE,
        target_label=f"{ENGAGEMENT_TARGET_ACCEPTANCE:.0f}%+",
    )

    base_eph = _parse_metric_value(baseline.get("earnings_per_hour")) if baseline else None
    cur_eph = _parse_metric_value(current.get("earnings_per_hour")) if current else None
    add_metric(
        "earnings_per_hour",
        "Earnings / hour",
        base_eph,
        cur_eph,
        money=True,
        target=ENGAGEMENT_TARGET_EPH,
        target_label=f"{fmt_rands(ENGAGEMENT_TARGET_EPH)}+",
    )

    base_trips = _parse_metric_value(baseline.get("trip_count")) if baseline else None
    cur_trips = _parse_metric_value(current.get("trip_count")) if current else None
    add_metric(
        "trip_count",
        "Trip count",
        base_trips,
        cur_trips,
        integer=True,
        target=ENGAGEMENT_TARGET_TRIPS,
        target_label=f"{ENGAGEMENT_TARGET_TRIPS:.0f}+",
    )

    return compare, display


def _ensure_engagement_tables() -> None:
    if not mysql_available():
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ENGAGEMENT_CAMPAIGN_TABLE} (
                    id VARCHAR(64) PRIMARY KEY,
                    admin_email VARCHAR(255) NULL,
                    source_filename VARCHAR(255) NULL,
                    campaign_type VARCHAR(32) NOT NULL DEFAULT 'performance',
                    template_map_json TEXT NULL,
                    total_rows INT NOT NULL DEFAULT 0,
                    sent_count INT NOT NULL DEFAULT 0,
                    failed_count INT NOT NULL DEFAULT 0,
                    skipped_count INT NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            if hasattr(_get_table_columns, "_cache"):
                _get_table_columns._cache.pop(ENGAGEMENT_CAMPAIGN_TABLE, None)
            campaign_cols = _get_table_columns(conn, ENGAGEMENT_CAMPAIGN_TABLE)
            if "campaign_type" not in campaign_cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_CAMPAIGN_TABLE} "
                    "ADD COLUMN campaign_type VARCHAR(32) NOT NULL DEFAULT 'performance' "
                    "AFTER source_filename"
                )
                campaign_cols.add("campaign_type")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ENGAGEMENT_ROW_TABLE} (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    campaign_id VARCHAR(64) NOT NULL,
                    wa_id VARCHAR(64) NOT NULL,
                    display_name VARCHAR(255) NULL,
                    driver_type VARCHAR(64) NULL,
                    template_id VARCHAR(128) NULL,
                    template_group VARCHAR(128) NULL,
                    variables_json TEXT NULL,
                    row_json TEXT NULL,
                    baseline_json TEXT NULL,
                    send_status VARCHAR(32) NOT NULL,
                    send_error TEXT NULL,
                    message_id VARCHAR(128) NULL,
                    sent_at TIMESTAMP NULL,
                    followup_template_id VARCHAR(128) NULL,
                    followup_status VARCHAR(32) NULL,
                    followup_error TEXT NULL,
                    followup_message_id VARCHAR(128) NULL,
                    followup_sent_at TIMESTAMP NULL,
                    INDEX idx_campaign (campaign_id),
                    INDEX idx_wa (wa_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            if hasattr(_get_table_columns, "_cache"):
                _get_table_columns._cache.pop(ENGAGEMENT_ROW_TABLE, None)
            cols = _get_table_columns(conn, ENGAGEMENT_ROW_TABLE)
            if "template_group" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN template_group VARCHAR(128) NULL AFTER template_id"
                )
                cols.add("template_group")
            if "row_json" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN row_json TEXT NULL AFTER variables_json"
                )
                cols.add("row_json")
            if "baseline_json" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN baseline_json TEXT NULL AFTER row_json"
                )
                cols.add("baseline_json")
            if "followup_template_id" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN followup_template_id VARCHAR(128) NULL AFTER sent_at"
                )
                cols.add("followup_template_id")
            if "followup_status" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN followup_status VARCHAR(32) NULL AFTER followup_template_id"
                )
                cols.add("followup_status")
            if "followup_error" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN followup_error TEXT NULL AFTER followup_status"
                )
                cols.add("followup_error")
            if "followup_message_id" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN followup_message_id VARCHAR(128) NULL AFTER followup_error"
                )
                cols.add("followup_message_id")
            if "followup_sent_at" not in cols:
                cur.execute(
                    f"ALTER TABLE {ENGAGEMENT_ROW_TABLE} ADD COLUMN followup_sent_at TIMESTAMP NULL AFTER followup_message_id"
                )
                cols.add("followup_sent_at")
    except Exception as exc:
        log.debug("Could not ensure engagement tables: %s", exc)


def _normalize_csv_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")


ENGAGEMENT_CSV_FIELDS = {
    "display_name": ["display_name", "name", "driver_name", "display"],
    "wa_raw": ["whatsapp", "wa_id", "phone", "phone_number"],
    "status": ["status"],
    "model": ["model"],
    "car_reg": ["car_reg", "car_reg_number", "car_registration", "registration_number", "vehicle"],
    "contract_start_date": ["contract_start_date", "contract_start", "contract_date"],
    "collections_agent": ["collections_agent", "collections", "collections_owner"],
    "driver_type": ["driver_type", "driver_type_label"],
    "payer_type": ["payer_type", "payer_type_label"],
    "online_hours": ["online_hours"],
    "acceptance_rate": ["acceptance_rate", "acceptance"],
    "gross_earnings": ["gross_earnings", "gross_earnings_week", "gross"],
    "earnings_per_hour": ["earnings_per_hour", "earnings_per_hr", "eph"],
    "xero_balance": ["xero_balance", "balance"],
    "payments": ["payments", "payment", "7d_payments", "7D_payments", "payments_7d", "payments_last_7d"],
    "bolt_wallet_payouts": ["bolt_wallet_payouts", "bolt_wallet"],
    "yday_wallet_balance": ["yesterday_wallet_balance", "yday_wallet_balance"],
    "trip_count": ["trip_count", "trips"],
    "target_online_hours": ["target_online_hours", "target_hours", "goal_online_hours", "goal_hours"],
    "target_trip_count": ["target_trip_count", "target_trips", "goal_trip_count", "goal_trips"],
    "last_synced_at": ["last_synced_at", "last_synced"],
    "contact_ids": ["contact_ids", "contacts"],
}


def _extract_csv_value(row: Dict[str, Any], keys: List[str]) -> Optional[str]:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            return str(row.get(key)).strip()
    return None


def _parse_engagement_csv(file: UploadFile) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    try:
        raw = file.file.read()
    except Exception as exc:
        return [], f"Failed to read upload: {exc}"
    if not raw:
        return [], "The uploaded CSV is empty."
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = raw.decode("latin-1")
        except Exception:
            return [], "Could not decode CSV. Please save it as UTF-8."

    reader = csv.DictReader(io.StringIO(text))
    rows: List[Dict[str, Any]] = []
    for idx, raw_row in enumerate(reader, start=2):
        if not raw_row:
            continue
        normalized = {_normalize_csv_header(k): v for k, v in raw_row.items()}
        mapped: Dict[str, Any] = {"row_number": idx}
        for key, aliases in ENGAGEMENT_CSV_FIELDS.items():
            mapped[key] = _extract_csv_value(normalized, aliases)
        rows.append(mapped)
        if len(rows) >= ENGAGEMENT_MAX_ROWS:
            break
    if not rows:
        return [], "No rows found in the uploaded CSV."
    return rows, None


def _normalize_driver_type(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def _default_template_for_driver_type(driver_type: Optional[str]) -> str:
    normalized = _normalize_driver_type(driver_type)
    return ENGAGEMENT_DRIVER_TYPE_TEMPLATES.get(normalized, ENGAGEMENT_DEFAULT_TEMPLATE)


def _resolve_template_params(template: Dict[str, Any], row: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    params: List[str] = []
    missing: List[str] = []
    variables = template.get("variables") or []
    template_id = str(template.get("id") or "")
    money_keys = {
        "outstanding",
        "amount",
        "balance",
        "xero_balance",
        "payments",
        "gross_earnings",
        "earnings_per_hour",
        "bolt_wallet_payouts",
        "yday_wallet_balance",
    }
    use_targets = template_id == "performance_no_trips_yet"
    target_hours = None
    target_trips = None
    if use_targets:
        target_hours = row.get("target_online_hours")
        target_trips = row.get("target_trip_count")
        if _coerce_float(target_hours) is not None and _coerce_float(target_hours) <= 0:
            target_hours = None
        if _coerce_float(target_trips) is not None and _coerce_float(target_trips) <= 0:
            target_trips = None
        if target_hours in (None, "") or target_trips in (None, ""):
            targets = get_model_targets(row.get("model") or "")
            if target_hours in (None, ""):
                target_hours = targets.get("online_hours") or ENGAGEMENT_TARGET_ONLINE_HOURS_MIN
            if target_trips in (None, ""):
                target_trips = targets.get("trip_count") or ENGAGEMENT_TARGET_TRIPS
    for var in variables:
        key = str(var or "").strip().lower()
        sources = ENGAGEMENT_VARIABLE_SOURCES.get(key, [])
        value = None
        if use_targets and key == "online_hours":
            value = _coerce_goal_value(target_hours) or target_hours
        elif use_targets and key == "trip_count":
            value = _coerce_goal_value(target_trips) or target_trips
        else:
            for source in sources:
                candidate = row.get(source)
                if candidate not in (None, ""):
                    value = candidate
                    break
        if value is None and key in {"outstanding", "amount", "balance"}:
            value = row.get("xero_balance") or row.get("payments")
        if value is None and key == "reference":
            value = row.get("car_reg") or row.get("contact_ids")
        if value in (None, ""):
            missing.append(key)
            params.append("")
        else:
            if key in money_keys:
                coerced = _coerce_float(value)
                if coerced is not None:
                    value = fmt_rands(coerced)
            if key == "acceptance_rate":
                if isinstance(value, (int, float)):
                    value = f"{value}%"
                else:
                    raw_val = str(value)
                    if raw_val and "%" not in raw_val:
                        value = f"{raw_val}%"
            params.append(str(value))
    return params, missing


def _fetch_last_template_by_wa_ids(wa_ids: List[str]) -> Dict[str, str]:
    if not (mysql_available() and wa_ids):
        return {}
    _ensure_engagement_tables()
    try:
        conn = get_mysql()
    except Exception:
        return {}
    placeholders = ", ".join(["%s"] * len(wa_ids))
    sql = (
        f"SELECT wa_id, template_id, sent_at, id "
        f"FROM {ENGAGEMENT_ROW_TABLE} "
        f"WHERE wa_id IN ({placeholders}) AND send_status='sent' "
        f"ORDER BY sent_at DESC, id DESC"
    )
    last_by_wa: Dict[str, str] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(wa_ids))
            rows = cur.fetchall() or []
        for row in rows:
            wa_id = row.get("wa_id")
            template_id = row.get("template_id")
            if wa_id and template_id and wa_id not in last_by_wa:
                last_by_wa[wa_id] = str(template_id)
    except Exception as exc:
        log.debug("fetch last template by wa failed: %s", exc)
    return last_by_wa


def _resolve_template_variant_id(
    templates: Dict[str, Dict[str, Any]],
    template_group: str,
    wa_id: Optional[str],
    last_template_id: Optional[str],
) -> str:
    tmpl = templates.get(template_group) or {}
    variants = [v for v in (tmpl.get("variants") or []) if v]
    if not variants:
        return template_group
    if last_template_id in variants:
        idx = variants.index(last_template_id)
        return variants[(idx + 1) % len(variants)]
    if wa_id:
        try:
            stable = int(hashlib.sha1(str(wa_id).encode("utf-8")).hexdigest(), 16)
            return variants[stable % len(variants)]
        except Exception:
            pass
    return variants[0]


def _engagement_row_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for key in ENGAGEMENT_CSV_FIELDS.keys():
        payload[key] = row.get(key)
    payload["wa_id"] = row.get("wa_id") or payload.get("wa_raw")
    payload["display_name"] = row.get("display_name")
    payload["driver_type"] = row.get("driver_type")
    return payload


def _parse_template_map(form: Dict[str, Any], driver_types: List[str]) -> Dict[str, str]:
    template_map: Dict[str, str] = {}
    for driver_type in driver_types:
        slug = re.sub(r"[^a-z0-9]+", "_", driver_type.strip().lower()).strip("_")
        key = f"template__{slug}"
        if key in form and form.get(key):
            template_map[driver_type] = str(form.get(key))
    default_key = "template__default"
    if default_key in form and form.get(default_key):
        template_map["__default__"] = str(form.get(default_key))
    return template_map


def _create_engagement_campaign(
    *,
    campaign_id: str,
    admin_email: str,
    source_filename: str,
    template_map: Dict[str, Any],
    total_rows: int,
    campaign_type: str = ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE,
) -> None:
    _ensure_engagement_tables()
    if not mysql_available():
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {ENGAGEMENT_CAMPAIGN_TABLE}
                  (id, admin_email, source_filename, campaign_type, template_map_json, total_rows, sent_count, failed_count, skipped_count)
                VALUES (%s, %s, %s, %s, %s, %s, 0, 0, 0)
                """,
                (
                    campaign_id,
                    admin_email,
                    source_filename,
                    campaign_type,
                    json.dumps(template_map, ensure_ascii=False),
                    int(total_rows),
                ),
            )
    except Exception as exc:
        log.debug("create engagement campaign failed: %s", exc)


def _update_engagement_campaign_stats(
    campaign_id: str,
    *,
    sent_count: int,
    failed_count: int,
    skipped_count: int,
) -> None:
    if not mysql_available():
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ENGAGEMENT_CAMPAIGN_TABLE}
                SET sent_count=%s, failed_count=%s, skipped_count=%s
                WHERE id=%s
                """,
                (int(sent_count), int(failed_count), int(skipped_count), campaign_id),
            )
    except Exception as exc:
        log.debug("update engagement campaign stats failed: %s", exc)


def _insert_engagement_rows(campaign_id: str, rows: List[Dict[str, Any]]) -> None:
    if not mysql_available() or not rows:
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {ENGAGEMENT_ROW_TABLE}
                  (campaign_id, wa_id, display_name, driver_type, template_id, template_group, variables_json, row_json,
                   baseline_json, send_status, send_error, message_id, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        campaign_id,
                        row.get("wa_id") or "",
                        row.get("display_name"),
                        row.get("driver_type"),
                        row.get("template_id"),
                        row.get("template_group"),
                        row.get("variables_json"),
                        row.get("row_json"),
                        row.get("baseline_json"),
                        row.get("send_status"),
                        row.get("send_error"),
                        row.get("message_id"),
                        row.get("sent_at"),
                    )
                    for row in rows
                ],
            )
    except Exception as exc:
        log.debug("insert engagement rows failed: %s", exc)


def _fetch_engagement_campaigns(
    limit: int = 20,
    *,
    campaign_type: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not mysql_available():
        return []
    _ensure_engagement_tables()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            where_clause = ""
            params: List[Any] = []
            if campaign_type:
                if campaign_type == ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE:
                    where_clause = "WHERE (campaign_type = %s OR campaign_type IS NULL)"
                else:
                    where_clause = "WHERE campaign_type = %s"
                params.append(campaign_type)
            cur.execute(
                f"""
                SELECT id, admin_email, source_filename, campaign_type, total_rows, sent_count,
                       failed_count, skipped_count, created_at
                FROM {ENGAGEMENT_CAMPAIGN_TABLE}
                {where_clause}
                ORDER BY created_at DESC
                LIMIT %s
                """,
                tuple(params + [limit]),
            )
            rows = cur.fetchall() or []
        for row in rows:
            row["campaign_type"] = row.get("campaign_type") or ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE
        return rows
    except Exception as exc:
        log.debug("fetch engagement campaigns failed: %s", exc)
        return []


def _fetch_engagement_campaign(campaign_id: str) -> Optional[Dict[str, Any]]:
    if not mysql_available():
        return None
    _ensure_engagement_tables()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, admin_email, source_filename, campaign_type, template_map_json, total_rows,
                       sent_count, failed_count, skipped_count, created_at
                FROM {ENGAGEMENT_CAMPAIGN_TABLE}
                WHERE id=%s
                LIMIT 1
                """,
                (campaign_id,),
            )
            row = cur.fetchone()
        if not row:
            return None
        row["campaign_type"] = row.get("campaign_type") or ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE
        row["template_map"] = _safe_json_load(row.get("template_map_json"), default={})
        return row
    except Exception as exc:
        log.debug("fetch engagement campaign failed: %s", exc)
        return None


def _fetch_engagement_rows(campaign_id: str) -> List[Dict[str, Any]]:
    if not mysql_available():
        return []
    _ensure_engagement_tables()
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, wa_id, display_name, driver_type, template_id, template_group, variables_json, row_json,
                       send_status, send_error, message_id, sent_at, baseline_json,
                       followup_template_id, followup_status, followup_error, followup_message_id, followup_sent_at
                FROM {ENGAGEMENT_ROW_TABLE}
                WHERE campaign_id=%s
                ORDER BY id ASC
                """,
                (campaign_id,),
            )
            rows = cur.fetchall() or []
        for row in rows:
            row["variables"] = _safe_json_load(row.get("variables_json"), default=[])
            row["baseline"] = _safe_json_load(row.get("baseline_json"), default={})
            row["row_data"] = _safe_json_load(row.get("row_json"), default={})
        return rows
    except Exception as exc:
        log.debug("fetch engagement rows failed: %s", exc)
        return []

def _fetch_latest_kpi_metrics_by_wa(wa_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not (mysql_available() and wa_ids):
        return {}
    try:
        conn = get_mysql()
    except Exception:
        return {}
    try:
        kpi_table = f"{MYSQL_DB}.driver_kpi_summary"
        if not _table_exists(conn, kpi_table):
            return {}
        kpi_cols = _get_table_columns(conn, kpi_table)
        wa_col = _pick_col_exists(
            conn,
            kpi_table,
            ["phone", "wa_id", "whatsapp_number", "whatsapp", "phone_number", "contact_number", "driver_phone"],
        )
        date_col = _pick_col_exists(conn, kpi_table, ["report_date", "snapshot_date", "created_at", "updated_at"])
        if not wa_col or not date_col:
            return {}

        online_col = _pick_col_exists(conn, kpi_table, ["total_online_hours", "online_hours"])
        acceptance_col = _pick_col_exists(conn, kpi_table, ["acceptance_pct", "acceptance_rate"])
        eph_col = _pick_col_exists(conn, kpi_table, ["eph", "earnings_per_hour"])
        trip_cols = [
            c
            for c in [
                "total_finished_orders",
                "finished_trips",
                "trip_count",
                "total_trips_accepted",
                "total_trips_sent",
            ]
            if c in kpi_cols
        ]

        wa_map: Dict[str, str] = {}
        digits: List[str] = []
        for wa in wa_ids:
            normalized = _normalize_wa_id(wa) or str(wa)
            variants = _wa_number_variants(wa) or [normalized]
            for var in variants:
                v_digits = re.sub(r"\\D", "", str(var))
                if not v_digits:
                    continue
                if v_digits not in wa_map:
                    wa_map[v_digits] = normalized
                digits.append(v_digits)
        digits = sorted({d for d in digits if len(d) >= 7})
        if not digits:
            return {}

        results: Dict[str, Dict[str, Any]] = {}
        chunk_size = 200
        for i in range(0, len(digits), chunk_size):
            batch = digits[i : i + chunk_size]
            placeholders = ", ".join(["%s"] * len(batch))
            sanitized_s = _sanitize_phone_expr(f"s.{wa_col}")
            sanitized_t = _sanitize_phone_expr(f"t.{wa_col}")
            inner = (
                f"SELECT {sanitized_s} AS wa_key, MAX(s.{date_col}) AS max_dt "
                f"FROM {kpi_table} s "
                f"WHERE {sanitized_s} IN ({placeholders}) "
                f"GROUP BY {sanitized_s}"
            )
            select_cols = [
                f"t.{wa_col} AS wa_raw",
                f"{sanitized_t} AS wa_key",
                f"t.{date_col} AS report_date",
            ]
            if online_col:
                select_cols.append(f"t.{online_col} AS online_hours")
            if acceptance_col:
                select_cols.append(f"t.{acceptance_col} AS acceptance_rate")
            if eph_col:
                select_cols.append(f"t.{eph_col} AS earnings_per_hour")
            for col in trip_cols:
                select_cols.append(f"t.{col} AS {col}")
            sql = (
                f"SELECT {', '.join(select_cols)} "
                f"FROM {kpi_table} t "
                f"JOIN ({inner}) latest ON {sanitized_t} = latest.wa_key AND t.{date_col} = latest.max_dt"
            )
            with conn.cursor() as cur:
                cur.execute(sql, tuple(batch))
                rows = cur.fetchall() or []
            for row in rows:
                wa_key = re.sub(r"\\D", "", str(row.get("wa_key") or row.get("wa_raw") or ""))
                if not wa_key:
                    continue
                normalized = wa_map.get(wa_key) or _normalize_wa_id(wa_key) or wa_key
                report_raw = row.get("report_date")
                report_dt = _parse_log_timestamp(report_raw)
                report_label = None
                if report_dt:
                    report_label = report_dt.strftime("%Y-%m-%d")
                elif report_raw not in (None, ""):
                    report_label = str(report_raw)
                current = {
                    "online_hours": _parse_metric_value(row.get("online_hours")) if online_col else None,
                    "acceptance_rate": _parse_metric_percent(row.get("acceptance_rate")) if acceptance_col else None,
                    "earnings_per_hour": _parse_metric_value(row.get("earnings_per_hour")) if eph_col else None,
                    "trip_count": None,
                    "report_date": report_label,
                    "report_dt": report_dt,
                }
                for col in trip_cols:
                    val = _parse_metric_value(row.get(col))
                    if val is not None:
                        current["trip_count"] = val
                        break
                results[normalized] = current
        return results
    except Exception as exc:
        log.debug("fetch latest kpi metrics failed: %s", exc)
        return {}


def _build_engagement_kpi_report(
    rows: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]], Optional[datetime]]:
    sent_rows = [r for r in rows if r.get("send_status") == "sent" and r.get("wa_id")]
    wa_ids = [r.get("wa_id") for r in sent_rows if r.get("wa_id")]
    current_map = _fetch_latest_kpi_metrics_by_wa(wa_ids)

    summary: Dict[str, Dict[str, int]] = {
        "online_hours": {"improved": 0, "comparable": 0, "current_count": 0, "target_met": 0},
        "acceptance_rate": {"improved": 0, "comparable": 0, "current_count": 0, "target_met": 0},
        "earnings_per_hour": {"improved": 0, "comparable": 0, "current_count": 0, "target_met": 0},
        "trip_count": {"improved": 0, "comparable": 0, "current_count": 0, "target_met": 0},
    }

    latest_snapshot: Optional[datetime] = None
    rows_out: List[Dict[str, Any]] = []
    for row in rows:
        row_copy = dict(row)
        baseline = row.get("baseline") or {}
        current = current_map.get(row.get("wa_id") or "")
        compare, display = _build_engagement_kpi_compare(baseline, current)
        row_copy["kpi_compare"] = compare
        row_copy["kpi_display"] = display
        row_copy["kpi_current"] = current
        row_copy["kpi_available"] = any(
            entry.get("current") is not None for entry in compare.values()
        )
        if current and current.get("report_dt"):
            if latest_snapshot is None or current["report_dt"] > latest_snapshot:
                latest_snapshot = current["report_dt"]
        if row.get("send_status") == "sent":
            for key, info in compare.items():
                if info.get("current") is not None:
                    summary[key]["current_count"] += 1
                    if info.get("target_met") is True:
                        summary[key]["target_met"] += 1
                if info.get("baseline") is not None and info.get("current") is not None:
                    summary[key]["comparable"] += 1
                    if info.get("delta") is not None and info.get("delta") > 0:
                        summary[key]["improved"] += 1
        rows_out.append(row_copy)

    return rows_out, summary, latest_snapshot


def _collections_target_payment_7d() -> Optional[float]:
    raw = os.getenv("COLLECTIONS_TARGET_PAYMENTS_7D", "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except Exception:
        return None


def _collections_target_balance_max() -> float:
    raw = os.getenv("COLLECTIONS_TARGET_BALANCE_MAX", "0").strip()
    try:
        return float(raw)
    except Exception:
        return 0.0


def _build_collections_kpi_compare(
    baseline: Dict[str, Any],
    current: Optional[Dict[str, Any]],
) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    compare: Dict[str, Dict[str, Any]] = {}
    display: List[Dict[str, Any]] = []

    def add_metric(
        key: str,
        label: str,
        base_val: Optional[float],
        cur_val: Optional[float],
        *,
        money: bool = False,
        integer: bool = False,
        invert: bool = False,
        target_min: Optional[float] = None,
        target_max: Optional[float] = None,
        target: Optional[float] = None,
        target_label: str = "",
    ) -> None:
        delta = None
        if base_val is not None and cur_val is not None:
            delta = (base_val - cur_val) if invert else (cur_val - base_val)
        met_target = None
        if cur_val is not None:
            if target_min is not None or target_max is not None:
                met_target = True
                if target_min is not None and cur_val < target_min:
                    met_target = False
                if target_max is not None and cur_val > target_max:
                    met_target = False
            elif target is not None:
                met_target = cur_val >= target
        compare[key] = {
            "baseline": base_val,
            "current": cur_val,
            "delta": delta,
            "target_met": met_target,
            "target_label": target_label,
        }
        display.append(
            {
                "label": label,
                "baseline": _format_metric_value(base_val, money=money, integer=integer),
                "current": _format_metric_value(cur_val, money=money, integer=integer),
                "delta": _format_delta_value(delta, money=money, integer=integer),
                "delta_class": _delta_class(delta),
                "target_met": met_target,
                "target_label": target_label,
            }
        )

    base_balance = _parse_metric_value(baseline.get("balance")) if baseline else None
    cur_balance = _parse_metric_value(current.get("balance")) if current else None
    balance_target = _collections_target_balance_max()
    add_metric(
        "balance",
        "Balance",
        base_balance,
        cur_balance,
        money=True,
        invert=True,
        target_max=balance_target,
        target_label="Cleared" if balance_target <= 0 else f"≤{fmt_rands(balance_target)}",
    )

    base_payments = _parse_metric_value(baseline.get("payments_7d")) if baseline else None
    cur_payments = _parse_metric_value(current.get("payments_7d")) if current else None
    payments_target = _collections_target_payment_7d()
    add_metric(
        "payments_7d",
        "Payments (7d)",
        base_payments,
        cur_payments,
        money=True,
        target=payments_target,
        target_label=f"{fmt_rands(payments_target)}+" if payments_target is not None else "",
    )

    return compare, display


def _fetch_latest_collections_metrics_by_wa(wa_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not (mysql_available() and wa_ids):
        return {}
    try:
        conn = get_mysql()
    except Exception:
        return {}
    try:
        kpi_table = f"{MYSQL_DB}.driver_kpi_summary"
        if not _table_exists(conn, kpi_table):
            return {}
        kpi_cols = _get_table_columns(conn, kpi_table)
        wa_col = _pick_col_exists(
            conn,
            kpi_table,
            ["phone", "wa_id", "whatsapp_number", "whatsapp", "phone_number", "contact_number", "driver_phone"],
        )
        date_col = _pick_col_exists(conn, kpi_table, ["report_date", "snapshot_date", "created_at", "updated_at"])
        if not wa_col or not date_col:
            return {}

        balance_col = _pick_col_exists(conn, kpi_table, ["xero_balance", "balance", "outstanding"])
        payments_7d_col = _pick_col_exists(
            conn,
            kpi_table,
            ["7D_payments", "7d_payments", "payments_7d", "payments_last_7d"],
        )
        payments_total_col = _pick_col_exists(
            conn,
            kpi_table,
            ["total_payments", "payments_total", "payments"],
        )

        wa_map: Dict[str, str] = {}
        digits: List[str] = []
        for wa in wa_ids:
            normalized = _normalize_wa_id(wa) or str(wa)
            variants = _wa_number_variants(wa) or [normalized]
            for var in variants:
                v_digits = re.sub(r"\D", "", str(var))
                if not v_digits:
                    continue
                if v_digits not in wa_map:
                    wa_map[v_digits] = normalized
                digits.append(v_digits)
        digits = sorted({d for d in digits if len(d) >= 7})
        if not digits:
            return {}

        results: Dict[str, Dict[str, Any]] = {}
        chunk_size = 200
        for i in range(0, len(digits), chunk_size):
            batch = digits[i : i + chunk_size]
            placeholders = ", ".join(["%s"] * len(batch))
            sanitized_s = _sanitize_phone_expr(f"s.{wa_col}")
            sanitized_t = _sanitize_phone_expr(f"t.{wa_col}")
            inner = (
                f"SELECT {sanitized_s} AS wa_key, MAX(s.{date_col}) AS max_dt "
                f"FROM {kpi_table} s "
                f"WHERE {sanitized_s} IN ({placeholders}) "
                f"GROUP BY {sanitized_s}"
            )
            select_cols = [
                f"t.{wa_col} AS wa_raw",
                f"{sanitized_t} AS wa_key",
                f"t.{date_col} AS report_date",
            ]
            if balance_col:
                select_cols.append(f"t.{balance_col} AS balance")
            if payments_7d_col:
                select_cols.append(f"t.{payments_7d_col} AS payments_7d")
            elif payments_total_col:
                select_cols.append(f"t.{payments_total_col} AS payments_7d")
            sql = (
                f"SELECT {', '.join(select_cols)} "
                f"FROM {kpi_table} t "
                f"JOIN ({inner}) latest ON {sanitized_t} = latest.wa_key AND t.{date_col} = latest.max_dt"
            )
            with conn.cursor() as cur:
                cur.execute(sql, tuple(batch))
                rows = cur.fetchall() or []
            for row in rows:
                wa_key = re.sub(r"\D", "", str(row.get("wa_key") or row.get("wa_raw") or ""))
                if not wa_key:
                    continue
                normalized = wa_map.get(wa_key) or _normalize_wa_id(wa_key) or wa_key
                report_raw = row.get("report_date")
                report_dt = _parse_log_timestamp(report_raw)
                report_label = None
                if report_dt:
                    report_label = report_dt.strftime("%Y-%m-%d")
                elif report_raw not in (None, ""):
                    report_label = str(report_raw)
                current = {
                    "balance": _parse_metric_value(row.get("balance")) if balance_col else None,
                    "payments_7d": _parse_metric_value(row.get("payments_7d")),
                    "report_date": report_label,
                    "report_dt": report_dt,
                }
                results[normalized] = current
        return results
    except Exception as exc:
        log.debug("fetch latest collections metrics failed: %s", exc)
        return {}


def _build_collections_kpi_report(
    rows: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]], Optional[datetime]]:
    sent_rows = [r for r in rows if r.get("send_status") == "sent" and r.get("wa_id")]
    wa_ids = [r.get("wa_id") for r in sent_rows if r.get("wa_id")]
    current_map = _fetch_latest_collections_metrics_by_wa(wa_ids)

    summary: Dict[str, Dict[str, int]] = {
        "balance": {"improved": 0, "comparable": 0, "current_count": 0, "target_met": 0},
        "payments_7d": {"improved": 0, "comparable": 0, "current_count": 0, "target_met": 0},
    }

    latest_snapshot: Optional[datetime] = None
    rows_out: List[Dict[str, Any]] = []
    for row in rows:
        row_copy = dict(row)
        baseline = row.get("baseline") or {}
        current = current_map.get(row.get("wa_id") or "")
        compare, display = _build_collections_kpi_compare(baseline, current)
        row_copy["kpi_compare"] = compare
        row_copy["kpi_display"] = display
        row_copy["kpi_current"] = current
        row_copy["kpi_available"] = any(
            entry.get("current") is not None for entry in compare.values()
        )
        if current and current.get("report_dt"):
            if latest_snapshot is None or current["report_dt"] > latest_snapshot:
                latest_snapshot = current["report_dt"]
        if row.get("send_status") == "sent":
            for key, info in compare.items():
                if info.get("current") is not None:
                    summary[key]["current_count"] += 1
                    if info.get("target_met") is True:
                        summary[key]["target_met"] += 1
                if info.get("baseline") is not None and info.get("current") is not None:
                    summary[key]["comparable"] += 1
                    if info.get("delta") is not None and info.get("delta") > 0:
                        summary[key]["improved"] += 1
        rows_out.append(row_copy)

    return rows_out, summary, latest_snapshot

def _build_engagement_response_rows(
    rows: List[Dict[str, Any]],
    *,
    commitment_mode: str = "kpi",
) -> Tuple[List[Dict[str, Any]], int, int, int, int]:
    sent_rows = [r for r in rows if r.get("send_status") == "sent"]
    engagement_window_days = ENGAGEMENT_RESPONSE_WINDOW_DAYS
    response_start = None
    response_end = None
    for row in sent_rows:
        ts = _parse_log_timestamp(row.get("sent_at"))
        if not ts:
            continue
        if response_start is None or ts < response_start:
            response_start = ts
        end_candidate = ts + timedelta(days=engagement_window_days)
        if response_end is None or end_candidate > response_end:
            response_end = end_candidate

    inbound_logs: List[Dict[str, Any]] = []
    message_logs: List[Dict[str, Any]] = []
    if response_start and response_end and sent_rows:
        inbound_logs = _fetch_inbound_logs(
            [r.get("wa_id") for r in sent_rows if r.get("wa_id")],
            response_start,
            response_end,
        )
        message_logs = _fetch_message_logs(
            [r.get("wa_id") for r in sent_rows if r.get("wa_id")],
            response_start,
            response_end,
        )

    inbound_by_wa: Dict[str, List[Dict[str, Any]]] = {}
    for entry in inbound_logs:
        wa_id = entry.get("wa_id")
        if not wa_id:
            continue
        inbound_by_wa.setdefault(wa_id, []).append(entry)

    messages_by_wa: Dict[str, List[Dict[str, Any]]] = {}
    for entry in message_logs:
        wa_id = entry.get("wa_id")
        if not wa_id:
            continue
        messages_by_wa.setdefault(wa_id, []).append(entry)
    for wa_id, entries in messages_by_wa.items():
        entries.sort(key=lambda r: r.get("ts") or datetime.max)
        messages_by_wa[wa_id] = entries

    responded = 0
    committed = 0
    ptp_count = 0
    rows_out: List[Dict[str, Any]] = []
    for row in rows:
        row_copy = dict(row)
        row_copy["responded"] = False
        row_copy["committed"] = False
        row_copy["ptp"] = False
        row_copy["response_at"] = None
        row_copy["response_preview"] = None
        response_ts: Optional[datetime] = None
        if row.get("send_status") == "sent":
            sent_at = _parse_log_timestamp(row.get("sent_at"))
            window_end = sent_at + timedelta(days=engagement_window_days) if sent_at else None
            responses = inbound_by_wa.get(row.get("wa_id") or "", [])
            responses = sorted(responses, key=lambda r: r.get("ts") or datetime.max)
            for resp in responses:
                if sent_at and window_end and (resp["ts"] < sent_at or resp["ts"] > window_end):
                    continue
                if not row_copy["responded"]:
                    row_copy["responded"] = True
                    response_ts = resp.get("ts")
                    row_copy["response_at"] = response_ts.strftime("%Y-%m-%d %H:%M:%S") if response_ts else None
                    row_copy["response_preview"] = (resp.get("body") or "")[:120]
                if _is_kpi_commitment_message(resp.get("body") or "", resp.get("intent")):
                    row_copy["committed"] = True
                if _is_payment_commitment_message(resp.get("body") or "", resp.get("intent")):
                    row_copy["ptp"] = True
                if row_copy["committed"] and row_copy["ptp"]:
                    break
        if response_ts:
            thread_entries: List[Dict[str, Any]] = []
            history = messages_by_wa.get(row.get("wa_id") or "", [])
            for entry in history:
                ts = entry.get("ts")
                if not ts or ts < response_ts:
                    continue
                body = (entry.get("body") or "").strip()
                if len(body) > 160:
                    body = body[:157] + "..."
                thread_entries.append(
                    {
                        "direction": (entry.get("direction") or "").strip().lower(),
                        "body": body,
                        "ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )
                if len(thread_entries) >= 6:
                    break
            row_copy["response_thread"] = thread_entries
        if commitment_mode == "payment":
            row_copy["committed"] = row_copy["ptp"]
        if row_copy["responded"]:
            responded += 1
        if row_copy["committed"]:
            committed += 1
        if row_copy["ptp"]:
            ptp_count += 1
        rows_out.append(row_copy)

    return rows_out, responded, committed, ptp_count, engagement_window_days


def _fetch_driver_kpi_trend(
    wa_id: str,
    driver_ref: Dict[str, Any],
    *,
    days: int = 7,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not mysql_available():
        return [], "database connection not configured"
    try:
        conn = get_mysql()
    except Exception:
        return [], "database connection not configured"

    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return [], "driver_kpi_summary not found"

    kpi_cols = _get_table_columns(conn, table)
    date_col = _pick_col_exists(conn, table, ["report_date", "snapshot_date", "created_at", "updated_at"])
    if not date_col:
        return [], "driver_kpi_summary missing report date"

    online_col = _pick_col_exists(conn, table, ["total_online_hours", "online_hours"])
    acceptance_col = _pick_col_exists(conn, table, ["acceptance_pct", "acceptance_rate"])
    eph_col = _pick_col_exists(conn, table, ["eph", "earnings_per_hour"])
    gross_col = _pick_col_exists(conn, table, ["total_gmv", "gross_earnings"])
    trip_col = next(
        (
            c
            for c in [
                "total_finished_orders",
                "finished_trips",
                "trip_count",
                "total_trips_accepted",
                "total_trips_sent",
            ]
            if c in kpi_cols
        ),
        None,
    )
    kpi_driver_id_col = "driver_id" if "driver_id" in kpi_cols else None

    driver_id = driver_ref.get("driver_id") or driver_ref.get("bolt_driver_id") or driver_ref.get("id")
    try:
        driver_id = int(driver_id) if driver_id is not None else None
    except Exception:
        driver_id = None

    params: List[Any] = []
    where_clause = ""
    if driver_id and _pick_col_exists(conn, table, ["driver_id", "id", "driver_uuid", "uuid"]):
        where_clause = "driver_id=%s" if "driver_id" in kpi_cols else "id=%s"
        params.append(driver_id)
    else:
        phone_col = _pick_col_exists(
            conn,
            table,
            ["phone", "wa_id", "whatsapp", "whatsapp_number", "wa_number", "phone_number", "contact_number"],
        )
        if not phone_col:
            return [], "driver_kpi_summary missing phone column"
        variants = _wa_number_variants(wa_id)
        if not variants:
            clean = re.sub(r"\D", "", wa_id or "")
            variants = [clean] if clean else []
        if not variants:
            return [], "no driver identifiers available"
        sanitized_expr = _sanitize_phone_expr(phone_col)
        placeholders = ", ".join(["%s"] * len(variants))
        where_clause = f"{sanitized_expr} IN ({placeholders})"
        params.extend(variants)

    select_cols = [f"{date_col} AS report_date"]
    if online_col:
        select_cols.append(f"{online_col} AS online_hours")
    if acceptance_col:
        select_cols.append(f"{acceptance_col} AS acceptance_rate")
    if eph_col:
        select_cols.append(f"{eph_col} AS earnings_per_hour")
    if gross_col:
        select_cols.append(f"{gross_col} AS gross_earnings")
    if trip_col:
        select_cols.append(f"{trip_col} AS trip_count")
    if kpi_driver_id_col:
        select_cols.append(f"{kpi_driver_id_col} AS kpi_driver_id")

    sql = (
        f"SELECT {', '.join(select_cols)} "
        f"FROM {table} "
        f"WHERE {where_clause} "
        f"ORDER BY {date_col} DESC "
        f"LIMIT %s"
    )
    params.append(max(1, int(days)))

    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except Exception as exc:
        log.debug("fetch driver kpi trend failed: %s", exc)
        return [], "could not fetch KPI trend"

    if driver_id is None and rows:
        for row in rows:
            try:
                candidate = row.get("kpi_driver_id")
            except Exception:
                candidate = None
            if candidate is None:
                continue
            try:
                driver_id = int(candidate)
                break
            except Exception:
                continue

    if driver_id is None:
        bolt_drivers_table = f"{MYSQL_DB}.bolt_drivers"
        if _table_exists(conn, bolt_drivers_table):
            id_col = _pick_col_exists(conn, bolt_drivers_table, ["id", "driver_id"])
            phone_col = _pick_col_exists(conn, bolt_drivers_table, SIMPLYFLEET_WHATSAPP_COLUMNS + ["mobile", "msisdn"])
            if id_col and phone_col:
                variants = _wa_number_variants(wa_id)
                if not variants:
                    clean = re.sub(r"\D", "", wa_id or "")
                    variants = [clean] if clean else []
                if variants:
                    sanitized_expr = _sanitize_phone_expr(phone_col)
                    placeholders = ", ".join(["%s"] * len(variants))
                    sql = (
                        f"SELECT {id_col} AS driver_id FROM {bolt_drivers_table} "
                        f"WHERE {sanitized_expr} IN ({placeholders}) "
                        f"ORDER BY {id_col} DESC LIMIT 1"
                    )
                    try:
                        with conn.cursor() as cur:
                            cur.execute(sql, tuple(variants))
                            row = cur.fetchone() or {}
                        if row.get("driver_id") is not None:
                            try:
                                driver_id = int(row.get("driver_id"))
                            except Exception:
                                driver_id = None
                    except Exception as exc:
                        log.debug("bolt driver_id lookup failed: %s", exc)

    rolling_trip_counts: Dict[date, int] = {}
    trip_dates: List[date] = []
    for row in rows:
        report_dt = _parse_log_timestamp(row.get("report_date"))
        if report_dt:
            trip_dates.append(report_dt.date())

    if trip_dates:
        bolt_table = f"{MYSQL_DB}.bolt_orders_new"
        if _table_exists(conn, bolt_table):
            bolt_cols = _get_table_columns(conn, bolt_table)
            id_column = None
            id_values: List[Any] = []
            if driver_id is not None and "driver_id" in bolt_cols:
                id_column = "driver_id"
                id_values = [driver_id]
            else:
                for key in ("driver_uuid", "uuid", "driver_contact_id"):
                    if key in bolt_cols and driver_ref.get(key):
                        id_column = key
                        id_values = [driver_ref.get(key)]
                        break
                if not id_column:
                    contact_ids = [str(c).strip() for c in (driver_ref.get("xero_contact_ids") or []) if c]
                    for key in ("contact_id", "xero_contact_id", "account_id", "driver_contact_id"):
                        if key in bolt_cols and contact_ids:
                            id_column = key
                            id_values = contact_ids
                            break
                if not id_column and "wa_id" in bolt_cols:
                    variants = _wa_number_variants(wa_id)
                    if not variants:
                        clean = re.sub(r"\D", "", wa_id or "")
                        variants = [clean] if clean else []
                    if variants:
                        id_column = "wa_id"
                        id_values = variants

            if "driver_assigned_time" in bolt_cols:
                date_expr = "driver_assigned_time"
            elif "driver_assigned_timestamp" in bolt_cols:
                date_expr = "driver_assigned_timestamp"
            else:
                date_expr = _pick_col_exists(conn, bolt_table, BOLT_DATE_COLUMNS)

            status_col = _pick_col_exists(conn, bolt_table, BOLT_STATUS_COLUMNS)
            if id_column and id_values and date_expr and status_col:
                start_date = min(trip_dates) - timedelta(days=7)
                end_date = max(trip_dates)
                start_str = start_date.strftime("%Y-%m-%d 00:00:00")
                end_str = end_date.strftime("%Y-%m-%d 00:00:00")
                placeholders = ", ".join(["%s"] * len(id_values))
                where_clauses = [
                    f"{id_column} IN ({placeholders})",
                    f"{date_expr} >= %s",
                    f"{date_expr} < %s",
                ]
                trip_params = list(id_values) + [start_str, end_str]
                if "company_id" in bolt_cols:
                    where_clauses.append("(company_id IS NULL OR company_id <> 96165)")
                where_clauses.append(f"LOWER({status_col}) = 'finished'")
                sql = (
                    f"SELECT DATE({date_expr}) AS trip_date, COUNT(*) AS finished_trips "
                    f"FROM {bolt_table} "
                    f"WHERE {' AND '.join(where_clauses)} "
                    f"GROUP BY DATE({date_expr})"
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql, tuple(trip_params))
                        trip_rows = cur.fetchall() or []
                except Exception as exc:
                    log.debug("fetch driver trip counts failed: %s", exc)
                    trip_rows = []

                daily_counts: Dict[date, int] = {}
                for trip_row in trip_rows:
                    trip_date = trip_row.get("trip_date")
                    trip_dt = None
                    if isinstance(trip_date, datetime):
                        trip_dt = trip_date.date()
                    elif isinstance(trip_date, date):
                        trip_dt = trip_date
                    elif isinstance(trip_date, str):
                        try:
                            trip_dt = datetime.strptime(trip_date[:10], "%Y-%m-%d").date()
                        except Exception:
                            trip_dt = None
                    if trip_dt:
                        daily_counts[trip_dt] = int(trip_row.get("finished_trips") or 0)

                for report_date in trip_dates:
                    window_start = report_date - timedelta(days=7)
                    rolling_trip_counts[report_date] = sum(
                        cnt for d, cnt in daily_counts.items() if window_start <= d < report_date
                    )

    formatted: List[Dict[str, Any]] = []
    for row in rows:
        report_dt = _parse_log_timestamp(row.get("report_date"))
        report_label = report_dt.strftime("%Y-%m-%d") if report_dt else str(row.get("report_date") or "")
        online_val = _parse_metric_value(row.get("online_hours"))
        acceptance_val = _parse_metric_percent(row.get("acceptance_rate"))
        eph_val = _parse_metric_value(row.get("earnings_per_hour"))
        gross_val = _parse_metric_value(row.get("gross_earnings"))
        trip_val = None
        if report_dt:
            trip_val = rolling_trip_counts.get(report_dt.date())
        if trip_val is None:
            trip_val = _parse_metric_value(row.get("trip_count"))
        formatted.append(
            {
                "report_date": report_label,
                "online_hours": _format_metric_value(online_val, decimals=1),
                "acceptance_rate": _format_metric_value(acceptance_val, decimals=1, suffix="%"),
                "earnings_per_hour": fmt_rands(eph_val) if eph_val is not None else "-",
                "gross_earnings": fmt_rands(gross_val) if gross_val is not None else "-",
                "trip_count": _format_metric_value(trip_val, integer=True),
            }
        )
    formatted.reverse()
    return formatted, None


def _snapshot_date(value: Any) -> Optional[date]:
    ts = _parse_log_timestamp(value)
    return ts.date() if ts else None


def _fetch_kpi_snapshots_by_wa_ids(
    wa_ids: List[str],
    *,
    start_date: date,
    end_date: date,
) -> Dict[str, List[Dict[str, Any]]]:
    if not (mysql_available() and wa_ids):
        return {}
    try:
        conn = get_mysql()
    except Exception:
        return {}

    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return {}

    kpi_cols = _get_table_columns(conn, table)
    phone_col = _pick_col_exists(
        conn,
        table,
        ["phone", "wa_id", "whatsapp", "whatsapp_number", "wa_number", "phone_number", "contact_number"],
    )
    date_col = _pick_col_exists(conn, table, ["report_date", "snapshot_date", "created_at", "updated_at"])
    if not phone_col or not date_col:
        return {}

    online_col = _pick_col_exists(conn, table, ["total_online_hours", "online_hours"])
    acceptance_col = _pick_col_exists(conn, table, ["acceptance_pct", "acceptance_rate"])
    eph_col = _pick_col_exists(conn, table, ["eph", "earnings_per_hour"])
    gross_col = _pick_col_exists(conn, table, ["total_gmv", "gross_earnings"])
    trip_col = next(
        (
            c
            for c in [
                "total_finished_orders",
                "finished_trips",
                "trip_count",
                "total_trips_accepted",
                "total_trips_sent",
            ]
            if c in kpi_cols
        ),
        None,
    )

    wa_map: Dict[str, str] = {}
    digits: List[str] = []
    for wa in wa_ids:
        normalized = _normalize_wa_id(wa) or str(wa)
        variants = _wa_number_variants(wa) or [normalized]
        for var in variants:
            v_digits = re.sub(r"\D", "", str(var))
            if not v_digits:
                continue
            if v_digits not in wa_map:
                wa_map[v_digits] = normalized
            digits.append(v_digits)
    digits = sorted({d for d in digits if len(d) >= 7})
    if not digits:
        return {}

    placeholders = ", ".join(["%s"] * len(digits))
    sanitized_expr = _sanitize_phone_expr(phone_col)
    select_cols = [
        f"{sanitized_expr} AS wa_key",
        f"{date_col} AS report_date",
    ]
    if online_col:
        select_cols.append(f"{online_col} AS online_hours")
    if acceptance_col:
        select_cols.append(f"{acceptance_col} AS acceptance_rate")
    if eph_col:
        select_cols.append(f"{eph_col} AS earnings_per_hour")
    if gross_col:
        select_cols.append(f"{gross_col} AS gross_earnings")
    if trip_col:
        select_cols.append(f"{trip_col} AS trip_count")

    sql = (
        f"SELECT {', '.join(select_cols)} "
        f"FROM {table} "
        f"WHERE {sanitized_expr} IN ({placeholders}) "
        f"AND DATE({date_col}) BETWEEN %s AND %s "
        f"ORDER BY {date_col} ASC"
    )
    params: List[Any] = list(digits) + [start_date.isoformat(), end_date.isoformat()]
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except Exception as exc:
        log.debug("fetch engagement kpi snapshots failed: %s", exc)
        return {}

    snapshot_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        wa_key = re.sub(r"\D", "", str(row.get("wa_key") or ""))
        if not wa_key:
            continue
        normalized = wa_map.get(wa_key) or _normalize_wa_id(wa_key) or wa_key
        report_dt = _snapshot_date(row.get("report_date"))
        snapshot = {
            "report_date": report_dt,
            "report_label": report_dt.strftime("%Y-%m-%d") if report_dt else "",
            "online_hours": _parse_metric_value(row.get("online_hours")) if online_col else None,
            "acceptance_rate": _parse_metric_percent(row.get("acceptance_rate")) if acceptance_col else None,
            "earnings_per_hour": _parse_metric_value(row.get("earnings_per_hour")) if eph_col else None,
            "gross_earnings": _parse_metric_value(row.get("gross_earnings")) if gross_col else None,
            "trip_count": _parse_metric_value(row.get("trip_count")) if trip_col else None,
        }
        snapshot_map.setdefault(normalized, []).append(snapshot)

    return snapshot_map


def _build_uplift_metric(
    pre: Optional[float],
    post: Optional[float],
    *,
    decimals: int = 1,
    suffix: str = "",
    money: bool = False,
    integer: bool = False,
) -> Dict[str, Any]:
    delta = None
    if pre is not None and post is not None:
        delta = post - pre
    return {
        "pre": _format_metric_value(pre, decimals=decimals, suffix=suffix, money=money, integer=integer),
        "post": _format_metric_value(post, decimals=decimals, suffix=suffix, money=money, integer=integer),
        "delta": _format_delta_value(delta, decimals=decimals, suffix=suffix, money=money, integer=integer),
        "delta_class": _delta_class(delta),
        "delta_raw": delta,
    }


def _build_engagement_uplift(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, int]]]:
    sent_rows = [r for r in rows if r.get("send_status") == "sent" and r.get("wa_id")]
    send_dates = []
    for row in sent_rows:
        ts = _parse_log_timestamp(row.get("sent_at"))
        if ts:
            send_dates.append(ts.date())
    if not send_dates:
        return rows, {
            "online_hours": {"improved": 0, "comparable": 0},
            "acceptance_rate": {"improved": 0, "comparable": 0},
            "earnings_per_hour": {"improved": 0, "comparable": 0},
            "trip_count": {"improved": 0, "comparable": 0},
        }

    min_date = min(send_dates)
    max_date = max(send_dates)
    lookback = int(os.getenv("ENGAGEMENT_UPLIFT_LOOKBACK_DAYS", "14"))
    lookahead = int(os.getenv("ENGAGEMENT_UPLIFT_LOOKAHEAD_DAYS", "14"))
    start_date = min_date - timedelta(days=lookback)
    end_date = max_date + timedelta(days=lookahead)

    snapshot_map = _fetch_kpi_snapshots_by_wa_ids(
        [r.get("wa_id") for r in sent_rows if r.get("wa_id")],
        start_date=start_date,
        end_date=end_date,
    )

    summary: Dict[str, Dict[str, int]] = {
        "online_hours": {"improved": 0, "comparable": 0},
        "acceptance_rate": {"improved": 0, "comparable": 0},
        "earnings_per_hour": {"improved": 0, "comparable": 0},
        "trip_count": {"improved": 0, "comparable": 0},
    }

    rows_out: List[Dict[str, Any]] = []
    for row in rows:
        row_copy = dict(row)
        row_copy["uplift"] = None
        row_copy["uplift_pre_date"] = None
        row_copy["uplift_post_date"] = None

        wa_id = row.get("wa_id")
        sent_at = _parse_log_timestamp(row.get("sent_at"))
        if not (wa_id and sent_at):
            rows_out.append(row_copy)
            continue

        snapshots = snapshot_map.get(wa_id) or []
        send_date = sent_at.date()
        pre = None
        post = None
        for snap in snapshots:
            snap_date = snap.get("report_date")
            if not snap_date:
                continue
            if snap_date <= send_date:
                if pre is None or snap_date > pre.get("report_date"):
                    pre = snap
            elif snap_date > send_date:
                if post is None or snap_date > post.get("report_date"):
                    post = snap

        if pre:
            row_copy["uplift_pre_date"] = pre.get("report_label")
        if post:
            row_copy["uplift_post_date"] = post.get("report_label")

        uplift = {
            "online_hours": _build_uplift_metric(
                pre.get("online_hours") if pre else None,
                post.get("online_hours") if post else None,
                decimals=1,
            ),
            "acceptance_rate": _build_uplift_metric(
                pre.get("acceptance_rate") if pre else None,
                post.get("acceptance_rate") if post else None,
                decimals=1,
                suffix="%",
            ),
            "earnings_per_hour": _build_uplift_metric(
                pre.get("earnings_per_hour") if pre else None,
                post.get("earnings_per_hour") if post else None,
                money=True,
            ),
            "trip_count": _build_uplift_metric(
                pre.get("trip_count") if pre else None,
                post.get("trip_count") if post else None,
                integer=True,
            ),
        }
        row_copy["uplift"] = uplift

        for key, info in uplift.items():
            if info.get("delta_raw") is not None:
                summary[key]["comparable"] += 1
                if info.get("delta_raw") > 0:
                    summary[key]["improved"] += 1

        rows_out.append(row_copy)

    return rows_out, summary


def _parse_log_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
        else:
            raw = str(value).strip()
            if not raw:
                return None
            if raw.isdigit():
                ts = float(raw)
            else:
                try:
                    return datetime.fromisoformat(raw)
                except Exception:
                    try:
                        return datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        return None
        if ts > 9999999999:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts)
    except Exception:
        return None


def _fetch_inbound_logs(wa_ids: List[str], start_dt: datetime, end_dt: datetime) -> List[Dict[str, Any]]:
    if not (mysql_available() and wa_ids):
        return []
    try:
        conn = get_mysql()
        table = _detect_logs_table(conn)
        available = _get_table_columns(conn, table)
        text_col = _pick_log_column(available, ["message_text", "message", "text"])
        dir_col = _pick_log_column(available, ["message_direction", "direction"])
        ts_col = _pick_log_column(available, ["created_at", "logged_at", "timestamp"])
        intent_col = _pick_log_column(available, ["intent", "intent_label"])
        if not (text_col and dir_col and ts_col):
            return []
        chunk_size = 200
        results: List[Dict[str, Any]] = []
        for i in range(0, len(wa_ids), chunk_size):
            batch = wa_ids[i : i + chunk_size]
            placeholders = ", ".join(["%s"] * len(batch))
            sql = (
                f"SELECT wa_id, {text_col} AS body, {dir_col} AS direction, {ts_col} AS ts"
            )
            if intent_col:
                sql += f", {intent_col} AS intent"
            else:
                sql += ", NULL AS intent"
            sql += f" FROM {table} WHERE wa_id IN ({placeholders}) AND {dir_col}='INBOUND'"
            with conn.cursor() as cur:
                cur.execute(sql, tuple(batch))
                rows = cur.fetchall() or []
            for row in rows:
                ts = _parse_log_timestamp(row.get("ts"))
                if not ts:
                    continue
                if ts < start_dt or ts > end_dt:
                    continue
                results.append(
                    {
                        "wa_id": row.get("wa_id"),
                        "body": row.get("body") or "",
                        "intent": row.get("intent"),
                        "ts": ts,
                    }
                )
        return results
    except Exception as exc:
        log.debug("fetch inbound logs failed: %s", exc)
        return []

def _fetch_message_logs(
    wa_ids: List[str],
    start_dt: datetime,
    end_dt: datetime,
) -> List[Dict[str, Any]]:
    if not (mysql_available() and wa_ids):
        return []
    try:
        conn = get_mysql()
    except Exception:
        return []
    try:
        table = _detect_logs_table(conn)
        available = _get_table_columns(conn, table)
        text_col = _pick_log_column(available, ["message_text", "message", "text"])
        dir_col = _pick_log_column(available, ["message_direction", "direction"])
        ts_col = _pick_log_column(available, ["created_at", "logged_at", "timestamp"])
        if not (text_col and dir_col and ts_col):
            return []
        chunk_size = 200
        results: List[Dict[str, Any]] = []
        for i in range(0, len(wa_ids), chunk_size):
            batch = wa_ids[i : i + chunk_size]
            placeholders = ", ".join(["%s"] * len(batch))
            sql = (
                f"SELECT wa_id, {text_col} AS body, {dir_col} AS direction, {ts_col} AS ts "
                f"FROM {table} WHERE wa_id IN ({placeholders})"
            )
            sql += f" AND UPPER({dir_col}) IN ('INBOUND','OUTBOUND')"
            with conn.cursor() as cur:
                cur.execute(sql, tuple(batch))
                rows = cur.fetchall() or []
            for row in rows:
                ts = _parse_log_timestamp(row.get("ts"))
                if not ts:
                    continue
                if ts < start_dt or ts > end_dt:
                    continue
                results.append(
                    {
                        "wa_id": row.get("wa_id"),
                        "body": row.get("body") or "",
                        "direction": (row.get("direction") or "").strip(),
                        "ts": ts,
                    }
                )
        return results
    except Exception as exc:
        log.debug("fetch message logs failed: %s", exc)
        return []


def _fetch_status_map_for_message_ids(message_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    if not (mysql_available() and message_ids):
        return {}
    try:
        conn = get_mysql()
    except Exception:
        return {}
    try:
        table = _detect_logs_table(conn)
        available = _get_table_columns(conn, table)
        msg_id_col = _pick_log_column(available, SYNONYMS["wa_message_id"] + ["message_id"])
        status_col = _pick_log_column(available, ["status", "send_status"])
        dir_col = _pick_log_column(available, ["message_direction", "direction"])
        ts_col = _pick_log_column(available, ["created_at", "logged_at", "timestamp"])
        if not (msg_id_col and status_col):
            return {}
        clean_ids = [str(mid) for mid in message_ids if mid]
        if not clean_ids:
            return {}
        status_map: Dict[str, Dict[str, Any]] = {}
        chunk_size = 200
        for i in range(0, len(clean_ids), chunk_size):
            batch = clean_ids[i : i + chunk_size]
            placeholders = ", ".join(["%s"] * len(batch))
            sql = f"SELECT {msg_id_col} AS message_id, {status_col} AS status"
            if ts_col:
                sql += f", {ts_col} AS logged_at"
            sql += f" FROM {table} WHERE {msg_id_col} IN ({placeholders}) AND {status_col} IS NOT NULL"
            if dir_col:
                sql += f" AND UPPER({dir_col})='STATUS'"
            if ts_col:
                sql += f" ORDER BY {ts_col} DESC"
            with conn.cursor() as cur:
                cur.execute(sql, tuple(batch))
                rows = cur.fetchall() or []
            for row in rows:
                message_id = row.get("message_id")
                status = row.get("status")
                if not message_id or not status:
                    continue
                key = str(message_id)
                status_value = str(status).strip().lower()
                entry = status_map.setdefault(key, {"badges": [], "latest": None})
                if entry["latest"] is None:
                    entry["latest"] = status_value
                if status_value and status_value not in entry["badges"]:
                    entry["badges"].append(status_value)
        return status_map
    except Exception as exc:
        log.debug("fetch status map failed: %s", exc)
        return {}


def _count_message_reads(message_ids: List[str]) -> int:
    status_map = _fetch_status_map_for_message_ids(message_ids)
    return sum(1 for entry in status_map.values() if entry.get("latest") == "read")


def _is_payment_commitment_message(text: str, intent: Optional[str]) -> bool:
    if intent and intent == CASH_BALANCE_UPDATE_INTENT:
        return True
    lowered = (text or "").lower()
    if any(keyword in lowered for keyword in CASH_BALANCE_UPDATE_KEYWORDS):
        return True
    if re.search(r"\\b(pay|paid|payment|settle|settled|will pay|will settle)\\b", lowered):
        return True
    return False

def _is_kpi_commitment_message(text: str, intent: Optional[str]) -> bool:
    if not text:
        return False
    if _is_payment_commitment_message(text, intent):
        return False
    if intent == "target_update":
        return True
    lowered = text.lower().strip()
    if "commit" in lowered:
        return True
    if any(phrase in lowered for phrase in KPI_COMMITMENT_PHRASES):
        return True
    tokens = set(re.findall(r"[a-z']+", lowered))
    if tokens and tokens <= YES_TOKENS:
        return True
    if (tokens & YES_TOKENS) and (tokens & {"will", "ill", "can", "do", "improve", "commit"}):
        return True
    if (tokens & KPI_COMMITMENT_VERBS) and (tokens & KPI_COMMITMENT_METRICS):
        return True
    if _extract_hours_count(lowered) is not None:
        return True
    if _extract_trip_count(lowered) is not None:
        return True
    return False


def admin_can_manage_users(user: Optional[Dict[str, Any]]) -> bool:
    if not user:
        return False
    return bool(user.get("can_manage_users", True))

# Driver portal session helpers
def _encode_driver_session(payload: Dict[str, Any]) -> str:
    return _driver_serializer.dumps(payload)

def _decode_driver_session(token: str) -> Optional[Dict[str, Any]]:
    try:
        return _driver_serializer.loads(token, max_age=DRIVER_SESSION_MAX_AGE)
    except SignatureExpired:
        return None
    except BadSignature:
        return None

def _set_driver_session_cookie(response, payload: Dict[str, Any]) -> None:
    token = _encode_driver_session(payload)
    response.set_cookie(
        DRIVER_COOKIE_NAME,
        token,
        max_age=DRIVER_SESSION_MAX_AGE,
        expires=DRIVER_SESSION_MAX_AGE,
        httponly=True,
        secure=DRIVER_COOKIE_SECURE,
        samesite=DRIVER_COOKIE_SAMESITE,
    )

def _clear_driver_session_cookie(response) -> None:
    response.delete_cookie(DRIVER_COOKIE_NAME)

def get_authenticated_driver(request: Request) -> Optional[Dict[str, Any]]:
    token = request.cookies.get(DRIVER_COOKIE_NAME)
    if not token:
        return None
    data = _decode_driver_session(token)
    if not data or not (data.get("wa_id") or data.get("personal_code")):
        return None
    return data

def _logo_data_url() -> Optional[str]:
    """Return a data URL for the logo if no explicit URL is configured and a local file exists."""
    if MNC_LOGO_URL:
        return MNC_LOGO_URL
    if MNC_LOGO_PATH and os.path.exists(MNC_LOGO_PATH):
        try:
            import base64
            data = Path(MNC_LOGO_PATH).read_bytes()
            ext = Path(MNC_LOGO_PATH).suffix.lower()
            mime = "image/png"
            if ext == ".svg":
                mime = "image/svg+xml"
            elif ext in {".jpg", ".jpeg"}:
                mime = "image/jpeg"
            encoded = base64.b64encode(data).decode("ascii")
            return f"data:{mime};base64,{encoded}"
        except Exception:
            return None
    return DEFAULT_LOGO_DATA_URL

def _decode_data_url_to_bytes(data_url: str) -> Optional[tuple[str, bytes]]:
    try:
        if not data_url.startswith("data:"):
            return None
        header, b64data = data_url.split(",", 1)
        mime = header.split(";", 1)[0].split(":", 1)[1]
        import base64
        return mime, base64.b64decode(b64data)
    except Exception:
        return None


def fetch_driver_issue_tickets(limit: int = 50, status_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    if not mysql_available():
        return []
    limit = max(1, min(limit, 200))
    where_clause = ""
    params: List[Any] = []
    if status_filter:
        where_clause = "WHERE status=%s"
        params.append(status_filter)
    sql = f"""
        SELECT id, wa_id, issue_type, status, initial_message, location_desc,
               last_update_at, created_at, metadata, media_urls
        FROM {ISSUE_TICKET_TABLE}
        {where_clause}
        ORDER BY COALESCE(last_update_at, created_at) DESC
        LIMIT %s
    """
    params.append(limit)
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except Exception as exc:
        log.error("fetch_driver_issue_tickets failed: %s", exc)
        return []
    for row in rows:
        row["metadata_dict"] = _safe_json_load(row.pop("metadata", None))
        row["media_list"] = _safe_json_load(row.pop("media_urls", None), default=[])
    return rows


def fetch_driver_issue_ticket(ticket_id: int) -> Optional[Dict[str, Any]]:
    if not (mysql_available() and ticket_id):
        return None
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, wa_id, issue_type, status, initial_message, location_desc,
                       location_lat, location_lng, metadata, media_urls,
                       last_update_at, created_at
                FROM {ISSUE_TICKET_TABLE}
                WHERE id=%s
                LIMIT 1
                """,
                (ticket_id,),
            )
            row = cur.fetchone()
    except Exception as exc:
        log.error("fetch_driver_issue_ticket failed: %s", exc)
        return None
    if not row:
        return None
    row["metadata_dict"] = _safe_json_load(row.pop("metadata", None))
    row["media_list"] = _safe_json_load(row.pop("media_urls", None), default=[])
    return row


def fetch_open_driver_issue_ticket(
    wa_id: str,
    issue_types: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    if not (mysql_available() and wa_id):
        return None
    clean_types = [str(t).strip().lower() for t in (issue_types or []) if t]
    closed_vals = list(CLOSED_TICKET_STATUS_VALUES)
    closed_placeholders = ", ".join(["%s"] * len(closed_vals))
    params: List[Any] = [wa_id]
    params.extend(closed_vals)
    sql = f"""
        SELECT id, wa_id, issue_type, status, initial_message, location_desc,
               last_update_at, created_at, metadata, media_urls
        FROM {ISSUE_TICKET_TABLE}
        WHERE wa_id=%s
          AND (status IS NULL OR status='' OR LOWER(status) NOT IN ({closed_placeholders}))
    """
    if clean_types:
        type_placeholders = ", ".join(["%s"] * len(clean_types))
        sql += f" AND LOWER(issue_type) IN ({type_placeholders})"
        params.extend(clean_types)
    sql += " ORDER BY COALESCE(last_update_at, created_at) DESC LIMIT 1"
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
    except Exception as exc:
        log.error("fetch_open_driver_issue_ticket failed: %s", exc)
        return None
    if not row:
        return None
    row["metadata_dict"] = _safe_json_load(row.pop("metadata", None))
    row["media_list"] = _safe_json_load(row.pop("media_urls", None), default=[])
    return row


def _pick_log_column(available: set[str], candidates: List[str]) -> Optional[str]:
    for name in candidates:
        if name in available:
            return name
    return None


def fetch_ticket_conversation(wa_id: Optional[str], limit: int = 40) -> List[Dict[str, Any]]:
    if not (mysql_available() and wa_id):
        return []
    try:
        conn = get_mysql()
        table = _detect_logs_table(conn)
        available = _get_table_columns(conn, table)
        text_col = _pick_log_column(available, ["message_text", "message", "text"])
        dir_col = _pick_log_column(available, ["message_direction", "direction"])
        ts_col = _pick_log_column(available, ["timestamp", "created_at", "logged_at"])
        status_col = _pick_log_column(available, ["status"])
        intent_col = _pick_log_column(available, ["intent", "intent_label"])
        if not (text_col and dir_col and ts_col):
            return []
        sql = (
            f"SELECT {dir_col} AS direction, {text_col} AS body, {ts_col} AS ts"
        )
        if status_col:
            sql += f", {status_col} AS status"
        else:
            sql += ", NULL AS status"
        if intent_col:
            sql += f", {intent_col} AS intent"
        else:
            sql += ", NULL AS intent"

        sql += f" FROM {table} WHERE wa_id=%s AND {dir_col} IN ('INBOUND','OUTBOUND') ORDER BY {ts_col} DESC LIMIT %s"
        with conn.cursor() as cur:
            cur.execute(sql, (wa_id, limit))
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("fetch_ticket_conversation failed: %s", exc)
        return []

    def _format_ts(value: Any) -> str:
        formatted = _format_timestamp_value(value)
        if formatted:
            return formatted
        return str(value or "")

    convo = []
    for row in rows:
        convo.append(
            {
                "direction": (row.get("direction") or "").upper(),
                "body": row.get("body") or "",
                "timestamp": _format_ts(row.get("ts")),
                "status": row.get("status"),
                "intent": row.get("intent"),
            }
        )
    convo.reverse()
    return convo


def _safe_json_load(raw: Optional[Any], default: Any = None):
    if raw is None:
        return default if default is not None else {}
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except Exception:
        return default if default is not None else {}


def get_ticket_status_options(limit: int = 12) -> List[str]:
    defaults = ["collecting", "pending_ops", "pending_driver", "driver_confirmed_resolved", "closed"]
    if not mysql_available():
        return defaults
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT status FROM {ISSUE_TICKET_TABLE} WHERE status IS NOT NULL ORDER BY status ASC LIMIT %s",
                (limit,),
            )
            rows = [row["status"] for row in (cur.fetchall() or []) if row.get("status")]
    except Exception as exc:
        log.error("get_ticket_status_options failed: %s", exc)
        return defaults
    seen = set()
    merged: List[str] = []
    for status in defaults + rows:
        if status and status not in seen:
            seen.add(status)
            merged.append(status)
    return merged



# -----------------------------------------------------------------------------
# Zero-trip nudge analytics helpers
# -----------------------------------------------------------------------------
NUDGE_EVENT_TABLE = f"{MYSQL_DB}.driver_nudge_events"
INTRADAY_UPDATE_TABLE = f"{MYSQL_DB}.driver_intraday_updates"


def _insert_nudge_event(
    *,
    wa_id: str,
    nudge_number: int,
    template_index: int,
    template_message: str,
    send_status: str,
    whatsapp_message_id: Optional[str],
    send_ts: datetime,
) -> Optional[int]:
    if not mysql_available():
        return None
    try:
        conn = get_mysql()
        nudge_date = send_ts.date().isoformat()
        metadata = {
            "template_variant": template_index,
            "generated_at_ts": send_ts.timestamp(),
        }
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {NUDGE_EVENT_TABLE}
                  (wa_id, nudge_date, nudge_number, template_index, template_message,
                   send_status, whatsapp_message_id, send_ts, metadata)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    wa_id,
                    nudge_date,
                    nudge_number,
                    template_index,
                    template_message,
                    send_status,
                    whatsapp_message_id,
                    send_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    json.dumps(metadata, ensure_ascii=False),
                ),
            )
            event_id = cur.lastrowid
        return int(event_id) if event_id else None
    except Exception as exc:
        log.error("_insert_nudge_event failed: %s", exc)
        return None


def _update_nudge_event_status(
    *,
    whatsapp_message_id: Optional[str],
    status: str,
    status_code: Optional[str],
    status_detail: Optional[str],
    raw_status: Optional[dict] = None,
) -> None:
    if not (mysql_available() and whatsapp_message_id):
        return
    try:
        conn = get_mysql()
        metadata_json = json.dumps(raw_status, ensure_ascii=False) if raw_status else None
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {NUDGE_EVENT_TABLE}
                SET send_status=%s,
                    status_code=%s,
                    status_detail=%s,
                    metadata = CASE
                        WHEN %s IS NOT NULL THEN JSON_SET(IFNULL(metadata, JSON_OBJECT()), '$.status_event', CAST(%s AS JSON))
                        ELSE metadata
                    END,
                    last_update_at=CURRENT_TIMESTAMP
                WHERE whatsapp_message_id=%s
                """,
                (
                    status,
                    status_code,
                    status_detail,
                    metadata_json,
                    metadata_json,
                    whatsapp_message_id,
                ),
            )
    except Exception as exc:
        log.error("_update_nudge_event_status failed: %s", exc)


def _record_nudge_response(
    *,
    whatsapp_message_id: Optional[str],
    response_message_id: str,
    response_ts: datetime,
    response_latency_sec: Optional[float],
    response_intent: str,
) -> bool:
    if not (mysql_available() and whatsapp_message_id):
        return False
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {NUDGE_EVENT_TABLE}
                SET response_message_id=%s,
                    response_ts=%s,
                    response_latency_sec=%s,
                    response_intent=%s,
                    metadata = JSON_SET(IFNULL(metadata, JSON_OBJECT()), '$.responded', TRUE),
                    last_update_at=CURRENT_TIMESTAMP
                WHERE whatsapp_message_id=%s AND response_message_id IS NULL
                """,
                (
                    response_message_id,
                    response_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    response_latency_sec,
                    response_intent,
                    whatsapp_message_id,
                ),
            )
            return cur.rowcount > 0
    except Exception as exc:
        log.error("_record_nudge_response failed: %s", exc)
        return False


def _reserve_intraday_update(
    *,
    wa_id: str,
    update_date: datetime.date,
    slot_hour: int,
    target_trips: int,
    finished_trips: Optional[int],
    acceptance_rate: Optional[float],
) -> bool:
    if not mysql_available():
        return False
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT IGNORE INTO {INTRADAY_UPDATE_TABLE}
                  (wa_id, update_date, slot_hour, target_trips, finished_trips, acceptance_rate, send_status)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    wa_id,
                    update_date.isoformat(),
                    slot_hour,
                    target_trips,
                    finished_trips,
                    acceptance_rate,
                    "pending",
                ),
            )
            return cur.rowcount > 0
    except Exception as exc:
        log.warning("_reserve_intraday_update failed: %s", exc)
        return False


def _update_intraday_update_status(
    *,
    wa_id: str,
    update_date: datetime.date,
    slot_hour: int,
    status: str,
    whatsapp_message_id: Optional[str],
) -> None:
    if not mysql_available():
        return
    try:
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {INTRADAY_UPDATE_TABLE}
                SET send_status=%s,
                    whatsapp_message_id=%s,
                    sent_at=CURRENT_TIMESTAMP
                WHERE wa_id=%s AND update_date=%s AND slot_hour=%s
                """,
                (
                    status,
                    whatsapp_message_id,
                    wa_id,
                    update_date.isoformat(),
                    slot_hour,
                ),
            )
    except Exception as exc:
        log.warning("_update_intraday_update_status failed: %s", exc)
# -----------------------------------------------------------------------------
# Context intent helpers
# -----------------------------------------------------------------------------
TICKET_STATUS_CACHE_TTL_SECONDS = int(os.getenv("TICKET_STATUS_CACHE_TTL_SECONDS", "300"))
CLOSED_TICKET_STATUS_VALUES = {
    "closed",
    "resolved",
    "driver_confirmed_resolved",
    "complete",
    "completed",
    "done",
    "finished",
    "success",
    "successful",
}

def _normalize_ticket_status(value: Optional[Any]) -> str:
    return str(value or "").strip().lower()

def _is_ticket_status_closed(status: Optional[Any]) -> bool:
    status_clean = _normalize_ticket_status(status)
    if not status_clean:
        return False
    return status_clean in CLOSED_TICKET_STATUS_VALUES

def _ticket_status_from_db(ticket_id: Optional[int], ctx: Dict[str, Any]) -> Optional[str]:
    if not (mysql_available() and ticket_id):
        return None
    cache = ctx.setdefault("_ticket_status_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        ctx["_ticket_status_cache"] = cache
    cache_key = str(ticket_id)
    cached = cache.get(cache_key)
    if isinstance(cached, dict):
        try:
            checked_at = float(cached.get("checked_at") or 0)
            if checked_at and (time.time() - checked_at) < TICKET_STATUS_CACHE_TTL_SECONDS:
                return cached.get("status")
        except Exception:
            pass
    status = None
    ticket = fetch_driver_issue_ticket(ticket_id)
    if isinstance(ticket, dict):
        status = _normalize_ticket_status(ticket.get("status"))
    cache[cache_key] = {"status": status, "checked_at": time.time()}
    return status

def _ticket_ctx_is_closed(ticket_ctx: Optional[Dict[str, Any]], ctx: Dict[str, Any]) -> bool:
    if not isinstance(ticket_ctx, dict):
        return False
    if ticket_ctx.get("closed"):
        return True
    status = _normalize_ticket_status(ticket_ctx.get("status"))
    if _is_ticket_status_closed(status):
        return True
    ticket_id = ticket_ctx.get("ticket_id")
    status = _ticket_status_from_db(ticket_id, ctx)
    if _is_ticket_status_closed(status):
        ticket_ctx["status"] = status
        ticket_ctx["closed"] = True
        ticket_ctx["closed_at"] = ticket_ctx.get("closed_at") or jhb_now().strftime("%Y-%m-%d %H:%M:%S")
        return True
    return False

def _clear_context_prefix(ctx: Dict[str, Any], prefix: str) -> None:
    for key in list(ctx.keys()):
        if key.startswith(prefix):
            ctx.pop(key, None)

def _clear_closed_ticket_context(ctx: Dict[str, Any]) -> None:
    if not isinstance(ctx, dict) or not ctx:
        return
    closed_types: set[str] = set()
    ticket_specs = [
        ("_pop_ticket", "pop_submission", "_pop_"),
        ("_medical_ticket", "medical", "_medical_"),
        ("_no_vehicle_ticket", "no_vehicle", "_no_vehicle_"),
        ("_no_vehicle_finance_ticket", "no_vehicle", "_no_vehicle_"),
        ("_no_vehicle_other_ticket", "no_vehicle", "_no_vehicle_"),
        ("_balance_dispute_ticket", "balance_dispute", "_balance_dispute_"),
        ("_low_demand_ticket", "low_demand", "_low_demand_"),
        ("_account_suspension_ticket", "account_suspension", "_account_suspension_"),
        ("_app_issue_ticket", "app_issue", "_app_issue_"),
        ("_car_ticket", "car", None),
        ("_cash_ticket", "cash_ride", None),
        ("_accident_case", "accident", None),
    ]
    for ticket_key, concern_type, prefix in ticket_specs:
        ticket_ctx = ctx.get(ticket_key)
        if isinstance(ticket_ctx, dict) and _ticket_ctx_is_closed(ticket_ctx, ctx):
            ctx.pop(ticket_key, None)
            if prefix:
                _clear_context_prefix(ctx, prefix)
            closed_types.add(concern_type)

    if not closed_types:
        return

    active_concern = ctx.get("_active_concern")
    if isinstance(active_concern, dict) and active_concern.get("type") in closed_types:
        ctx.pop("_active_concern", None)

    if "no_vehicle" in closed_types and ctx.get("_pending_intent") == PENDING_NO_VEHICLE_REASON:
        ctx.pop("_pending_intent", None)
    if "balance_dispute" in closed_types:
        ctx.pop("_balance_dispute_pending", None)

    closed_intents: set[str] = set()
    if "car" in closed_types:
        closed_intents.add("car_problem")
    if "medical" in closed_types:
        closed_intents.add("medical_issue")
    if "no_vehicle" in closed_types:
        closed_intents.add("no_vehicle")
    if "balance_dispute" in closed_types:
        closed_intents.add("balance_dispute")
    if "low_demand" in closed_types:
        closed_intents.add("low_demand")
    if "account_suspension" in closed_types:
        closed_intents.add("account_suspension")
    if "app_issue" in closed_types:
        closed_intents.add("app_issue")
    if "cash_ride" in closed_types:
        closed_intents.add(CASH_BALANCE_UPDATE_INTENT)
    if "accident" in closed_types:
        closed_intents.add(ACCIDENT_REPORT_INTENT)
    if ctx.get("_last_intent") in closed_intents:
        ctx.pop("_last_intent", None)

MEDIA_INTENT_OVERRIDES = {
    "image": "media_image",
    "video": "media_video",
    "audio": "media_audio",
    "sticker": "media_sticker",
    "document": "media_document",
    "voice": "media_voice",
    "location": "media_location",
    "contacts": "media_contacts",
    "reaction": "media_reaction",
}

def resolve_context_intent(
    *,
    detected_intent: str,
    message_type: str,
    ctx: Dict[str, Any],
    message_text: str,
) -> str:
    message_text = _normalize_text(message_text or "")
    accident_ctx = ctx.get("_accident_case") if isinstance(ctx.get("_accident_case"), dict) else None
    accident_status = accident_ctx.get("status") if accident_ctx else None
    accident_awaiting = accident_ctx.get("awaiting") if accident_ctx else None
    accident_closed = bool(accident_ctx.get("closed")) if accident_ctx else False
    accident_active = (accident_status in {"collecting", "pending_ops"}) and not accident_closed
    accident_waiting = accident_active and bool(accident_awaiting)

    if detected_intent and detected_intent != "unknown":
        if accident_waiting and detected_intent in {"acknowledgement"}:
            return ACCIDENT_REPORT_INTENT
        return detected_intent

    if message_type in AUDIO_MESSAGE_TYPES:
        audio_status = ctx.get("_audio_transcript_status")
        if audio_status == "failed":
            return "voice_unavailable"
        if audio_status == "ok":
            if accident_active:
                return ACCIDENT_REPORT_INTENT
            previous = ctx.get("_last_intent")
            if previous and previous != "unknown":
                if previous == ACCIDENT_REPORT_INTENT and accident_closed:
                    return "clarify"
                return previous
            return "clarify"

    if message_type and message_type != "text":
        if accident_active:
            return ACCIDENT_REPORT_INTENT
        cash_ticket = ctx.get("_cash_ticket")
        if (
            isinstance(cash_ticket, dict)
            and cash_ticket.get("status") in {"collecting", "pending_ops"}
            and cash_ticket.get("awaiting_pop", True)
            and message_type in CASH_POP_MEDIA_TYPES
        ):
            return CASH_BALANCE_UPDATE_INTENT
        ticket_ctx = ctx.get("_car_ticket")
        if isinstance(ticket_ctx, dict) and ticket_ctx.get("status") in {"collecting", "pending_ops"} and not ticket_ctx.get("closed"):
            return "car_problem"
        override = MEDIA_INTENT_OVERRIDES.get(message_type)
        if override:
            return override
        return "media_message"

    pending = ctx.get("_pending_intent")
    if pending in {PENDING_EARNINGS_TRIPS, PENDING_EARNINGS_AVG}:
        return "earnings_projection"
    if pending == PENDING_REPOSSESSION_REASON and _parse_repossession_reason(message_text or ""):
        return "vehicle_repossession"
    if pending == PENDING_NO_VEHICLE_REASON:
        if _is_vehicle_back(message_text) and not _is_no_vehicle(message_text):
            return "vehicle_back"
        if _is_medical_issue(message_text):
            return "medical_issue"
        if _is_vehicle_repossession(message_text):
            return "vehicle_repossession"
        if _parse_no_vehicle_reason(message_text):
            return "no_vehicle"
    if ctx.get("_balance_dispute_pending"):
        return "balance_dispute"

    lowered = (message_text or "").strip().lower()
    if lowered:
        if _is_account_suspension(message_text):
            return "account_suspension"
        if _is_app_issue(message_text):
            return "app_issue"
        if _is_vehicle_back(message_text) and not _is_no_vehicle(message_text):
            return "vehicle_back"
        if _is_balance_dispute(message_text):
            return "balance_dispute"
        if re.search(r"\b\d+(?:\.\d+)?\s*(?:per\s*hour|/hour|ph)\b", lowered):
            return "earnings_per_hour_status"
        if any(tok == lowered for tok in ACKNOWLEDGEMENT_TOKENS):
            return "acknowledgement"
        if any(kw in lowered for kw in CONCERN_COST_KEYWORDS):
            return "cost_concern"
        if _is_medical_issue(message_text):
            return "medical_issue"
        if _is_no_vehicle(message_text):
            return "no_vehicle"
        if _is_vehicle_repossession(message_text):
            return "vehicle_repossession"
        if _is_car_problem(message_text):
            return "car_problem"
        if any(kw in lowered for kw in CONCERN_GENERAL_KEYWORDS):
            return "raise_concern"
        if accident_waiting:
            return ACCIDENT_REPORT_INTENT
        if _is_schedule_update(message_text):
            return "schedule_update"

    active_concern = ctx.get("_active_concern")
    if isinstance(active_concern, dict):
        concern_type = active_concern.get("type")
        if concern_type == "cost":
            return "cost_concern"
        if concern_type == "medical":
            return "medical_issue"
        if concern_type == "no_vehicle":
            return "no_vehicle"
        if concern_type == "balance_dispute":
            return "balance_dispute"
        if concern_type == "app_issue":
            return "app_issue"
        if concern_type == "car":
            return "car_problem"
        if concern_type == "repossession":
            if _is_vehicle_repossession(message_text) or _parse_repossession_reason(message_text or ""):
                return "vehicle_repossession"
        if concern_type == "general":
            return "raise_concern"
        if concern_type == "accident" and accident_active:
            return ACCIDENT_REPORT_INTENT
        if concern_type == "cash_ride":
            return CASH_BALANCE_UPDATE_INTENT
        if concern_type == "branding_bonus":
            return BRANDING_BONUS_INTENT

    previous = ctx.get("_last_intent")
    if not lowered and previous and previous != "unknown":
        if previous == ACCIDENT_REPORT_INTENT and (not accident_active) and accident_closed:
            return "clarify"
        return previous

    return "clarify"

# -----------------------------------------------------------------------------
# Driver lookup (name + asset + xero ids)
# -----------------------------------------------------------------------------
def lookup_driver_by_wa(wa_id: str) -> Dict[str, Any]:
    """Lookup driver details from the KPI summary table (no SimplyFleet dependency)."""
    if not mysql_available():
        return {"first_name": "Driver", "last_name": "", "display_name": "Driver"}

    try:
        conn = get_mysql()
        table = f"{MYSQL_DB}.driver_kpi_summary"
        available = _get_table_columns(conn, table)

        phone_col = _pick_col_exists(conn, table, ["phone", "wa_id", "whatsapp", "whatsapp_number", "wa_number", "phone_number", "contact_number"])
        name_col = _pick_col_exists(conn, table, ["name", "full_name", "driver_name"])
        model_col = _pick_col_exists(conn, table, ["model", "asset_model"])
        reg_col = _pick_col_exists(conn, table, ["car_reg_number", "vehicle_number", "registration_number", "reg_number"])
        xero_col = _pick_col_exists(conn, table, ["xero_contact_id", "contact_id", "account_id"])
        personal_col = _pick_col_exists(conn, table, ["personal_code"])
        driver_id_col = _pick_col_exists(conn, table, ["driver_id", "id"])

        if not phone_col:
            return {"first_name": "Driver", "last_name": "", "display_name": "Driver"}

        variants = _wa_number_variants(wa_id)
        if not variants:
            clean = re.sub(r"\D", "", wa_id or "")
            variants = [clean] if clean else []
        if not variants:
            return {"first_name": "Driver", "last_name": "", "display_name": "Driver"}

        sanitized_expr = _sanitize_phone_expr(phone_col)
        placeholders = ", ".join(["%s"] * len(variants))

        select_cols = []
        if name_col: select_cols.append(name_col)
        if model_col: select_cols.append(model_col)
        if reg_col: select_cols.append(reg_col)
        if xero_col: select_cols.append(xero_col)
        if personal_col: select_cols.append(personal_col)
        if driver_id_col: select_cols.append(driver_id_col)
        select_cols.append(phone_col)
        select_sql = ", ".join(select_cols) if select_cols else "*"

        order_clause = " ORDER BY report_date DESC" if "report_date" in available else ""
        sql = (
            f"SELECT {select_sql} FROM {table} "
            f"WHERE {sanitized_expr} IN ({placeholders}){order_clause} LIMIT 1"
        )

        with conn.cursor() as cur:
            cur.execute(sql, variants)
            row = cur.fetchone() or {}

        full = (row.get(name_col or "") or "").strip()
        parts = full.split()
        first = parts[0] if parts else "Driver"
        last = " ".join(parts[1:]) if len(parts) > 1 else ""

        driver_id_val = row.get(driver_id_col) if driver_id_col else None
        if driver_id_val is None:
            driver_id_val = _lookup_bolt_driver_id_by_wa(conn, wa_id)

        xero_ids: List[str] = []
        if xero_col and row.get(xero_col):
            try:
                xero_ids = [str(row.get(xero_col)).strip()]
            except Exception:
                xero_ids = []

        return {
            "first_name": first,
            "last_name": last,
            "display_name": full or f"{first} {last}".strip(),
            "asset_model": row.get(model_col) if model_col else "",
            "car_reg_number": row.get(reg_col) if reg_col else None,
            "xero_contact_ids": xero_ids,
            "personal_code": row.get(personal_col) if personal_col else None,
            "driver_phone": row.get(phone_col),
            "driver_source": "driver_kpi_summary",
            "driver_id": driver_id_val,
        }
    except Exception as e:
        log.warning("lookup_driver_by_wa failed: %s", e)
        return {"first_name": "Driver", "last_name": "", "display_name": "Driver", "xero_contact_ids": []}

def _lookup_bolt_driver_id_by_wa(conn, wa_id: str) -> Optional[int]:
    bolt_drivers_table = f"{MYSQL_DB}.bolt_drivers"
    if not _table_exists(conn, bolt_drivers_table):
        return None
    id_col = _pick_col_exists(conn, bolt_drivers_table, ["id", "driver_id"])
    phone_col = _pick_col_exists(conn, bolt_drivers_table, SIMPLYFLEET_WHATSAPP_COLUMNS + ["mobile", "msisdn"])
    if not id_col or not phone_col:
        return None
    variants = _wa_number_variants(wa_id)
    if not variants:
        clean = re.sub(r"\D", "", wa_id or "")
        variants = [clean] if clean else []
    if not variants:
        return None
    sanitized_expr = _sanitize_phone_expr(phone_col)
    placeholders = ", ".join(["%s"] * len(variants))
    sql = (
        f"SELECT {id_col} AS driver_id FROM {bolt_drivers_table} "
        f"WHERE {sanitized_expr} IN ({placeholders}) "
        f"ORDER BY {id_col} DESC LIMIT 1"
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(variants))
            row = cur.fetchone() or {}
    except Exception as exc:
        log.debug("bolt driver_id lookup failed: %s", exc)
        return None
    driver_id = row.get("driver_id")
    try:
        return int(driver_id) if driver_id is not None else None
    except Exception:
        return None

def lookup_driver_by_personal_code(code: str) -> Dict[str, Any]:
    """Lookup driver details from the KPI summary table by personal code."""
    if not mysql_available():
        return {"first_name": "Driver", "last_name": "", "display_name": "Driver"}

    clean = re.sub(r"\D", "", code or "")
    if not clean:
        return {"first_name": "Driver", "last_name": "", "display_name": "Driver"}

    try:
        conn = get_mysql()
        table = f"{MYSQL_DB}.driver_kpi_summary"
        available = _get_table_columns(conn, table)

        personal_col = _pick_col_exists(conn, table, ["personal_code"])
        if not personal_col:
            return {"first_name": "Driver", "last_name": "", "display_name": "Driver"}

        name_col = _pick_col_exists(conn, table, ["name", "full_name", "driver_name"])
        model_col = _pick_col_exists(conn, table, ["model", "asset_model"])
        reg_col = _pick_col_exists(conn, table, ["car_reg_number", "vehicle_number", "registration_number", "reg_number"])
        xero_col = _pick_col_exists(conn, table, ["xero_contact_id", "contact_id", "account_id"])
        phone_col = _pick_col_exists(conn, table, ["phone", "wa_id", "whatsapp", "whatsapp_number", "wa_number", "phone_number", "contact_number"])
        driver_id_col = _pick_col_exists(conn, table, ["driver_id", "id"])

        select_cols = [personal_col]
        if name_col: select_cols.append(name_col)
        if model_col: select_cols.append(model_col)
        if reg_col: select_cols.append(reg_col)
        if xero_col: select_cols.append(xero_col)
        if phone_col: select_cols.append(phone_col)
        if driver_id_col: select_cols.append(driver_id_col)
        select_sql = ", ".join(select_cols) if select_cols else "*"

        order_clause = " ORDER BY report_date DESC" if "report_date" in available else ""
        sql = f"SELECT {select_sql} FROM {table} WHERE {personal_col} = %s{order_clause} LIMIT 1"

        with conn.cursor() as cur:
            cur.execute(sql, (clean,))
            row = cur.fetchone() or {}

        if not row:
            return {"first_name": "Driver", "last_name": "", "display_name": "Driver"}

        full = (row.get(name_col or "") or "").strip()
        parts = full.split()
        first = parts[0] if parts else "Driver"
        last = " ".join(parts[1:]) if len(parts) > 1 else ""

        xero_ids: List[str] = []
        if xero_col and row.get(xero_col):
            try:
                xero_ids = [str(row.get(xero_col)).strip()]
            except Exception:
                xero_ids = []

        return {
            "first_name": first,
            "last_name": last,
            "display_name": full or f"{first} {last}".strip(),
            "asset_model": row.get(model_col) if model_col else "",
            "car_reg_number": row.get(reg_col) if reg_col else None,
            "xero_contact_ids": xero_ids,
            "personal_code": row.get(personal_col) if personal_col else None,
            "driver_phone": row.get(phone_col) if phone_col else None,
            "driver_source": "driver_kpi_summary",
            "driver_id": row.get(driver_id_col) if driver_id_col else None,
        }
    except Exception as e:
        log.warning("lookup_driver_by_personal_code failed: %s", e)
        return {"first_name": "Driver", "last_name": "", "display_name": "Driver", "xero_contact_ids": []}

# -----------------------------------------------------------------------------
# Driver KPI targets + performance metrics
# -----------------------------------------------------------------------------
BOLT_ORDER_TABLES = [
    lambda db: f"{db}.bolt_orders_new",
    lambda db: f"{db}.bolt_orders",
    lambda db: f"{db}.bolt_daily_orders",
    lambda _db: "mnc_reports.bolt_orders",
]

BOLT_AREA_COLUMNS = [
    "pickup_suburb",
    "suburb",
    "pickup_area",
    "area",
    "pickup_zone",
    "zone",
    "pickup_region",
    "region",
    "pickup_address",
    "address",
]

BOLT_EARNINGS_COLUMNS = [
    "total_earnings","fare","amount","total_amount","driver_earnings","earnings","net_earnings",
    "total_price","fare_finalised"
]

BOLT_TRIP_COLUMNS = [
    "completed_trips","trips","orders","ride_count","total_rides","completed_orders"
]

BOLT_DATE_COLUMNS = [
    "order_date","created_at","completed_at","finished_at","order_completed_at","timestamp","datetime","order_time","start_time",
    "driver_assigned_time","driver_assigned_timestamp","driver_assigned_at"
]

BOLT_CONTACT_COLUMNS = [
    "contact_id","xero_contact_id","driver_id","driver_uuid","wa_id","account_id","uuid","driver_contact_id"
]

BOLT_ADDRESS_SPLIT_RE = re.compile(r"[,|-]")
BOLT_STATUS_COLUMNS = ["status","order_status","trip_status","state","ride_status","journey_status"]
FINISHED_STATUS_VALUES = ["completed","finished","done","closed","success","successful"]

SUBURB_MAP_PATH = _env("SUBURB_MAP_PATH", "suburb_map.csv")
SUBURB_MAP_CACHE: Optional[List[Tuple[str, str]]] = None

HOTSPOT_SCOPE_DEFAULT = "driver"
HOTSPOT_SCOPE_GLOBAL_PATTERNS = [
    r"\bglobal\b",
    r"\ball drivers?\b",
    r"\ball (the )?drivers\b",
    r"\bfleet\b",
    r"\boverall\b",
    r"\beveryone\b",
    r"\bwhole (team|fleet)\b",
    r"\bcompany\b",
]

HOTSPOT_TIMEFRAME_DEFAULT = "today"
HOTSPOT_TIMEFRAME_PATTERNS: List[Tuple[str, List[str]]] = [
    ("last_month", [r"\blast month\b", r"\bprevious month\b"]),
    ("this_month", [r"\bthis month\b", r"\bcurrent month\b"]),
    ("last_week", [r"\blast week\b", r"\bprevious week\b"]),
    ("this_week", [r"\bthis week\b", r"\bcurrent week\b", r"\bweek to date\b", r"\bweek-to-date\b"]),
    ("today", [r"\btoday\b", r"\btonight\b", r"\bright now\b"]),
]

HOTSPOT_LABELS = {
    "today": "today",
    "this_week": "this week",
    "last_week": "last week",
    "this_month": "this month",
    "last_month": "last month",
}

BOT_IDENTITY_KEYWORDS = [
    "your name",
    "whats your name",
    "what is your name",
    "who is this",
    "who are you",
    "who am i chatting with",
    "who am i talking to",
    "tell me your name",
    "what do i call you",
]

CONCERN_GENERAL_KEYWORDS = [
    "want to complain", "have a complaint", "file a complaint", "raise a complaint",
    "i want to complain", "complain about", "not happy", "unhappy", "frustrated", "issue with",
    "need to escalate", "want to escalate", "this is a problem", "it's a problem", "problem with",
    "i'm upset", "i am upset", "i'm angry", "i am angry", "i'm annoyed", "i am annoyed",
]

CONCERN_COST_KEYWORDS = [
    "too expensive", "cost is too high", "costs are too high", "fuel is expensive",
    "km cost", "kms cost", "kilometre cost", "mileage cost", "excess km", "extra km cost",
    "excess mileage", "extra mileage", "cost me too much", "costing too much",
]

BALANCE_DISPUTE_KEYWORDS = [
    "dispute", "disputed", "wrong balance", "incorrect balance", "balance is wrong",
    "balance seems wrong", "balance not right", "not my balance", "not correct",
    "incorrect", "mistake", "error in balance", "statement wrong", "charges wrong",
    "overcharged", "over charged", "charged too much", "shouldn't owe", "shouldnt owe",
]

CASH_ENABLE_KEYWORDS = [
    "cash enabled", "enable cash", "cash reactivation", "reactivate cash",
    "turn on cash", "switch on cash", "cash rides", "cash rides enabled",
    "enable cash rides", "cash trips", "need cash",
    "cash option", "cash enablement",
]

CASH_BALANCE_UPDATE_KEYWORDS = [
    "reduced my balance", "paid my balance", "paid the balance", "cleared my balance",
    "paid my account", "made a payment", "made payment", "i have paid", "i paid already",
    "settled the balance", "balance settled", "paid towards my account", "paid the amount",
    "reduced balance", "i've paid", "i have paid", "cleared the amount",
]
KPI_COMMITMENT_VERBS = {
    "commit", "committed", "commitment", "will", "i'll", "ill", "can",
    "promise", "aim", "aiming", "focus", "focused", "push", "improve", "increase",
    "reach", "hit", "do", "try",
}
KPI_COMMITMENT_METRICS = {
    "kpi", "kpis", "performance", "target", "targets", "goal", "goals",
    "hours", "online", "trips", "rides", "acceptance", "rate",
    "earnings", "hour", "eph", "gross",
}
KPI_COMMITMENT_PHRASES = [
    "i can commit", "i will commit", "i'll commit", "i will do", "i'll do",
    "i can do", "i will improve", "i'll improve", "i can improve",
    "i will try", "i'll try", "i will be online", "i'll be online",
    "i will hit", "i'll hit", "i will reach", "i'll reach",
    "will do", "will do it",
]

BRANDING_BONUS_KEYWORDS = [
    "branding bonus", "branding payout", "branding incentive", "branding campaign bonus",
    "campaign bonus", "campaign payout", "advertising bonus", "advertising payout",
    "branding allowance",
]

CASH_POP_MEDIA_TYPES = {"image", "document"}
MEDICAL_CERT_MEDIA_TYPES = {"image", "document"}
POP_PENDING_TTL_HOURS = int(os.getenv("POP_PENDING_TTL_HOURS", "24"))
POP_CONFIRM_KEYWORDS = [
    "pop",
    "proof of payment",
    "payment proof",
    "payment receipt",
    "payment confirmation",
    "bank transfer",
    "eft",
    "deposit",
    "receipt",
]

CAR_PROBLEM_KEYWORDS = [
    "car problem", "car issue", "vehicle problem", "vehicle issue",
    "car is broken", "car broke", "car not working", "car isnt working", "car isn't working",
    "car wont start", "car won't start", "car won't move", "car wont move",
    "car is leaking", "car leaking", "leaking oil", "oil leak", "car overheating",
    "engine light", "check engine", "warning light", "gearbox problem", "brake problem",
    "tyre puncture", "tire puncture", "flat tyre", "flat tire", "car shaking",
]

MEDICAL_ISSUE_KEYWORDS = [
    "sick", "ill", "illness", "unwell", "not well", "fever", "flu", "headache",
    "hospital", "hospitalised", "hospitalized", "clinic", "doctor", "medical", "injured", "injury", "pain", "ambulance",
    "icu", "ward", "admitted", "surgery", "operation", "oxygen", "monitor", "monitors",
]

LOW_DEMAND_EXPLICIT_PHRASES = [
    "not getting trips", "not getting any trips", "not getting rides", "not getting any rides",
    "not getting orders", "not getting any orders", "not getting requests", "not getting any requests",
    "not receiving trips", "not receiving rides", "not receiving orders", "not receiving requests",
    "not getting jobs", "not getting any jobs", "not getting work", "not getting any work",
    "not getting bookings", "not getting any bookings", "not getting pings", "not getting any pings",
    "no trips", "no rides", "no orders", "no requests", "no trip requests", "no jobs", "no work", "no pings",
    "few trips", "few rides", "few orders", "few requests", "low demand", "no demand",
    "not giving me trips", "you are not giving me trips",
]

NO_VEHICLE_KEYWORDS = [
    "no car", "no vehicle", "dont have a car", "don't have a car", "without a car",
    "no cab", "no taxi", "no qute", "no vitz", "no car today", "no car right now",
    "vehicle not available", "car not available", "waiting for car", "awaiting car",
    "no car assigned", "no vehicle assigned", "between vehicles", "car taken",
    "vehicle taken", "car not with me", "vehicle not with me", "off the road",
    "not driving", "no ride", "in repair", "in repairs", "in workshop", "in service", "in maintenance",
    "returned to office", "returned car", "returned the car", "returned vehicle",
    "handed back", "handed the car back", "gave back the car", "car back at office",
]

VEHICLE_BACK_KEYWORDS = [
    "got a car now", "got a car", "got car now", "got car back", "got the car back",
    "car is back", "vehicle is back", "vehicle back", "car back", "car back now",
    "car returned", "vehicle returned", "collected the car", "collected car",
    "picked up the car", "picked up car", "car with me now", "vehicle with me now",
    "have a car", "have a car now", "have the car", "have the car back", "have car now",
]

NO_VEHICLE_WORKSHOP_KEYWORDS = [
    "workshop", "service", "maintenance", "repair", "mechanic", "garage", "broken", "breakdown",
]

NO_VEHICLE_ASSIGNMENT_KEYWORDS = [
    "waiting for car", "awaiting car", "no car assigned", "no vehicle assigned",
    "replacement", "replacement car", "replacement vehicle", "waiting for replacement",
    "waiting for a replacement", "awaiting replacement", "replacement pending",
    "swap", "handover", "collection", "collect", "new car",
]

NO_VEHICLE_BLOCKED_KEYWORDS = [
    "blocked", "suspended", "banned", "ban", "account hold", "account blocked",
    "platform blocked", "deactivated",
]

ACCOUNT_SUSPENSION_KEYWORDS = [
    "account suspended",
    "account got suspended",
    "account is suspended",
    "account blocked",
    "account got blocked",
    "account is blocked",
    "account banned",
    "account got banned",
    "account is banned",
    "account deactivated",
    "account disabled",
    "profile suspended",
    "driver profile suspended",
    "bolt account suspended",
    "bolt profile suspended",
    "platform suspended",
    "platform blocked",
]
APP_ISSUE_KEYWORDS = [
    "app", "login", "log in", "log-in", "cant login", "can't login", "cannot login",
    "not logging in", "not log in", "not working", "app not working", "app issue",
    "network", "data", "signal", "gps", "update", "crash", "frozen", "stuck", "error",
]

VEHICLE_REPOSSESSION_KEYWORDS = [
    "repossessed",
    "repossession",
    "repossess",
    "repoed",
    "car was repossessed",
    "vehicle was repossessed",
    "car got repossessed",
    "vehicle got repossessed",
    "car seized",
    "vehicle seized",
    "car impounded",
    "vehicle impounded",
]

def _pop_pending_expired(ctx: Dict[str, Any]) -> bool:
    try:
        pending_at = float(ctx.get("_pop_pending_at") or 0)
    except Exception:
        pending_at = 0
    if not pending_at:
        return False
    return (time.time() - pending_at) > POP_PENDING_TTL_HOURS * 3600

def _parse_pop_confirmation(text: str) -> Optional[bool]:
    if not text:
        return None
    lowered = _normalize_text(text).strip().lower()
    if not lowered:
        return None
    if any(phrase in lowered for phrase in ["not pop", "not a pop", "no pop", "not proof"]):
        return False
    if _is_negative_confirmation(lowered):
        return False
    if re.search(r"\bpop\b", lowered):
        return True
    if any(kw in lowered for kw in POP_CONFIRM_KEYWORDS):
        return True
    if _is_positive_confirmation(lowered):
        return True
    return None

def _pop_prompt_allowed(ctx: Dict[str, Any], intent: str) -> bool:
    if intent not in {"media_image", "media_document", "media_message", "clarify", "unknown"}:
        return False
    if ctx.get("_pop_pending_confirmation"):
        return False
    if ctx.get("_medical_pending_certificate") or ctx.get("_medical_pending_location") or ctx.get("_medical_pending_commitment"):
        return False
    if ctx.get("_no_vehicle_pending"):
        return False
    if ctx.get("_pending_intent"):
        return False
    active_concern = ctx.get("_active_concern")
    if isinstance(active_concern, dict):
        return False
    return True

REPOSSESSION_BALANCE_KEYWORDS = [
    "outstanding",
    "balance",
    "arrears",
    "overdue",
    "late payment",
    "missed payment",
    "unpaid",
    "default",
    "owe",
    "owing",
    "debt",
]

REPOSSESSION_BEHAVIOR_KEYWORDS = [
    "behaviour",
    "behavior",
    "misconduct",
    "bad behaviour",
    "bad behavior",
    "complaint",
    "unsafe",
    "reckless",
    "fraud",
    "abuse",
    "policy",
    "violation",
    "blocked",
    "block",
    "banned",
    "ban",
    "suspended",
    "suspension",
    "blacklisted",
]

CAR_PROBLEM_VEHICLE_TERMS = {
    "car", "vehicle", "cab", "taxi", "qute", "vitz", "suzuki", "almera", "micra",
    "engine", "gearbox", "motor", "brake", "brakes", "tyre", "tire", "wheel", "battery",
}

CAR_PROBLEM_SYMPTOM_TERMS = {
    "problem", "issue", "broken", "broke", "fault", "leak", "leaking", "leaks",
    "smoke", "smoking", "overheat", "overheating", "won't start", "wont start", "won't move",
    "wont move", "stalled", "stuck", "noisy", "noise", "warning", "light", "flat",
}

CAR_PROBLEM_MEDIA_TYPES = {"image", "video", "document"}
CAR_PROBLEM_RESOLUTION_KEYWORDS = [
    "sorted out",
    "sorted now",
    "sorted thanks",
    "sorted thank you",
    "sorted",
    "fixed",
    "issue resolved",
    "problem resolved",
    "all good now",
    "all sorted",
    "no longer needed",
    "dont worry anymore",
    "sorted it",
    "i fixed it",
    "it's fixed",
    "its fixed",
    "handled",
    "resolved thanks",
]

ACKNOWLEDGEMENT_TOKENS = {"ok", "okay", "cool", "thanks", "thank you", "noted", "got it", "done"}

ACCIDENT_KEYWORDS = [
    "i was in an accident",
    "i was involved in an accident",
    "had an accident",
    "been in an accident",
    "car accident",
    "vehicle accident",
    "got into an accident",
    "crashed the car",
    "car crash",
    "got in a crash",
    "collision",
    "collided",
    "i hit someone",
    "someone hit me",
    "another car hit",
    "rear ended",
    "fender bender",
    "got bumped",
    "got rammed",
    "knocked into",
]

ACCIDENT_VERB_TERMS = {
    "accident",
    "crash",
    "crashed",
    "collided",
    "collision",
    "hit",
    "bumped",
    "rammed",
    "smashed",
    "knocked",
    "rear",
}

ACCIDENT_OBJECT_TERMS = {
    "car",
    "vehicle",
    "truck",
    "bus",
    "taxi",
    "bike",
    "minibus",
    "motorcycle",
    "pedestrian",
    "someone",
    "person",
    "pole",
    "wall",
}

ACCIDENT_MEDIA_TYPES = {"image", "video", "document"}

YES_TOKENS = {"yes", "y", "yeah", "yep", "affirmative", "sure", "please", "ok", "okay"}
NO_TOKENS = {"no", "n", "nope", "nah", "fine", "alright", "i'm ok", "im ok", "i am ok", "i'm fine", "im fine"}

ACCIDENT_REPORT_INTENT = "accident_report"
CASH_ENABLE_INTENT = "cash_enable_request"
CASH_BALANCE_UPDATE_INTENT = "cash_balance_update"
BRANDING_BONUS_INTENT = "branding_bonus_issue"

INJURY_POSITIVE_KEYWORDS = [
    "injured",
    "injury",
    "hurt",
    "hurting",
    "bleeding",
    "blood",
    "pain",
    "sore",
    "fracture",
    "broken",
    "hospital",
    "ambulance",
    "paramedic",
    "doctor",
    "not ok",
    "not okay",
    "can't move",
    "cannot move",
    "can't walk",
    "cannot walk",
]

INJURY_NEGATIVE_KEYWORDS = [
    "not injured",
    "no injuries",
    "no injury",
    "i'm fine",
    "im fine",
    "i am fine",
    "i'm okay",
    "im okay",
    "i am okay",
    "everyone ok",
    "everyone okay",
    "no one hurt",
    "all good",
    "i'm alright",
    "im alright",
    "i am alright",
    "i'm safe",
    "im safe",
]

NO_OTHER_VEHICLE_PHRASES = [
    "no other car",
    "no other vehicle",
    "no one else",
    "nobody else",
    "just me",
    "only me",
    "only my car",
    "alone",
    "single vehicle",
]

OTHER_VEHICLE_POSITIVE_PHRASES = [
    "another car",
    "another vehicle",
    "other car",
    "other vehicle",
    "other driver",
    "someone else",
    "they hit",
    "they crashed",
    "a taxi",
    "a truck",
    "a bus",
    "rear ended",
]

OTHER_VEHICLE_DETAIL_KEYWORDS = [
    "reg",
    "registration",
    "plate",
    "plates",
    "license",
    "licence",
    "driver",
    "contact",
    "phone",
    "cell",
    "number",
    "id",
    "insurance",
]

ADDRESS_KEYWORDS = {
    "street",
    "st",
    "road",
    "rd",
    "avenue",
    "ave",
    "drive",
    "dr",
    "lane",
    "ln",
    "junction",
    "mall",
    "centre",
    "center",
    "garage",
    "station",
    "depot",
    "corner",
    "cnr",
    "park",
    "close",
    "crescent",
    "highway",
    "hwy",
    "boulevard",
    "blvd",
    "block",
}

AUDIO_MESSAGE_TYPES = {"audio", "voice"}
MIME_SUFFIX_MAP = {
    "audio/ogg": ".ogg",
    "audio/opus": ".opus",
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/webm": ".webm",
    "audio/mp4": ".mp4",
    "audio/aac": ".aac",
    "audio/flac": ".flac",
}

SIMPLYFLEET_TABLE_CANDIDATES = [
    lambda db: f"{db}.simplyfleet_driver_backup",
    lambda db: f"{db}.driver_backup",
    lambda _db: "mnc_reports.simplyfleet_driver_backup",
]

SIMPLYFLEET_STATUS_COLUMNS = ["status","driver_status","account_status","state"]
SIMPLYFLEET_WHATSAPP_COLUMNS = ["whatsapp_number","whatsapp","wa_number","phone","phone_number"]
SIMPLYFLEET_BACKUP_DATE_COLUMNS = ["backup_date","snapshot_date","created_at","imported_at","updated_at","recorded_at"]
SIMPLYFLEET_CONTACT_ID_COLUMNS = [
    "xero_contact_id",
    "xero_contact_id_bjj",
    "xero_contact_id_hakki",
    "xero_contact_id_a49",
    "contact_id",
    "driver_contact_id",
    "bolt_contact_id",
]
SIMPLYFLEET_DRIVER_NAME_COLUMNS = ["full_name","driver_name","name"]
SIMPLYFLEET_PERSONAL_CODE_COLUMNS = ["personal_code", "personalcode"]
ACTIVE_STATUS_VALUES = {"active","enabled","live"}
DRIVER_PROFILE_FIELD_LABELS = {
    "linked_vehicles": "Vehicle",
    "asset_model": "Asset Model",
    "driver_status": "Driver Status",
    "status": "Status",
    "driver_tier": "Tier",
    "driver_segment": "Segment",
    "city": "City",
    "region": "Region",
    "hub": "Hub",
    "stage": "Lifecycle Stage",
    "manager": "Manager",
    "manager_name": "Manager",
    "email": "Email",
    "driver_email": "Email",
    "onboarding_date": "Onboarded",
    "last_trip_at": "Last Trip",
    "notes": "Notes",
    "rental_balance": "Rental Balance",
}
ROSTER_DETAIL_LABEL_WHITELIST = {
    "Vehicle",
    "Status",
    "Email",
    "Rental Balance",
    "Address",
    "Age",
    "Bolt Driver Id",
    "Bolt Phone",
    "Collection Agent",
    "Contact Next Of Kin",
    "Contract Start Date",
    "Dob",
    "Driver Id Photo Url",
    "Driver License Photo Url",
    "Gender",
    "Group",
    "License Code",
    "License Expiry Date",
    "Linked Company",
    "Linked Company Vehicle",
    "Name Next Of Kin",
    "Nationality",
    "Personal Code",
    "Prdp Expiry Date",
    "Prdp Number",
    "Sales Rep",
    "Website Xero Balance",
    "Whatsapp Number",
}
ROSTER_DETAIL_LABEL_BLOCKLIST = {
    "Backup Date",
    "Createdat",
    "Full Name",
    "Id",
    "Updatedat",
    "Xero Contact Id",
    "Xero Contact Id Bjj",
}
ROSTER_DETAIL_LABEL_SET = {
    re.sub(r"[^a-z0-9]+", "", label.lower()) for label in ROSTER_DETAIL_LABEL_WHITELIST
}
ROSTER_DETAIL_LABEL_BLOCK_SET = {
    re.sub(r"[^a-z0-9]+", "", label.lower()) for label in ROSTER_DETAIL_LABEL_BLOCKLIST
}
PROFILE_SILENT_KEYS = {
    "contact_ids",
    "display_name",
    "last_synced_at",
    "wa_id",
    "_source_table",
}

DRIVER_KPI_STATUS_COLUMNS = ["sf_status","driver_status","status","account_status","state"]
DRIVER_KPI_NAME_COLUMNS = ["name","driver_name","display_name"]
DRIVER_KPI_BACKUP_DATE_COLUMNS = ["report_date","snapshot_date","last_sync","updated_at","created_at"]

KPI_SUMMARY_TABLE = f"{MYSQL_DB}.driver_kpi_summary"
KPI_NAME_COLUMNS = ["driver_name", "name", "full_name"]
KPI_WHATSAPP_COLUMNS = ["wa_id", "whatsapp_number", "whatsapp", "phone", "phone_number", "contact_number"]
KPI_CONTACT_COLUMNS = [
    "xero_contact_id",
    "xero_contact_id_bjj",
    "xero_contact_id_hakki",
    "xero_contact_id_a49",
    "contact_id",
    "driver_contact_id",
    "account_id",
]
KPI_STATUS_COLUMNS = ["status", "portal_status", "sf_status", "driver_status"]
KPI_SYNC_COLUMNS = ["updated_at", "report_date", "snapshot_date", "backup_date", "created_at"]

DRIVER_KPI_CACHE_TTL_SECONDS = int(os.getenv("DRIVER_KPI_CACHE_TTL_SECONDS", "180"))
DRIVER_ORDER_STATS_CACHE_TTL_SECONDS = int(os.getenv("DRIVER_ORDER_STATS_CACHE_TTL_SECONDS", "300"))
DRIVER_ROSTER_CACHE_TTL_SECONDS = int(os.getenv("DRIVER_ROSTER_CACHE_TTL_SECONDS", "300"))
DRIVER_ROSTER_CACHE_MAX_ROWS = int(os.getenv("DRIVER_ROSTER_CACHE_MAX_ROWS", "4000"))
DRIVER_ROSTER_WARM_INTERVAL_SECONDS = int(os.getenv("DRIVER_ROSTER_WARM_INTERVAL_SECONDS", str(DRIVER_ROSTER_CACHE_TTL_SECONDS or 30)))
DRIVER_ROSTER_REFRESH_STATUSES_ON_CACHE_HIT = os.getenv("DRIVER_ROSTER_REFRESH_STATUSES_ON_CACHE_HIT", "0") == "1"
DRIVER_DETAIL_CACHE_TTL_SECONDS = int(os.getenv("DRIVER_DETAIL_CACHE_TTL_SECONDS", "300"))
ACCOUNT_STATEMENT_CACHE_TTL_SECONDS = int(os.getenv("ACCOUNT_STATEMENT_CACHE_TTL_SECONDS", "300"))
DRIVER_DETAIL_STATEMENT_LIMIT = int(os.getenv("DRIVER_DETAIL_STATEMENT_LIMIT", "50"))
ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_DEFAULT = int(os.getenv("ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_DEFAULT", "50"))
ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_STEP = int(os.getenv("ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_STEP", "50"))
ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX = int(os.getenv("ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX", "500"))
ADMIN_DRIVER_DETAIL_KPI_TREND_DAYS = int(os.getenv("ADMIN_DRIVER_DETAIL_KPI_TREND_DAYS", "7"))
DRIVER_PORTAL_STATEMENT_LIMIT_DEFAULT = int(os.getenv("DRIVER_PORTAL_STATEMENT_LIMIT_DEFAULT", "100"))
DRIVER_PORTAL_STATEMENT_LIMIT_STEP = int(os.getenv("DRIVER_PORTAL_STATEMENT_LIMIT_STEP", "100"))
DRIVER_PORTAL_STATEMENT_LIMIT_MAX = int(os.getenv("DRIVER_PORTAL_STATEMENT_LIMIT_MAX", "500"))
DRIVER_PORTAL_STATEMENT_EXPORT_LIMIT = int(
    os.getenv("DRIVER_PORTAL_STATEMENT_EXPORT_LIMIT", str(DRIVER_PORTAL_STATEMENT_LIMIT_MAX))
)
ADMIN_DRIVER_DETAIL_TIMING = os.getenv("ADMIN_DRIVER_DETAIL_TIMING", "1") == "1"

# Bank details mapped by model patterns
BANK_DETAILS_BY_MODEL = [
    # Put the most specific patterns first to avoid generic matches swallowing them.
    (("bls",), "First National Bank", "63161930102", "250655"),
    (("hakki",), "First National Bank", "63140644279", "250655"),
    (("qute", "qute - new"), "First National Bank", "63041571472", "250655"),
    (("dzire", "micra", "almera", "s-presso", "vitz"), "First National Bank", "63029834793", "250655"),
]
_driver_kpi_cache_lock = threading.Lock()
_driver_kpi_cache: Dict[str, Tuple[float, Optional[Dict[str, Any]], Optional[str]]] = {}

_driver_order_stats_cache_lock = threading.Lock()
_driver_order_stats_cache: Dict[Tuple[str, ...], Tuple[float, Optional[Dict[str, Any]], Optional[str]]] = {}

_driver_roster_cache_lock = threading.Lock()
_driver_roster_cache: Dict[str, Any] = {
    "expiry": 0.0,
    "drivers": [],
    "max_rows": 0,
    "collections": [],
    "driver_types": [],
    "payer_types": [],
}

_driver_detail_cache_lock = threading.Lock()
_driver_detail_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}

_account_statement_cache_lock = threading.Lock()
_account_statement_cache: Dict[Tuple[str, int], Tuple[float, List[Dict[str, Any]]]] = {}

_engagement_preview_cache_lock = threading.Lock()
_engagement_preview_cache: Dict[str, Dict[str, Any]] = {}
_engagement_send_progress_lock = threading.Lock()
_engagement_send_progress: Dict[str, Dict[str, Any]] = {}


def _payer_badge(xero_balance: Optional[float], payments_total: float, yday_balance: Optional[float], rental_balance: Optional[float]) -> Tuple[Optional[str], Optional[str]]:
    if xero_balance is None:
        return None, None
    xb = _coerce_float(xero_balance) or 0.0
    # Ignore payments/yday/rental for badge; use xero balance thresholds
    if xb > 8700:
        return "ICU", "alert"
    if xb < 500:
        return "Good standing", "good"
    if 500.01 <= xb <= 2900:
        return "1 week arrears", "warn"
    if 2900.01 <= xb <= 5800:
        return "2 weeks arrears", "alert"
    if 5800.01 <= xb <= 8699.99:
        return "3 weeks arrears", "alert"
    return None, None


def _avg(values: list[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)

DRIVER_KPI_TABLE_CANDIDATES = [
    lambda db: f"{db}.driver_kpi_summary",
]

DRIVER_KPI_WHATSAPP_COLUMNS = ["whatsapp_number","whatsapp","wa_id","driver_phone","phone","phone_number"]
DRIVER_KPI_STATUS_COLUMNS = ["sf_status","status","driver_status","portal_status"]
DRIVER_KPI_NAME_COLUMNS = ["display_name","name","driver_name","full_name"]
DRIVER_KPI_DATE_COLUMNS = ["report_date","last_update_at","updated_at","created_at"]
DRIVER_KPI_MERGE_KEYS = [
    "driver_id",
    "online_hours",
    "trip_count",
    "gross_earnings",
    "earnings_per_hour",
    "acceptance_rate",
    "xero_balance",
    "payments",
    "bolt_wallet_payouts",
    "yday_wallet_balance",
    "total_trip_distance",
    "total_tracking_distance",
    "rental_balance",
]

DRIVER_ROSTER_WARM_LIMIT = int(os.getenv("DRIVER_ROSTER_WARM_LIMIT", "0"))

def _driver_kpi_cache_key(wa_id: str, driver: Dict[str, Any], *, include_today: bool = True) -> str:
    if wa_id:
        key = wa_id
    else:
        driver_id = driver.get("driver_id") or driver.get("bolt_driver_id")
        if driver_id:
            key = f"driver_id:{driver_id}"
        else:
            key = ""
    if not key:
        return ""
    if include_today:
        return key
    return f"{key}:no_today"

def _normalize_contact_ids(contact_ids: List[str]) -> List[str]:
    return [str(cid).strip() for cid in (contact_ids or []) if cid and str(cid).strip()]

def _detect_hotspot_scope(text: str) -> str:
    if not text:
        return HOTSPOT_SCOPE_DEFAULT
    for pattern in HOTSPOT_SCOPE_GLOBAL_PATTERNS:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return "global"
    return HOTSPOT_SCOPE_DEFAULT

def _detect_hotspot_timeframe(text: str) -> str:
    text = text or ""
    for key, patterns in HOTSPOT_TIMEFRAME_PATTERNS:
        for pattern in patterns:
            if re.search(pattern, text, flags=re.IGNORECASE):
                return key
    return HOTSPOT_TIMEFRAME_DEFAULT

def _timeframe_label(key: str) -> str:
    return HOTSPOT_LABELS.get(key, HOTSPOT_LABELS[HOTSPOT_TIMEFRAME_DEFAULT])

def _resolve_time_range(timeframe: str) -> Tuple[datetime, datetime, str]:
    key = timeframe if timeframe in HOTSPOT_LABELS else HOTSPOT_TIMEFRAME_DEFAULT
    now = jhb_now()
    now = now.replace(microsecond=0)

    if key == "this_week":
        start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
        end = start + timedelta(days=7)
    elif key == "last_week":
        this_week_start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0)
        start = this_week_start - timedelta(days=7)
        end = this_week_start
    elif key == "this_month":
        start = now.replace(day=1, hour=0, minute=0, second=0)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
    elif key == "last_month":
        this_month_start = now.replace(day=1, hour=0, minute=0, second=0)
        if this_month_start.month == 1:
            start = this_month_start.replace(year=this_month_start.year - 1, month=12)
        else:
            start = this_month_start.replace(month=this_month_start.month - 1)
        end = this_month_start
    else:
        start = now.replace(hour=0, minute=0, second=0)
        end = start + timedelta(days=1)

    return start, end, _timeframe_label(key)

KPI_INTENT_KEYWORDS = {
    "performance_summary": [
        "performance summary", "weekly performance", "my stats",
        "kpi summary", "how am i doing", "performance report", "driver kpi",
        "performance analytics", "driver analytics", "kpi analytics",
        "performance data", "performance dashboard", "driver scorecard",
        "kpi scorecard", "performance overview", "my kpis", "kpi report"
    ],
    "progress_update": [
        "track my progress", "track progress", "tracking my progress", "progress update",
        "daily update", "daily progress", "weekly progress", "how am i tracking",
        "progress this week", "weekly update", "update on my progress", "progress report",
        "track my performance", "track performance", "help me track", "help me track my performance",
        "help me track my progress", "keep me updated", "update me", "performance update",
        "update my performance", "track my kpis", "bi-hourly update", "bi hourly update",
        "bi-hourly updates", "bi hourly updates", "bi-hourly", "bi hourly",
        "every 2 hours", "every two hours"
    ],
    "daily_target_status": [
        "trips today", "today trips", "today's trips", "daily target", "today target",
        "finished orders today", "completed orders today", "orders today", "rides today",
        "how many trips today", "how many trips must i finish today", "how many orders today",
        "daily goal", "goal today", "daily trips target", "daily trip target"
    ],
    "online_hours_status": [
        "online hour", "hours online", "time online", "online time", "hours logged",
        "online hours target", "online hours progress", "hours remaining",
        "hours so far", "hours this week", "hours analytics"
    ],
    "acceptance_rate_status": [
        "acceptance rate", "acceptance%", "acceptance pct", "acceptance percentage", "accept rate",
        "acceptance analytics", "acceptance stats", "acceptance target",
        "acceptance kpi", "acceptance score"
    ],
    "earnings_per_hour_status": [
        "earnings per hour", "earning per hour", "rate per hour", "rph", "rands per hour",
        "hourly earnings", "hourly income", "income per hour", "earnings analytics per hour",
        "hourly analytics"
    ],
    "weekly_earnings_status": [
        "weekly earnings", "gross earnings", "weekly revenue", "total earnings", "gross this week",
        "weekly income", "weekly payout", "earnings analytics", "weekly performance"
    ],
    "trip_count_status": [
        "trip count", "trips this week", "rides this week", "number of trips", "how many trips",
        "trip analytics", "trip stats", "trip target", "trips progress", "rides target",
        "orders this week", "finished orders", "completed orders", "order count", "orders progress"
    ],
    "top_driver_tips": [
        "top driver tips", "improve performance", "hit targets", "boost my stats", "reach target", "help me improve",
        "improve kpis", "boost performance", "improve analytics", "hit my kpis"
    ],
    "hotspot_summary": [
        "hotspot", "hotspots", "global hotspots", "fleet hotspots",
        "hotspots today", "hotspots this week", "hotspots last week",
        "hotspots this month", "hotspots last month", "best areas", "busiest areas",
        "busy area", "busy areas", "busy zone", "busy zones", "where are the busy zones",
        "where are the hotspots", "hotspots near me", "busy places", "high demand areas",
        "hotspot analytics", "performance hotspots", "heatmap", "zone analytics",
        "best time", "best times", "best hours", "peak times", "peak hours",
        "busiest times", "when should i drive", "when to drive", "best time to drive",
        "best time to operate", "best hours to drive",
        "which areas should i focus on", "areas should i focus on", "focus on today",
        "high in demand", "high demand now", "where is demand", "where is it busy now",
        "busy now", "busy right now", "busy at the moment", "areas are busy", "areas are busy now",
        "areas are busy at the moment", "which areas are busy", "which areas are busy now",
        "which areas are busy at the moment", "what areas are busy", "where is it busy at the moment",
        "where should i work", "where should i drive", "where to work", "where to drive",
        "where can i work", "where can i drive", "which areas should i work",
        "which areas should i drive", "areas to work", "areas to drive",
        "best suburbs", "busy suburbs", "high demand suburbs", "which suburbs"
    ],
}

KPI_INTENTS = set(KPI_INTENT_KEYWORDS.keys())

MODEL_TARGETS = {
    "bajaj qute":       {"online_hours": 55, "acceptance_rate": 80, "earnings_per_hour": 130, "gross_earnings": 7150, "trip_count": 165},
    "toyota vitz":      {"online_hours": 55, "acceptance_rate": 80, "earnings_per_hour": 150, "gross_earnings": 8250, "trip_count": 105},
    "suzuki s-presso":  {"online_hours": 55, "acceptance_rate": 80, "earnings_per_hour": 150, "gross_earnings": 8250, "trip_count": 105},
    "suzuki dzire":     {"online_hours": 55, "acceptance_rate": 80, "earnings_per_hour": 150, "gross_earnings": 8250, "trip_count": 105},
    "nissan micra":     {"online_hours": 55, "acceptance_rate": 80, "earnings_per_hour": 150, "gross_earnings": 8250, "trip_count": 105},
    "nissan almera":    {"online_hours": 55, "acceptance_rate": 80, "earnings_per_hour": 150, "gross_earnings": 8250, "trip_count": 105},
}

DEFAULT_TARGETS = {"online_hours": 55, "acceptance_rate": 80, "earnings_per_hour": 140, "gross_earnings": 8000, "trip_count": 110}

def _normalize_model_key(asset_model: str) -> str:
    return (asset_model or "").strip().lower()

def _normalize_text(text: str) -> str:
    if not text:
        return ""
    return (
        text.replace("\u2019", "'")
        .replace("\u2018", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u00a0", " ")
    )

FUEL_PRICE_RANDS = float(_env("FUEL_PRICE_RANDS", "21.63") or "21.63")
MODEL_FUEL_EFFICIENCY_KM_PER_L = {
    "bajaj qute": 25.0,
    "toyota vitz": 18.0,
    "suzuki s-presso": 18.5,
    "suzuki dzire": 19.0,
    "nissan micra": 15.5,
    "nissan almera": 13.5,
}
DEFAULT_FUEL_EFFICIENCY_KM_PER_L = 16.0
FUEL_INTENT_KEYWORDS = [
    "fuel cost", "fuel estimate", "petrol cost", "petrol need", "fuel needed", "petrol needed",
    "fuel budget", "petrol budget", "fuel for the week", "petrol for the week", "how much petrol",
    "how much fuel", "fuel usage", "petrol usage", "fuel litres", "petrol litres",
]
EARNINGS_INTENT_KEYWORDS = [
    "earn if i do", "earnings if i do", "how much will i earn", "earnings for", "trip earnings",
    "per trip earnings", "earnings projection", "income if i do", "revenue if i do", "what will i make",
    "how much money from", "gross if i do", "earnings per trip is", "i make per trip", "trip income",
]
EARNINGS_TIPS_KEYWORDS = [
    "tips to increase my earnings", "tips to increase earnings", "tips for earnings", "tips to earn more",
    "advice to earn more", "how to earn more", "help me earn more", "increase my earnings", "boost my earnings",
    "make more money", "increase income", "improve earnings", "raise my earnings", "earn more",
]

PENDING_EARNINGS_TRIPS = "earnings_projection_trips"
PENDING_EARNINGS_AVG = "earnings_projection_avg"
PENDING_REPOSSESSION_REASON = "repossession_reason"
PENDING_NO_VEHICLE_REASON = "no_vehicle_reason"
NO_VEHICLE_PROMPT_COOLDOWN_SECONDS = 60 * 30

def get_model_targets(asset_model: str) -> Dict[str, float]:
    key = _normalize_model_key(asset_model)
    for model, targets in MODEL_TARGETS.items():
        if model in key:
            return targets
    return DEFAULT_TARGETS

def get_model_efficiency(asset_model: str) -> float:
    key = _normalize_model_key(asset_model)
    for model, eff in MODEL_FUEL_EFFICIENCY_KM_PER_L.items():
        if model in key:
            return eff
    return DEFAULT_FUEL_EFFICIENCY_KM_PER_L

def _extract_distance_km(text: str) -> Optional[float]:
    if not text:
        return None
    lowered = text.lower()
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:km|kilometer|kilometre|kilometers|kilometres|kms)", lowered)
    if match:
        try:
            value = float(match.group(1))
            if value > 0:
                return value
        except Exception:
            pass
    match = re.search(r"distance\s*(?:of|about|around)?\s*(\d+(?:\.\d+)?)", lowered)
    if match:
        try:
            value = float(match.group(1))
            if value > 0:
                return value
        except Exception:
            pass
    return None

def _extract_trip_count(text: str, allow_plain: bool = False) -> Optional[int]:
    if not text:
        return None
    lowered = text.lower()
    match = re.search(r"(\d+)\s*(?:trips|rides|deliveries|orders)", lowered)
    if match:
        try:
            value = int(match.group(1))
            if value > 0:
                return value
        except Exception:
            pass
    match = re.search(r"do\s+(\d+)", lowered)
    if match:
        try:
            value = int(match.group(1))
            if value > 0:
                return value
        except Exception:
            pass
    if allow_plain:
        if re.search(r"\b\d+(?:\.\d+)?\s*(?:km|kilometer|kilometre|kms)\b", lowered):
            if not re.search(r"\b(trip|trips|ride|rides|delivery|deliveries|order|orders|hour|hours|hr|hrs|h)\b", lowered):
                return None
        stripped = lowered.strip()
        if re.fullmatch(r"\d{1,4}", stripped):
            try:
                value = int(stripped)
                if value > 0:
                    return value
            except Exception:
                pass
        plain_numbers = re.findall(r"\d{1,4}", stripped)
        if len(plain_numbers) == 1:
            try:
                value = int(plain_numbers[0])
                if value > 0:
                    return value
            except Exception:
                pass
    return None

def _extract_hours_count(text: str, allow_plain: bool = False) -> Optional[float]:
    if not text:
        return None
    lowered = text.lower()
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:hours|hour|hrs|hr|h)\b", lowered)
    if match:
        try:
            value = float(match.group(1))
            if value > 0:
                return value
        except Exception:
            pass
    if allow_plain:
        if re.search(r"\b\d+(?:\.\d+)?\s*(?:km|kilometer|kilometre|kms)\b", lowered):
            if not re.search(r"\b(hour|hours|hrs|hr|h)\b", lowered):
                return None
        stripped = lowered.strip()
        if re.fullmatch(r"\d{1,3}(?:\.\d+)?", stripped):
            try:
                value = float(stripped)
                if value > 0:
                    return value
            except Exception:
                return None
    return None

def _mentions_hours(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"\b(hour|hours|hrs|hr|h)\b", text.lower()))

def _mentions_trips(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"\b(trip|trips|ride|rides|delivery|deliveries|order|orders)\b", text.lower()))

def _mentions_long_pickup_issue(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if re.search(r"\b\d+(?:\.\d+)?\s*(?:km|kms|kilometer|kilometre)s?\b", lowered):
        return True
    return any(
        kw in lowered
        for kw in [
            "far", "distance", "long pickup", "long trip", "too far", "fuel", "consumption",
            "dead km", "dead kms", "dead miles",
        ]
    )

def _first_number(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"\d+(?:\.\d+)?", text)
    return match.group(0) if match else None

def _needs_target_clarification(text: str) -> bool:
    if not text:
        return False
    lowered = text.strip().lower()
    if _mentions_hours(lowered) or _mentions_trips(lowered):
        return False
    return bool(re.search(r"\d", lowered))

def _min_target_thresholds() -> Tuple[int, int]:
    min_hours = int(round((ENGAGEMENT_TARGET_ONLINE_HOURS_MIN * GOAL_TARGET_MIN_RATIO) / 5.0) * 5)
    min_trips = int(round((ENGAGEMENT_TARGET_TRIPS * GOAL_TARGET_MIN_RATIO) / 10.0) * 10)
    return max(min_hours, 1), max(min_trips, 1)

def _extract_earnings_per_trip(text: str, allow_plain: bool = False) -> Optional[float]:
    if not text:
        return None
    lowered = text.lower()
    match = re.search(r"r?\s*(\d+(?:\.\d+)?)\s*(?:per\s*trip|per\s*ride|per\s*delivery|each\s*trip)", lowered)
    if match:
        try:
            return float(match.group(1))
        except Exception:
            pass
    match = re.search(r"average\s+earnings\s+per\s+trip\s+is\s+r?\s*(\d+(?:\.\d+)?)", lowered)
    if match:
        try:
            return float(match.group(1))
        except Exception:
            pass
    match = re.search(r"r\s*(\d+(?:\.\d+)?)", lowered)
    if match and "per trip" in lowered:
        try:
            return float(match.group(1))
        except Exception:
            pass
    if allow_plain:
        stripped = lowered.strip()
        simple = re.fullmatch(r"r?\s*(\d+(?:\.\d+)?)", stripped)
        if simple:
            try:
                return float(simple.group(1))
            except Exception:
                pass
        plain_numbers = re.findall(r"\d+(?:\.\d+)?", stripped)
        if len(plain_numbers) == 1:
            try:
                return float(plain_numbers[0])
            except Exception:
                pass
    return None

def _extract_week_reference(text: str) -> Optional[str]:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None
    lowered = cleaned.lower()
    if "week" in lowered or re.search(r"\bwk\s*\d{1,2}\b", lowered):
        return cleaned
    if re.search(r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b", lowered):
        return cleaned
    if re.search(r"\b\d{1,2}\s*(?:/|-)\s*\d{1,2}\s*(?:/|-)\s*\d{2,4}\b", lowered):
        return cleaned
    if re.search(r"\b\d{4}-\d{2}-\d{2}\b", lowered):
        return cleaned
    return None

TIME_OF_DAY_RE = re.compile(r"\b(?:by|before|at)\s*(\d{1,2})(?::(\d{2}))?\s*(am|pm)?\b", re.IGNORECASE)
PLAIN_TIME_RE = re.compile(r"\b(\d{1,2})(?::(\d{2}))\s*(am|pm)?\b", re.IGNORECASE)

def _extract_time_hour(text: str) -> Optional[int]:
    if not text:
        return None
    lowered = text.lower()
    if "noon" in lowered or "midday" in lowered:
        return 12
    if "midnight" in lowered:
        return 0
    match = TIME_OF_DAY_RE.search(lowered) or PLAIN_TIME_RE.search(lowered)
    if not match:
        return None
    hour = int(match.group(1))
    minute = int(match.group(2) or 0)
    ampm = (match.group(3) or "").lower()
    if hour > 24 or minute > 59:
        return None
    if ampm:
        if hour == 12:
            hour = 0
        if ampm == "pm":
            hour += 12
    if hour == 24:
        hour = 0
    return hour if 0 <= hour <= 23 else None

def _is_kpi_daily_target_query(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    has_trip_term = bool(re.search(r"\b(trip|trips|ride|rides|order|orders|finish|finished|complete|completed)\b", lowered))
    has_hour_term = _mentions_hours(lowered)
    has_today = "today" in lowered or "tonight" in lowered or "this morning" in lowered or "this afternoon" in lowered or "this evening" in lowered
    has_time = _extract_time_hour(lowered) is not None
    has_daily = "daily" in lowered or "per day" in lowered or "day target" in lowered or "day goal" in lowered
    has_goal_term = "goal" in lowered or "target" in lowered
    if has_daily and (has_trip_term or has_hour_term or has_goal_term):
        return True
    return (has_trip_term or has_hour_term) and (has_today or has_time)

def _is_remaining_kpi_query(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    remaining_terms = [
        "left", "remaining", "to go", "still need", "need to finish", "need to complete",
        "how many more", "how many left", "how many remaining",
    ]
    if not any(term in lowered for term in remaining_terms):
        return False
    if re.search(r"\b(trip|trips|ride|rides|order|orders|finish|finished|complete|completed|hour|hours|hrs|hr)\b", lowered):
        return True
    return False

def _match_kpi_intent(text: str) -> Optional[str]:
    for intent, keywords in KPI_INTENT_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                return intent
    return None


def _is_accident_message(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    for phrase in ACCIDENT_KEYWORDS:
        if phrase in lowered:
            return True
    tokens = set(re.findall(r"[a-z']+", lowered))
    if not (ACCIDENT_VERB_TERMS & tokens):
        return False
    if ACCIDENT_OBJECT_TERMS & tokens:
        return True
    return "accident" in lowered or "crash" in lowered or "collision" in lowered


def _interpret_medical_response(text: str) -> Tuple[Optional[bool], Optional[bool]]:
    if not text:
        return None, None
    lowered = text.lower()
    driver_ok: Optional[bool] = None
    medical_needed: Optional[bool] = None

    if any(phrase in lowered for phrase in INJURY_POSITIVE_KEYWORDS):
        driver_ok = False
        medical_needed = True
    elif any(phrase in lowered for phrase in INJURY_NEGATIVE_KEYWORDS):
        driver_ok = True
        medical_needed = False

    tokens = set(re.findall(r"[a-z']+", lowered))

    if driver_ok is None and medical_needed is None:
        yes_tokens = tokens & YES_TOKENS
        no_tokens = tokens & NO_TOKENS
        if yes_tokens and not no_tokens:
            medical_needed = True
            driver_ok = False
        elif no_tokens and not yes_tokens:
            driver_ok = True
            medical_needed = False

    if driver_ok is None and medical_needed is None:
        if {"yes", "ok"} <= tokens or {"yes", "okay"} <= tokens:
            driver_ok = True
            medical_needed = False

    if medical_needed is None and any(word in lowered for word in ["ambulance", "hospital", "paramedic", "doctor"]):
        medical_needed = True
    if driver_ok is None and any(word in lowered for word in ["fine", "ok", "okay", "alright", "safe"]):
        driver_ok = True

    return driver_ok, medical_needed


def _interpret_other_vehicle_response(text: str) -> Optional[bool]:
    if not text:
        return None
    lowered = text.lower()
    if any(phrase in lowered for phrase in NO_OTHER_VEHICLE_PHRASES):
        return False
    if any(phrase in lowered for phrase in OTHER_VEHICLE_POSITIVE_PHRASES):
        return True
    tokens = set(re.findall(r"[a-z']+", lowered))
    yes_tokens = tokens & YES_TOKENS
    no_tokens = tokens & NO_TOKENS
    if yes_tokens and not no_tokens:
        return True
    if no_tokens and not yes_tokens:
        return False
    return None


def _contains_vehicle_details(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if any(keyword in lowered for keyword in OTHER_VEHICLE_DETAIL_KEYWORDS):
        return True
    has_plate = bool(re.search(r"\b[A-Z]{2,}\s*\d{2,}[A-Z]{0,3}\b", text.upper()))
    has_mixed_digits = bool(re.search(r"\b\d{3,}\b", text)) and bool(re.search(r"[A-Za-z]", text))
    return has_plate or has_mixed_digits


def _contains_contact_details(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    digits = re.findall(r"\d", text)
    if len(digits) >= 7:
        return True
    keywords = {"name", "contact", "phone", "cell", "number", "email", "driver", "id"}
    if keywords & set(re.findall(r"[a-z']+", lowered)):
        return True
    return False


def _looks_like_address(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if ADDRESS_KEYWORDS & set(re.findall(r"[a-z']+", lowered)):
        return True
    if re.search(r"\d+\s+[a-z]", lowered):
        return True
    if re.search(r"\b(?:corner|cnr)\b", lowered) and re.search(r"\b(?:and|&)\b", lowered):
        return True
    return False

AREA_QUERY_PREFIXES = (
    "what about",
    "how about",
    "in",
    "near",
    "around",
    "at",
    "by",
)

def _is_name_question(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower().strip()
    if re.search(r"\b(what('?s| is)?\s+(your|ur)\s+name)\b", lowered):
        return True
    if re.search(r"\bwho\s+are\s+you\b", lowered):
        return True
    if re.search(r"\bwho\s+is\s+this\b", lowered):
        return True
    if re.search(r"\bwho\s+am\s+i\s+(talking|chatting|speaking)\s+to\b", lowered):
        return True
    if re.search(r"\byour\s+name\b", lowered):
        return True
    return False
AREA_GENERIC_WORDS = {
    "yes", "no", "ok", "okay", "thanks", "thank", "thank you", "hello", "hi",
    "what", "about", "in", "near", "around", "at", "by", "there", "here",
    "is", "are", "your", "name", "who", "this", "please",
    "hey",
    "i", "im", "i'm", "am", "me", "my", "mine", "we", "us", "our", "not",
    "a", "an", "the", "to", "from", "into", "onto", "toward", "towards", "via", "for", "of", "on",
    "busy", "busiest", "demand", "hot", "peak", "now", "today", "moment", "currently", "right",
    "area", "areas", "zone", "zones", "suburb", "suburbs", "hotspot", "hotspots", "place", "places",
    "trip", "trips", "ride", "rides", "order", "orders", "request", "requests", "job", "jobs",
    "booking", "bookings", "ping", "pings",
    "getting", "get", "work", "drive", "driving", "go", "going",
    "anywhere", "everywhere", "all", "over", "around", "want", "need", "looking", "seeking",
    "ticket", "case", "support", "ops",
    "tip", "tips",
}
LOW_DEMAND_AREA_STRIP_RE = r"\b(?:not|no|get(?:ting)?|trips?|rides?|orders?|requests?|jobs?|bookings?|demand|slow|quiet|dead|few)\b"

def _extract_area_candidates(text: str) -> List[str]:
    if not text:
        return []
    cleaned = _normalize_text(text).strip()
    if not cleaned:
        return []
    if _is_name_question(cleaned):
        return []
    lowered = cleaned.lower().strip(" ?!.,")
    candidate = cleaned
    for prefix in AREA_QUERY_PREFIXES:
        if lowered.startswith(prefix + " "):
            candidate = cleaned[len(prefix):].strip()
            break
    if not candidate:
        return []
    raw_parts = re.split(r"\s*(?:,|/| and )\s*", candidate, flags=re.IGNORECASE)
    results: List[str] = []
    for part in raw_parts:
        val = part.strip(" ?!.,")
        if not val:
            continue
        if val.lower() in AREA_GENERIC_WORDS:
            continue
        raw_tokens = [tok for tok in re.findall(r"[a-zA-Z0-9']+", val) if tok.lower() not in AREA_GENERIC_WORDS]
        if not raw_tokens:
            continue
        alpha_tokens = [tok for tok in raw_tokens if any(ch.isalpha() for ch in tok)]
        if not alpha_tokens:
            continue
        tokens: List[str] = []
        for tok in raw_tokens:
            if tok.lower() in AREA_GENERIC_WORDS:
                continue
            if any(ch.isalpha() for ch in tok):
                if len(tok) <= 2 and tok.lower() not in {"n"}:
                    continue
                tokens.append(tok)
            else:
                if alpha_tokens:
                    tokens.append(tok)
        if not tokens:
            continue
        if len(tokens) > 6:
            continue
        normalized = " ".join(tokens).strip().title()
        if normalized and normalized.lower() not in AREA_GENERIC_WORDS:
            results.append(normalized)
    return results

def _extract_low_demand_area_candidates(text: str) -> List[str]:
    if not text:
        return []
    cleaned = _normalize_text(text).strip()
    if not cleaned:
        return []
    results: List[str] = []
    for match in re.finditer(r"\b(?:in|near|around|by|at)\s+([^.;!?]+)", cleaned, flags=re.IGNORECASE):
        segment = match.group(1)
        segment = re.split(r"\b(?:because|since|as)\b", segment, flags=re.IGNORECASE)[0].strip()
        if segment:
            results.extend(_extract_area_candidates(segment))
    if results:
        return list(dict.fromkeys(results))
    raw_candidates = _extract_area_candidates(cleaned)
    for candidate in raw_candidates:
        if _is_low_demand_issue(candidate):
            scrubbed = candidate
            for phrase in LOW_DEMAND_EXPLICIT_PHRASES:
                scrubbed = re.sub(re.escape(phrase), " ", scrubbed, flags=re.IGNORECASE)
            scrubbed = re.sub(LOW_DEMAND_AREA_STRIP_RE, " ", scrubbed, flags=re.IGNORECASE)
            scrubbed = _normalize_text(scrubbed).strip()
            results.extend(_extract_area_candidates(scrubbed))
        else:
            results.append(candidate)
    return list(dict.fromkeys([r for r in results if r]))


def _next_accident_stage(case: Dict[str, Any]) -> str:
    if not case:
        return "safety"
    if case.get("driver_ok") is None or case.get("medical_needed") is None:
        return "safety"
    if not case.get("location_received"):
        return "location"
    other_vehicle = case.get("other_vehicle_involved")
    if other_vehicle is None:
        return "other_vehicle"
    if other_vehicle:
        if not case.get("vehicle_details"):
            return "vehicle_details"
        if not case.get("other_driver_details"):
            return "other_driver_details"
    return "wrapup"


def _is_vehicle_repossession(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if any(phrase in lowered for phrase in VEHICLE_REPOSSESSION_KEYWORDS):
        return True
    if re.search(r"\brepo(?:ed|ssess(?:ed|ion)?)?\b", lowered):
        return any(term in lowered for term in CAR_PROBLEM_VEHICLE_TERMS)
    return False

def _is_balance_dispute(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if not any(term in lowered for term in ["balance", "outstanding", "owe", "amount", "account"]):
        return False
    if any(kw in lowered for kw in BALANCE_DISPUTE_KEYWORDS):
        return True
    if re.search(r"\b(do\s*not|don't|dont|not)\s+owe\b", lowered):
        return True
    if re.search(r"\b(balance|outstanding|amount)\b.*\b(wrong|incorrect|not right|not correct|mistake|error)\b", lowered):
        return True
    return False


def _is_low_demand_issue(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if any(phrase in lowered for phrase in LOW_DEMAND_EXPLICIT_PHRASES):
        return True
    tokens = set(re.findall(r"[a-z']+", lowered))
    trip_terms = {"trip", "trips", "ride", "rides", "order", "orders", "request", "requests", "job", "jobs", "booking", "bookings", "ping", "pings"}
    demand_terms = {"slow", "quiet", "dead", "dry"}
    if (trip_terms & tokens) and (demand_terms & tokens):
        return True
    if ("low demand" in lowered or "no demand" in lowered) and (trip_terms & tokens):
        return True
    return False


def _is_everywhere_no_trips(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if not _is_low_demand_issue(lowered):
        return False
    if re.search(r"\b(everywhere|anywhere|wherever)\b", lowered):
        return True
    if re.search(r"\ball\s+(over|around)\b", lowered):
        return True
    if re.search(r"\b(every|all)\s+(area|areas|zone|zones|suburb|suburbs|place|places)\b", lowered):
        return True
    return False


def _is_ticket_request(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if re.search(r"\b(ticket|case)\b", lowered):
        if re.search(r"\b(open|log|raise|create|submit|file)\b", lowered):
            return True
        return True
    if re.search(r"\b(escalate|report|complain|complaint)\b", lowered):
        return True
    return False

def _is_hotspot_query(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    tokens = set(re.findall(r"[a-z']+", lowered))
    area_terms = {"area", "areas", "zone", "zones", "suburb", "suburbs", "hotspot", "hotspots", "place", "places"}
    demand_terms = {"busy", "busiest", "demand", "hot", "peak"}
    if ("where" in tokens or "which" in tokens) and (tokens & area_terms) and (tokens & demand_terms):
        return True
    if (tokens & area_terms) and (tokens & demand_terms):
        return True
    if "hotspot" in tokens or "hotspots" in tokens:
        return True
    if (tokens & demand_terms) and _extract_area_candidates(text):
        return True
    if "busy" in tokens and _extract_area_candidates(text):
        return True
    if "busy" in tokens and ("now" in tokens or "moment" in tokens or "today" in tokens):
        return True
    if _is_area_planning_query(text):
        return True
    return False


def _is_area_planning_query(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if not _extract_area_candidates(text):
        return False
    if re.search(r"\b(plan|planning|go|going|head|heading|work|working|drive|driving|operate|operating)\b", lowered):
        return True
    return False


def _is_oph_definition_question(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if "oph" not in lowered:
        return False
    if re.search(r"\b(what|whats|what's|mean|meaning|stands for|stand for|define|definition)\b", lowered):
        return True
    return lowered.strip() == "oph"


def _is_platform_exclusivity_question(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if re.search(r"\b(uber|in-?drive|indrive|ehail|e-hail|e hailing|e-hailing)\b", lowered):
        return True
    if re.search(r"\b(other|another)\s+(platform|app|e-?hailing)\b", lowered):
        return True
    if re.search(r"\boperate\s+on\b", lowered) or re.search(r"\bcan\s+i\s+use\b", lowered):
        return True
    return False

def _is_account_suspension(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if any(phrase in lowered for phrase in ACCOUNT_SUSPENSION_KEYWORDS):
        return True
    if re.search(r"\b(account|profile|bolt|platform)\b", lowered) and re.search(
        r"\b(suspend(?:ed|ion)?|block(?:ed)?|ban(?:ned)?|deactivat(?:ed|ion)?|disable(?:d)?)\b",
        lowered,
    ):
        return True
    return False


def _is_app_issue(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if any(phrase in lowered for phrase in APP_ISSUE_KEYWORDS):
        return True
    if "app" in lowered and re.search(r"\b(error|issue|problem|not working|crash|frozen|stuck|login|log in)\b", lowered):
        return True
    return False


def _is_medical_issue(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    tokens = set(re.findall(r"[a-z']+", lowered))
    if "i'll" in tokens:
        if re.search(r"\b(i'?m|im|i am|am|feeling|feel|very|so)\s+i'll\b", lowered) or re.search(
            r"\bi'll\s+(today|now|tonight|this\s+morning|this\s+week)\b", lowered
        ):
            tokens.add("ill")
    for kw in MEDICAL_ISSUE_KEYWORDS:
        if " " in kw:
            if kw in lowered:
                return True
        else:
            if kw in tokens:
                return True
            if re.search(rf"\b{re.escape(kw)}\b", lowered):
                return True
    return False


def _is_no_vehicle(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if any(kw in lowered for kw in NO_VEHICLE_KEYWORDS):
        return True
    if re.search(r"\b(no|without)\s+(a|the)?\s*(car|csr|vehicle|taxi|cab)\b", lowered):
        return True
    return bool(re.search(r"\b(do\s*not|don't|dont)\s+(have|got)?\s*(a|the)?\s*(car|csr|vehicle|taxi|cab)\b", lowered))


def _is_vehicle_back(text: str) -> bool:
    if not text:
        return False
    lowered = _normalize_text(text).lower()
    if any(kw in lowered for kw in VEHICLE_BACK_KEYWORDS):
        return True
    return bool(re.search(r"\b(got|have|collected|picked\s+up|received)\s+(the|a|my)?\s*(car|vehicle|replacement)\b", lowered))


def _parse_no_vehicle_reason(text: str) -> Optional[str]:
    if not text:
        return None
    lowered = text.lower()
    if _is_medical_issue(lowered):
        return "medical"
    if any(kw in lowered for kw in NO_VEHICLE_WORKSHOP_KEYWORDS):
        return "workshop"
    if any(kw in lowered for kw in NO_VEHICLE_ASSIGNMENT_KEYWORDS):
        return "assignment"
    if any(kw in lowered for kw in NO_VEHICLE_BLOCKED_KEYWORDS):
        return "blocked"
    if any(kw in lowered for kw in REPOSSESSION_BALANCE_KEYWORDS):
        return "balance"
    if any(kw in lowered for kw in ["something else", "other", "another", "different", "not sure", "unknown"]):
        return "other"
    return None


def _parse_repossession_reason(text: str) -> Optional[str]:
    if not text:
        return None
    lowered = text.lower()
    balance_hit = any(kw in lowered for kw in REPOSSESSION_BALANCE_KEYWORDS)
    behavior_hit = any(kw in lowered for kw in REPOSSESSION_BEHAVIOR_KEYWORDS)
    if "both" in lowered or (balance_hit and behavior_hit):
        return "both"
    if balance_hit:
        return "outstanding"
    if behavior_hit:
        return "behavior"
    return None


def _schedule_no_vehicle_checkin(ctx: Dict[str, Any], reason: Optional[str] = None) -> None:
    if not ctx:
        return
    ctx["_engagement_followup_paused"] = True
    ctx["_engagement_followup_paused_at"] = time.time()
    if reason:
        ctx["_engagement_followup_pause_reason"] = reason
    if not NO_VEHICLE_CHECKIN_ENABLED:
        return
    delay_hours = max(1.0, float(NO_VEHICLE_CHECKIN_DELAY_HOURS))
    ctx["_no_vehicle_checkin_due_at"] = time.time() + delay_hours * 3600.0
    ctx["_no_vehicle_checkin_pending"] = True
    if reason:
        ctx["_no_vehicle_checkin_reason"] = reason


def _is_car_problem(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    for phrase in CAR_PROBLEM_KEYWORDS:
        if phrase in lowered:
            return True
    vehicle_hit = any(term in lowered for term in CAR_PROBLEM_VEHICLE_TERMS)
    if not vehicle_hit:
        return False
    return any(term in lowered for term in CAR_PROBLEM_SYMPTOM_TERMS)

def _is_car_resolution_message(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if any(phrase in lowered for phrase in CAR_PROBLEM_RESOLUTION_KEYWORDS):
        return True
    if lowered.strip() in {"resolved", "fixed", "sorted"}:
        return True
    return False

def _is_positive_confirmation(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    tokens = set(re.findall(r"[a-z']+", lowered))
    return bool(tokens & YES_TOKENS) or lowered in {"yep", "yup", "sorted", "confirmed", "do it", "will do", "will do it"}

def _is_negative_confirmation(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    tokens = set(re.findall(r"[a-z']+", lowered))
    return bool(tokens & NO_TOKENS)

INTRADAY_OPT_IN_KEYWORDS = [
    "daily update",
    "daily updates",
    "bi-hourly",
    "bi hourly",
    "every 2 hours",
    "every two hours",
    "keep me updated",
    "update me",
    "track my progress",
    "track my performance",
    "track my kpis",
]

INTRADAY_STOP_PATTERNS = [
    r"\bstop\b.*\bupdates?\b",
    r"\bpause\b.*\bupdates?\b",
    r"\bno\s+more\s+updates?\b",
    r"\bopt\s*out\b",
    r"\bunsubscribe\b",
]

def _wants_intraday_updates(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    return any(kw in lowered for kw in INTRADAY_OPT_IN_KEYWORDS)

def _is_intraday_pause_request(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    return any(re.search(pat, lowered) for pat in INTRADAY_STOP_PATTERNS)

GLOBAL_OPT_OUT_PATTERNS = [
    r"\bleave\s+me\s+alone\b",
    r"\bstop\b.*\b(message|messages|messaging|text|texts|texting|contact|contacting)\b",
    r"\bdo\s+not\b.*\b(message|messages|contact|contacting)\b",
    r"\bdon't\b.*\b(message|messages|contact|contacting)\b",
    r"\bno\s+more\b.*\b(messages|texts|contact)\b",
    r"\bremove\s+me\b",
    r"\bopt\s*out\b",
    r"\bunsubscribe\b",
]

GLOBAL_OPT_IN_KEYWORDS = [
    "start",
    "resume",
    "opt in",
    "subscribe",
    "message me",
    "contact me",
]

def _is_global_opt_out(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    return any(re.search(pat, lowered) for pat in GLOBAL_OPT_OUT_PATTERNS)

def _is_global_opt_in(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    return any(kw in lowered for kw in GLOBAL_OPT_IN_KEYWORDS)

PEAK_TIME_PATTERNS = [
    r"\bpeak\s+time(s)?\b",
    r"\bpeak\s+hour(s)?\b",
    r"\bbusiest\s+time(s)?\b",
    r"\bbest\s+time\s+to\s+(drive|work|operate)\b",
    r"\bwhen\s+should\s+i\s+(drive|work|operate)\b",
    r"\brush\s+hour(s)?\b",
]

def _is_peak_time_query(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    return any(re.search(pat, lowered) for pat in PEAK_TIME_PATTERNS)

PERFORMANCE_TIP_PATTERNS = [
    r"\btip(s)?\b",
    r"\btime(s)?\b",
    r"\bhour(s)?\b",
    r"\barea(s)?\b",
    r"\bzone(s)?\b",
    r"\bhotspot(s)?\b",
    r"\bwhere\b",
    r"\bwhen\b",
]

def _is_performance_tip_request(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    return any(re.search(pat, lowered) for pat in PERFORMANCE_TIP_PATTERNS)

PERFORMANCE_PLAN_PATTERNS = [
    r"\bplan\b",
    r"\bschedule\b",
    r"\broutine\b",
    r"\bshift\b",
]

def _parse_hours_days_plan(text: str) -> Tuple[Optional[float], Optional[int]]:
    if not text:
        return None, None
    lowered = text.lower()
    hours_per_day = None
    days_per_week = None
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:hours|hour|hrs|hr)\s*(?:per|a)\s*day", lowered)
    if match:
        try:
            hours_per_day = float(match.group(1))
        except Exception:
            hours_per_day = None
    match = re.search(r"(?:over|for)\s*(\d{1,2})\s*(?:days|day)", lowered)
    if match:
        try:
            days_per_week = int(match.group(1))
        except Exception:
            days_per_week = None
    return hours_per_day, days_per_week

def _is_performance_plan_request(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    if any(re.search(pat, lowered) for pat in PERFORMANCE_PLAN_PATTERNS):
        return True
    hours_per_day, days_per_week = _parse_hours_days_plan(lowered)
    return hours_per_day is not None or days_per_week is not None

TARGET_BENEFIT_PATTERNS = [
    r"\bbenefit(s)?\b",
    r"\bwhy\b.*\b(?:hours|trips|targets?|goals?)\b",
    r"\bwhat\s+is\s+the\s+target\b",
    r"\bwhat\s+are\s+the\s+targets\b",
    r"\bwhat\s+target\b",
    r"\btarget\s+for\s+this\s+week\b",
    r"\bweekly\s+target\b",
    r"\bweekly\s+goals?\b",
    r"\bgoal\s+for\s+this\s+week\b",
    r"\bwhat(?:'s| is)\s+the\s+point\b",
    r"\bwhat\s+do\s+i\s+get\b",
    r"\bwhat\s+does\s+it\s+do\s+for\s+me\b",
    r"\bminimum\s+(?:hours|trips|target|goal)s?\b",
    r"\blowest\s+(?:hours|trips|target|goal)s?\b",
    r"\bminimum\s+can\s+i\s+target\b",
    r"\bwhy\s+(?:55|110)\b",
]

def _is_target_benefit_query(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower().strip()
    if any(re.search(pat, lowered) for pat in TARGET_BENEFIT_PATTERNS):
        return True
    if ("benefit" in lowered or "why" in lowered) and ("hour" in lowered or "trip" in lowered or "target" in lowered):
        return True
    if "minimum" in lowered and ("hour" in lowered or "trip" in lowered or "target" in lowered or "goal" in lowered):
        return True
    if "lowest" in lowered and ("hour" in lowered or "trip" in lowered or "target" in lowered or "goal" in lowered):
        return True
    if "target" in lowered and ("week" in lowered or "goal" in lowered):
        return True
    return False

def _has_goal_update_values(text: str, *, allow_plain: bool = False) -> bool:
    if not text:
        return False
    if _extract_trip_count(text, allow_plain=allow_plain) is not None:
        return True
    if _extract_hours_count(text, allow_plain=allow_plain) is not None:
        return True
    return False

def _coerce_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        try:
            return float(str(val).strip())
        except Exception:
            return None


def _coerce_pct(val: Any) -> Optional[float]:
    """Coerce percentage values that may include a trailing % sign."""
    if val is None:
        return None
    try:
        txt = str(val).strip()
        if txt.endswith("%"):
            txt = txt[:-1]
        return float(txt)
    except Exception:
        return _coerce_float(val)

def _normalize_acceptance(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if value <= 1.5:
        return round(value * 100, 1)
    return round(value, 1)


def _build_today_kpi_sections(kpi_metrics: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not kpi_metrics:
        return []

    def _fmt_int(val: Optional[float]) -> Optional[str]:
        if val is None:
            return None
        try:
            return str(int(round(float(val))))
        except Exception:
            return None

    def _fmt_pct(val: Optional[float], digits: int = 1) -> Optional[str]:
        if val is None:
            return None
        try:
            return f"{float(val):.{digits}f}%"
        except Exception:
            return None

    def _fmt_float(val: Optional[float], digits: int = 1) -> Optional[str]:
        if val is None:
            return None
        try:
            return f"{float(val):.{digits}f}"
        except Exception:
            return None

    sent = _coerce_float(kpi_metrics.get("today_trips_sent"))
    accepted = _coerce_float(kpi_metrics.get("today_trips_accepted"))
    finished = _coerce_float(kpi_metrics.get("today_finished_trips"))
    gmv = _coerce_float(kpi_metrics.get("today_gross_earnings"))
    cash_trips = _coerce_float(kpi_metrics.get("today_cash_trips"))
    card_trips = _coerce_float(kpi_metrics.get("today_card_trips"))
    cash_earn = _coerce_float(kpi_metrics.get("today_cash_earnings"))
    card_earn = _coerce_float(kpi_metrics.get("today_card_earnings"))
    dist_sum = _coerce_float(kpi_metrics.get("today_ride_distance_km"))
    dist_avg = _coerce_float(kpi_metrics.get("today_avg_ride_distance_km"))
    tips_sum = _coerce_float(kpi_metrics.get("today_tip_total"))

    accept_pct = None
    if sent:
        accept_pct = _normalize_acceptance((accepted or 0.0) / sent)

    avg_fare = None
    if finished and finished > 0 and gmv is not None:
        try:
            avg_fare = gmv / finished
        except Exception:
            avg_fare = None

    payment_mix_pct = None
    if sent:
        payment_mix_pct = {
            "cash_pct": (cash_trips or 0.0) / sent * 100.0,
            "card_pct": (card_trips or 0.0) / sent * 100.0,
        }

    today_sections: list[dict[str, Any]] = []

    efficiency_items = []
    for label, value in [
        ("Trips Sent", _fmt_int(sent)),
        ("Trips Accepted", _fmt_int(accepted)),
        ("Acceptance Rate", _fmt_pct(accept_pct)),
        ("Finished Trips", _fmt_int(finished)),
    ]:
        if value is not None:
            efficiency_items.append({"label": label, "value": value})
    if efficiency_items:
        today_sections.append({"title": "Efficiency", "items": efficiency_items})

    distance_items = []
    if dist_sum is not None:
        distance_items.append({"label": "Ride Distance", "value": f"{dist_sum:.1f} km"})
    if dist_avg is not None:
        distance_items.append({"label": "Avg Ride Distance", "value": f"{dist_avg:.1f} km"})
    if distance_items:
        today_sections.append({"title": "Distances", "items": distance_items})

    financial_items = []
    if gmv is not None:
        financial_items.append({"label": "Gross Earnings", "value": fmt_rands(gmv)})
    if tips_sum is not None:
        financial_items.append({"label": "Tips", "value": fmt_rands(tips_sum)})
    if avg_fare is not None:
        financial_items.append({"label": "Avg Fare", "value": fmt_rands(avg_fare)})
    if financial_items:
        today_sections.append({"title": "Financial", "items": financial_items})

    payment_items = []
    for label, value in [
        ("Cash Trips", _fmt_int(cash_trips)),
        ("Card Trips", _fmt_int(card_trips)),
        ("Cash Earnings", fmt_rands(cash_earn) if cash_earn is not None else None),
        ("Card Earnings", fmt_rands(card_earn) if card_earn is not None else None),
    ]:
        if value is not None:
            payment_items.append({"label": label, "value": value})
    if payment_mix_pct:
        cash_pct = _fmt_pct(payment_mix_pct.get("cash_pct"))
        card_pct = _fmt_pct(payment_mix_pct.get("card_pct"))
        if cash_pct:
            payment_items.append({"label": "Cash Trips %", "value": cash_pct})
        if card_pct:
            payment_items.append({"label": "Card Trips %", "value": card_pct})
    if payment_items:
        today_sections.append({"title": "Payment Mix", "items": payment_items})

    return today_sections

def _query_driver_kpis(
    wa_id: str,
    driver: Dict[str, Any],
    *,
    include_today: bool = True,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not mysql_available():
        return None, "database connection not configured"
    driver_id = driver.get("driver_id") or driver.get("bolt_driver_id")
    if driver_id:
        try:
            driver_id = int(driver_id)
        except Exception:
            driver_id = None

    contact_ids = [str(cid).strip() for cid in (driver.get("xero_contact_ids") or []) if cid and str(cid).strip()]
    schema = MYSQL_DB or "mnc_report"
    table = f"{schema}.driver_kpi_summary"
    contact_columns = ["xero_contact_id", "xero_contact_id_bjj", "xero_contact_id_hakki", "xero_contact_id_a49"]
    placeholders = ", ".join(["%s"] * len(contact_ids)) if contact_ids else ""
    contact_filter = " OR ".join(f"sd.{col} IN ({placeholders})" for col in contact_columns) if contact_ids else ""

    def _parse_pct(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            text = str(value).strip()
            if text.endswith("%"):
                text = text[:-1]
            return float(text)
        except Exception:
            return None

    conn = None
    try:
        conn = get_mysql()

        # 1) Resolve driver identity. Prefer explicit driver_id (Bolt) when available, otherwise fall back to contact IDs.
        identity_row: Optional[Dict[str, Any]] = None
        if driver_id is not None:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        f"SELECT * FROM {schema}.driver_kpi_summary WHERE driver_id=%s ORDER BY report_date DESC LIMIT 1",
                        (driver_id,),
                    )
                    identity_row = cur.fetchone()
            except Exception as exc:
                log.warning("driver_kpi_summary identity lookup failed: %s", exc)

        if identity_row is None and contact_ids:
            id_params: List[Any] = []
            for _ in contact_columns:
                id_params.extend(contact_ids)
            id_sql = f"""
                SELECT
                  COALESCE(bd.id, sd.id) AS driver_id,
                  MAX(bd.name) AS name,
                  MAX(sd.personal_code) AS personal_code,
                  COALESCE(MAX(bd.car_reg_number), MAX(sd.linked_vehicles)) AS car_reg_number,
                  MAX(sf.asset_model) AS model,
                  MAX(sd.status) AS sf_status,
                  MAX(bd.portal_status) AS portal_status,
                  MAX(sd.contract_start_date) AS contract_start_date,
                  MAX(bd.phone) AS phone,
                  MAX(bd.email) AS email,
                  MAX(sd.xero_contact_id) AS xero_contact_id
                FROM {schema}.simplyfleet_driver sd
                LEFT JOIN {schema}.bolt_drivers bd
                  ON bd.mapped_simplyfleet_id = sd.id
                LEFT JOIN {schema}.simplyfleet sf
                  ON REGEXP_REPLACE(UPPER(COALESCE(bd.car_reg_number, sd.linked_vehicles, '')), '[^0-9A-Z]', '') =
                     REGEXP_REPLACE(UPPER(sf.registration_number), '[^0-9A-Z]', '')
                WHERE ({contact_filter})
                GROUP BY COALESCE(bd.id, sd.id)
                ORDER BY MAX(sd.contract_start_date) DESC
                LIMIT 1
            """
            if LOG_DB_INSERTS:
                log.info("[KPI] identity_sql=%s params=%s", id_sql.replace("%", "%%"), id_params)
            with conn.cursor() as cur:
                cur.execute(id_sql, id_params)
                identity_row = cur.fetchone()

        if not identity_row or not identity_row.get("driver_id"):
            # Fallback: try phone match on wa_id variants in driver_kpi_summary
            variants = _wa_number_variants(wa_id)
            if not variants:
                clean = re.sub(r"\D", "", wa_id or "")
                if clean:
                    variants = [clean]
            if variants:
                phone_col = _pick_col_exists(conn, table, ["phone", "wa_id", "whatsapp_number", "whatsapp", "contact_number", "driver_phone"])
                if phone_col:
                    sanitized_expr = _sanitize_phone_expr(phone_col)
                    placeholders = ", ".join(["%s"] * len(variants))
                    sql = (
                        f"SELECT * FROM {table} "
                        f"WHERE {sanitized_expr} IN ({placeholders}) "
                        f"ORDER BY report_date DESC LIMIT 1"
                    )
                    with conn.cursor() as cur:
                        cur.execute(sql, variants)
                        identity_row = cur.fetchone()

        if not identity_row or not identity_row.get("driver_id"):
            bolt_driver_id = _lookup_bolt_driver_id_by_wa(conn, wa_id)
            if bolt_driver_id:
                if identity_row is None:
                    identity_row = {}
                identity_row["driver_id"] = bolt_driver_id

        if not identity_row or not identity_row.get("driver_id"):
            if driver_id is None and not contact_ids:
                return None, "no driver identifiers available"
            return None, "no driver matched the provided identifiers"

        driver_id = identity_row.get("driver_id")

        # If contact IDs are missing but the identity row carries one, keep it for downstream queries.
        if not contact_ids:
            possible_contact = identity_row.get("xero_contact_id") or identity_row.get("contact_id")
            if possible_contact:
                contact_ids.append(str(possible_contact))

        # 2) Latest 7d KPI snapshot from driver_kpi_summary
        river_sql = f"""
            SELECT *
            FROM {schema}.driver_kpi_summary
            WHERE driver_id = %s
            ORDER BY report_date DESC
            LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(river_sql, (driver_id,))
            river_row = cur.fetchone()
        if not river_row:
            return None, "no driver_kpi_summary records matched the driver"

        today_row: Dict[str, Any] = {}
        if include_today:
            # 3) Today’s performance from bolt_orders_new (status, payment mix, distance, duration, tips)
            finished_values = ", ".join([f"'{s.lower()}'" for s in FINISHED_STATUS_VALUES])
            today_sql = f"""
                SELECT
                  COUNT(*) AS trips_sent,
                  SUM(CASE WHEN LOWER(status) LIKE '%%accept%%' OR LOWER(status) LIKE '%%finish%%' THEN 1 ELSE 0 END) AS trips_accepted,
                  SUM(CASE WHEN LOWER(status) IN ('finished','completed','done','success','successful') THEN 1 ELSE 0 END) AS finished_trips,
                  SUM(
                    COALESCE(
                      total_price,
                      COALESCE(fare_finalised,0)
                        + COALESCE(cancellation_fee,0)
                        + COALESCE(booking_fee,0)
                        + COALESCE(toll_fee,0)
                        + COALESCE(tip,0)
                    )
                  ) AS total_gmv,
                  SUM(CASE WHEN payment_method = 'cash' THEN 1 ELSE 0 END) AS cash_trips,
                  SUM(CASE WHEN payment_method = 'in_app' THEN 1 ELSE 0 END) AS card_trips,
                  SUM(CASE WHEN payment_method = 'cash' THEN
                        COALESCE(total_price, COALESCE(fare_finalised,0) + COALESCE(cancellation_fee,0) + COALESCE(booking_fee,0) + COALESCE(toll_fee,0) + COALESCE(tip,0))
                    ELSE 0 END) AS cash_gmv,
                  SUM(CASE WHEN payment_method = 'in_app' THEN
                        COALESCE(total_price, COALESCE(fare_finalised,0) + COALESCE(cancellation_fee,0) + COALESCE(booking_fee,0) + COALESCE(toll_fee,0) + COALESCE(tip,0))
                    ELSE 0 END) AS card_gmv,
                  SUM(CASE WHEN LOWER(status) IN ({finished_values}) THEN ride_distance ELSE 0 END) AS ride_distance_sum,
                  AVG(CASE WHEN LOWER(status) IN ({finished_values}) THEN ride_distance END) AS ride_distance_avg,
                  SUM(ride_duration) AS ride_duration_sum,
                  SUM(pickup_duration) AS pickup_duration_sum,
                  SUM(tip) AS tip_sum
                FROM {schema}.bolt_orders_new
                WHERE driver_id = %s
                  AND COALESCE(driver_assigned_time, driver_assigned_timestamp) >= CURDATE()
                  AND COALESCE(driver_assigned_time, driver_assigned_timestamp) <  CURDATE() + INTERVAL 1 DAY
                  AND (company_id IS NULL OR company_id <> 96165)
            """
            with conn.cursor() as cur:
                cur.execute(today_sql, (driver_id,))
                today_row = cur.fetchone() or {}

        total_trips = _coerce_float(river_row.get("total_trips_sent"))
        total_accepted = _coerce_float(river_row.get("total_trips_accepted")) or 0.0
        acceptance_rate = None
        if total_trips:
            acceptance_rate = _normalize_acceptance(total_accepted / total_trips)
        elif river_row.get("acceptance_pct") is not None:
            acceptance_rate = _parse_pct(river_row.get("acceptance_pct"))

        report_date = river_row.get("report_date")
        period_label = None
        try:
            if report_date:
                if isinstance(report_date, str):
                    try:
                        report_date_dt = datetime.strptime(report_date[:10], "%Y-%m-%d")
                    except Exception:
                        report_date_dt = None
                else:
                    report_date_dt = report_date
                if report_date_dt:
                    start = report_date_dt - timedelta(days=6)
                    period_label = f"{start.strftime('%d %b %Y')} - {report_date_dt.strftime('%d %b %Y')}"
        except Exception:
            period_label = None
        period_label = period_label or "Last 7 days"
        updated_at = jhb_now().strftime("%Y-%m-%d %H:%M:%S")

        payments_raw = None
        for key in ("payments", "7D_payments", "total_payments"):
            val = river_row.get(key)
            if val not in (None, ""):
                payments_raw = val
                break

        finished_trips = _coerce_float(river_row.get("total_finished_orders"))
        if finished_trips is None:
            finished_trips = _coerce_float(river_row.get("finished_trips"))

        metrics = {
            "contact_ids": contact_ids,
            "driver_id": driver_id,
            "fleet_id": river_row.get("fleet_id"),
            "display_name": identity_row.get("name") or river_row.get("name"),
            "personal_code": identity_row.get("personal_code") or river_row.get("personal_code"),
            "car_reg_number": identity_row.get("car_reg_number") or river_row.get("car_reg_number"),
            "model": river_row.get("model"),
            "sf_status": river_row.get("sf_status") or identity_row.get("sf_status"),
            "portal_status": river_row.get("portal_status") or identity_row.get("portal_status"),
            "contract_start_date": river_row.get("contract_start_date") or identity_row.get("contract_start_date"),
            "phone": river_row.get("phone") or identity_row.get("phone"),
            "email": river_row.get("email") or identity_row.get("email"),
            "active_days": int(float(river_row.get("active_days") or 0)),
            # 7d snapshot (driver_kpi_summary)
            "online_hours": _coerce_float(river_row.get("total_online_hours")),
            "total_online_hours": _coerce_float(river_row.get("total_online_hours")),
            "total_gmv": _coerce_float(river_row.get("total_gmv")),
            "gross_earnings": _coerce_float(river_row.get("total_gmv")),
            "finished_trips": finished_trips,
            "total_finished_orders": finished_trips,
            "eph": _coerce_float(river_row.get("eph")),
            "rph": _coerce_float(river_row.get("rph")),
            "online_per_day": _coerce_float(river_row.get("online_per_day")),
            "gmv_per_day": _coerce_float(river_row.get("gmv_per_day")),
            "avg_trip_distance": _coerce_float(river_row.get("avg_trip_distance")),
            "total_trip_distance": _coerce_float(river_row.get("total_trip_distance")),
            "total_tracking_distance": _coerce_float(river_row.get("total_tracking_distance")),
            "trip_distance_pct": river_row.get("trip_distance_pct"),
            "xero_balance": _coerce_float(river_row.get("xero_balance")),
            "payments": _coerce_float(payments_raw),
            "first_payment_date": river_row.get("first_payment_date"),
            "first_payment_amount": _coerce_float(river_row.get("first_payment_amount")),
            "bolt_wallet_payouts": _coerce_float(river_row.get("bolt_wallet_payouts")),
            "yday_wallet_balance": _coerce_float(river_row.get("yday_wallet_balance")),
            "adjustments": _coerce_float(river_row.get("adjustments")),
            "cash_rides_7d": _coerce_float(river_row.get("cash_rides_7d")),
            "acc_gmv_pct": _coerce_pct(river_row.get("acc_gmv_pct")),
            "card_payment_pct": _coerce_pct(river_row.get("card_payment_pct")),
            "cash_payment_pct": _coerce_pct(river_row.get("cash_payment_pct")),
            "card_trips_sent": _coerce_float(river_row.get("card_trips_sent")),
            "card_trips_accepted": _coerce_float(river_row.get("card_trips_accepted")),
            "card_accepted_pct": _coerce_pct(river_row.get("card_accepted_pct")),
            "cash_trips_sent": _coerce_float(river_row.get("cash_trips_sent")),
            "cash_trips_accepted": _coerce_float(river_row.get("cash_trips_accepted")),
            "cash_accepted_pct": _coerce_pct(river_row.get("cash_accepted_pct")),
            "total_trips_sent": total_trips,
            "total_trips_accepted": total_accepted,
            "acceptance_rate": acceptance_rate,
            "period_label": period_label,
            "updated_at": updated_at,
            "source_table": "driver_kpi_summary",
            # Today’s metrics
            "today_trips_sent": _coerce_float(today_row.get("trips_sent")),
            "today_trips_accepted": _coerce_float(today_row.get("trips_accepted")),
            "today_finished_trips": _coerce_float(today_row.get("finished_trips")),
            "today_gross_earnings": _coerce_float(today_row.get("total_gmv")),
            "today_cash_trips": _coerce_float(today_row.get("cash_trips")),
            "today_card_trips": _coerce_float(today_row.get("card_trips")),
            "today_cash_earnings": _coerce_float(today_row.get("cash_gmv")),
            "today_card_earnings": _coerce_float(today_row.get("card_gmv")),
            "today_ride_distance_km": _coerce_float(today_row.get("ride_distance_sum")),
            "today_avg_ride_distance_km": _coerce_float(today_row.get("ride_distance_avg")),
            "today_ride_duration_min": _coerce_float(today_row.get("ride_duration_sum")),
            "today_pickup_duration_min": _coerce_float(today_row.get("pickup_duration_sum")),
            "today_tip_total": _coerce_float(today_row.get("tip_sum")),
        }

        metrics["trip_count"] = finished_trips if finished_trips is not None else total_trips
        metrics.setdefault("earnings_per_hour", _coerce_float(river_row.get("eph")))

        # Today acceptance rate
        if metrics.get("today_trips_sent"):
            metrics["today_acceptance_rate"] = _normalize_acceptance(
                (metrics.get("today_trips_accepted") or 0.0) / metrics["today_trips_sent"]
            )

        return metrics, None
    except Exception as e:
        log.warning("get_driver_kpis failed: %s", e)
        return None, f"error querying KPI data: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_driver_kpis(
    wa_id: str,
    driver: Dict[str, Any],
    *,
    use_cache: bool = True,
    include_today: bool = True,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not use_cache:
        result = _query_driver_kpis(wa_id, driver, include_today=include_today)
        if not result:
            return None, "no KPI data returned"
        return result
    key = _driver_kpi_cache_key(wa_id, driver, include_today=include_today)
    now = time.time()
    if key:
        with _driver_kpi_cache_lock:
            entry = _driver_kpi_cache.get(key)
            if entry and entry[0] > now:
                return entry[1], entry[2]
    result = _query_driver_kpis(wa_id, driver, include_today=include_today)
    if not result:
        metrics, reason = None, "no KPI data returned"
    else:
        metrics, reason = result
    if key:
        with _driver_kpi_cache_lock:
            _driver_kpi_cache[key] = (time.time() + DRIVER_KPI_CACHE_TTL_SECONDS, metrics, reason)
    return metrics, reason


def _detect_bolt_orders_table(conn) -> Optional[str]:
    for builder in BOLT_ORDER_TABLES:
        try:
            candidate = builder(MYSQL_DB)
            if _table_exists(conn, candidate):
                return candidate
        except Exception:
            continue
    return None


def _bolt_trips_agg(available: set[str]) -> str:
    for col in BOLT_TRIP_COLUMNS:
        if col in available:
            return f"SUM({col})"
    return "COUNT(*)"


def _bolt_earnings_expr(available: set[str]) -> str:
    if "total_price" in available:
        return "COALESCE(total_price, 0)"
    if "fare_finalised" in available:
        parts = ["COALESCE(fare_finalised, 0)"]
        for col in ("cancellation_fee", "booking_fee", "toll_fee", "tip"):
            if col in available:
                parts.append(f"COALESCE({col}, 0)")
        return " + ".join(parts) if parts else "0"
    for col in BOLT_EARNINGS_COLUMNS:
        if col in available:
            return f"COALESCE({col}, 0)"
    return "0"


def _bolt_hour_bucket_label(hour: int) -> str:
    start = int(hour) % 24
    end = (start + 1) % 24
    return f"{start:02d}:00-{end:02d}:00"


def _bolt_contact_prefers_driver_id(contact_col: Optional[str]) -> bool:
    return contact_col in {"driver_id", "driver_uuid", "driver_contact_id", "uuid"}


def get_hotspots(
    *,
    contact_ids: List[str],
    scope: str,
    timeframe: str,
    limit: int = 3,
) -> Tuple[List[Dict[str, Any]], Optional[str], str]:
    start_dt, end_dt, label = _resolve_time_range(timeframe or HOTSPOT_TIMEFRAME_DEFAULT)
    if not mysql_available():
        return [], "database connection not configured", label
    if scope != "global" and not contact_ids:
        return [], "driver has no linked contact IDs", label
    try:
        conn = get_mysql()
        table = _detect_bolt_orders_table(conn)
        if not table:
            return [], "no bolt orders table found", label
        available = _get_table_columns(conn, table)
        contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS)
        if scope != "global" and not contact_col:
            return [], "bolt orders table missing driver reference", label
        area_col = _pick_col_exists(conn, table, BOLT_AREA_COLUMNS)
        if not area_col:
            return [], "bolt orders table missing area column", label
        date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
        if not date_col:
            return [], "bolt orders table missing timestamp column", label
        earnings_expr = _bolt_earnings_expr(available)
        trips_agg = _bolt_trips_agg(available)

        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        select_parts = [f"{area_col} AS area"]
        select_parts.append(f"{trips_agg} AS trips_sum")
        select_parts.append(f"SUM({earnings_expr}) AS earnings_sum")
        select_clause = ", ".join(select_parts)

        where_clauses = [
            f"{area_col} IS NOT NULL",
            f"{area_col}<>''",
            f"{date_col} >= %s",
            f"{date_col} < %s",
        ]
        params: List[Any] = [start_str, end_str]

        if scope != "global":
            placeholders = ", ".join(["%s"] * len(contact_ids))
            where_clauses.insert(0, f"{contact_col} IN ({placeholders})")
            params = list(contact_ids) + params

        where_sql = " AND ".join(where_clauses)
        raw_limit = max(10, int(limit) * 5)
        sql = (
            f"SELECT {select_clause} FROM {table} "
            f"WHERE {where_sql} "
            f"GROUP BY {area_col} ORDER BY earnings_sum DESC, trips_sum DESC LIMIT {raw_limit}"
        )
        if LOG_DB_INSERTS:
            log.info("[BOLT] sql=%s params=%s", sql, params)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
        hotspots_map: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            raw_area = (row.get("area") or "").strip()
            area = _normalize_pickup_area(raw_area)
            if not area:
                continue
            trips_val = row.get("trips_sum")
            earnings_val = row.get("earnings_sum")
            trips = int(round(trips_val or 0)) if trips_val is not None else 0
            earnings = float(earnings_val) if earnings_val is not None else 0.0
            bucket = hotspots_map.get(area)
            if not bucket:
                hotspots_map[area] = {"area": area, "trips": trips, "earnings": earnings}
            else:
                bucket["trips"] = int(bucket.get("trips") or 0) + trips
                bucket["earnings"] = float(bucket.get("earnings") or 0.0) + earnings
        hotspots = sorted(
            hotspots_map.values(),
            key=lambda h: (h.get("earnings") or 0.0, h.get("trips") or 0),
            reverse=True,
        )
        hotspots = hotspots[: max(1, int(limit))]
        if not hotspots:
            scope_note = " globally" if scope == "global" else ""
            return [], f"no trips recorded {label}{scope_note}", label
        return hotspots, None, label
    except Exception as e:
        log.warning("get_hotspots failed: %s", e)
        return [], f"error querying bolt orders: {e}", label


def get_top_oph_areas(
    *,
    contact_ids: List[str],
    scope: str,
    timeframe: str,
    limit: int = 3,
) -> Tuple[List[Dict[str, Any]], Optional[str], str]:
    start_dt, end_dt, label = _resolve_time_range(timeframe or HOTSPOT_TIMEFRAME_DEFAULT)
    if not mysql_available():
        return [], "database connection not configured", label
    if scope != "global" and not contact_ids:
        return [], "driver has no linked contact IDs", label
    conn = None
    try:
        conn = get_mysql()
        table = _detect_bolt_orders_table(conn)
        if not table:
            return [], "no bolt orders table found", label
        available = _get_table_columns(conn, table)
        contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS)
        if scope != "global" and not contact_col:
            return [], "bolt orders table missing driver reference", label
        area_col = _pick_col_exists(conn, table, BOLT_AREA_COLUMNS)
        if not area_col:
            return [], "bolt orders table missing area column", label
        date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
        if not date_col:
            return [], "bolt orders table missing timestamp column", label
        trips_agg = _bolt_trips_agg(available)

        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        where_clauses = [
            f"{area_col} IS NOT NULL",
            f"{area_col}<>''",
            f"{date_col} >= %s",
            f"{date_col} < %s",
        ]
        params: List[Any] = [start_str, end_str]
        if scope != "global":
            placeholders = ", ".join(["%s"] * len(contact_ids))
            where_clauses.insert(0, f"{contact_col} IN ({placeholders})")
            params = list(contact_ids) + params
        if "company_id" in available:
            where_clauses.append("(company_id IS NULL OR company_id <> 96165)")

        where_sql = " AND ".join(where_clauses)
        raw_limit = max(20, int(limit) * 10)
        sql = (
            f"SELECT {area_col} AS area, "
            f"{trips_agg} AS trips_sum, "
            f"COUNT(DISTINCT DATE_FORMAT({date_col}, '%%Y-%%m-%%d %%H')) AS active_hours "
            f"FROM {table} WHERE {where_sql} "
            f"GROUP BY {area_col} ORDER BY trips_sum DESC LIMIT {raw_limit}"
        )
        if LOG_DB_INSERTS:
            log.info("[BOLT] oph_area_sql=%s params=%s", sql, params)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []

        area_map: Dict[str, Dict[str, float]] = {}
        for row in rows:
            raw_area = (row.get("area") or "").strip()
            area = _normalize_pickup_area(raw_area)
            if not area:
                continue
            trips_val = _coerce_float(row.get("trips_sum")) or 0.0
            hours_val = _coerce_float(row.get("active_hours")) or 0.0
            bucket = area_map.get(area)
            if not bucket:
                area_map[area] = {"trips": trips_val, "hours": hours_val}
            else:
                bucket["trips"] += trips_val
                bucket["hours"] += hours_val

        results: List[Dict[str, Any]] = []
        for area, vals in area_map.items():
            trips = vals.get("trips") or 0.0
            hours = vals.get("hours") or 0.0
            if hours <= 0 or trips <= 0:
                continue
            oph = trips / hours
            results.append(
                {
                    "area": area,
                    "trips": int(round(trips)),
                    "active_hours": hours,
                    "oph": oph,
                    "oph_display": f"{oph:.1f}",
                }
            )

        results.sort(
            key=lambda r: (r.get("oph") or 0.0, r.get("trips") or 0),
            reverse=True,
        )
        results = results[: max(1, int(limit))]
        if not results:
            scope_note = " globally" if scope == "global" else ""
            return [], f"no trips recorded {label}{scope_note}", label
        return results, None, label
    except Exception as exc:
        log.warning("get_top_oph_areas failed: %s", exc)
        return [], f"error querying bolt orders: {exc}", label
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_oph_for_areas(
    *,
    contact_ids: List[str],
    scope: str,
    timeframe: str,
    areas: List[str],
) -> Tuple[List[Dict[str, Any]], Optional[str], str]:
    start_dt, end_dt, label = _resolve_time_range(timeframe or HOTSPOT_TIMEFRAME_DEFAULT)
    if not mysql_available():
        return [], "database connection not configured", label
    if scope != "global" and not contact_ids:
        return [], "driver has no linked contact IDs", label
    if not areas:
        return [], "no areas provided", label
    conn = None
    try:
        conn = get_mysql()
        table = _detect_bolt_orders_table(conn)
        if not table:
            return [], "no bolt orders table found", label
        available = _get_table_columns(conn, table)
        contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS)
        if scope != "global" and not contact_col:
            return [], "bolt orders table missing driver reference", label
        area_col = _pick_col_exists(conn, table, BOLT_AREA_COLUMNS)
        if not area_col:
            return [], "bolt orders table missing area column", label
        date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
        if not date_col:
            return [], "bolt orders table missing timestamp column", label
        trips_agg = _bolt_trips_agg(available)

        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        area_terms: List[str] = []
        for area in areas:
            cleaned = _normalize_text(area).strip().lower()
            if not cleaned:
                continue
            area_terms.append(cleaned)
            mapped = _lookup_suburb_from_address(cleaned)
            if mapped and mapped.lower() not in area_terms:
                area_terms.append(mapped.lower())
        if not area_terms:
            return [], "no usable area terms", label

        where_clauses = [
            f"{area_col} IS NOT NULL",
            f"{area_col}<>''",
            f"{date_col} >= %s",
            f"{date_col} < %s",
        ]
        params: List[Any] = [start_str, end_str]
        if scope != "global":
            placeholders = ", ".join(["%s"] * len(contact_ids))
            where_clauses.insert(0, f"{contact_col} IN ({placeholders})")
            params = list(contact_ids) + params
        if "company_id" in available:
            where_clauses.append("(company_id IS NULL OR company_id <> 96165)")

        area_like = " OR ".join([f"LOWER({area_col}) LIKE %s" for _ in area_terms])
        where_clauses.append(f"({area_like})")
        params.extend([f"%{term}%" for term in area_terms])

        where_sql = " AND ".join(where_clauses)
        sql = (
            f"SELECT {area_col} AS area, "
            f"{trips_agg} AS trips_sum, "
            f"COUNT(DISTINCT DATE_FORMAT({date_col}, '%%Y-%%m-%%d %%H')) AS active_hours "
            f"FROM {table} WHERE {where_sql} "
            f"GROUP BY {area_col}"
        )
        if LOG_DB_INSERTS:
            log.info("[BOLT] oph_area_query_sql=%s params=%s", sql, params)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []

        area_map: Dict[str, Dict[str, float]] = {}
        for row in rows:
            raw_area = (row.get("area") or "").strip()
            area = _normalize_pickup_area(raw_area)
            if not area:
                continue
            trips_val = _coerce_float(row.get("trips_sum")) or 0.0
            hours_val = _coerce_float(row.get("active_hours")) or 0.0
            bucket = area_map.get(area)
            if not bucket:
                area_map[area] = {"trips": trips_val, "hours": hours_val}
            else:
                bucket["trips"] += trips_val
                bucket["hours"] += hours_val

        results: List[Dict[str, Any]] = []
        for area, vals in area_map.items():
            trips = vals.get("trips") or 0.0
            hours = vals.get("hours") or 0.0
            if hours <= 0 or trips <= 0:
                continue
            oph = trips / hours
            results.append(
                {
                    "area": area,
                    "trips": int(round(trips)),
                    "active_hours": hours,
                    "oph": oph,
                    "oph_display": f"{oph:.1f}",
                }
            )

        results.sort(
            key=lambda r: (r.get("oph") or 0.0, r.get("trips") or 0),
            reverse=True,
        )
        if not results:
            return [], "no OPH data for that area", label
        return results, None, label
    except Exception as exc:
        log.warning("get_oph_for_areas failed: %s", exc)
        return [], f"error querying bolt orders: {exc}", label
    finally:
        try:
            conn.close()
        except Exception:
            pass


def get_hotspot_times(
    *,
    contact_ids: List[str],
    scope: str,
    timeframe: str,
    driver_id: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str], str]:
    start_dt, end_dt, label = _resolve_time_range(timeframe or HOTSPOT_TIMEFRAME_DEFAULT)
    if not mysql_available():
        return [], "database connection not configured", label
    if scope != "global" and not (contact_ids or driver_id is not None):
        return [], "driver has no linked contact IDs", label
    try:
        conn = get_mysql()
        table = _detect_bolt_orders_table(conn)
        if not table:
            return [], "no bolt orders table found", label
        available = _get_table_columns(conn, table)
        date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
        if not date_col:
            return [], "bolt orders table missing timestamp column", label
        contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS) if scope != "global" else None
        if scope != "global" and not contact_col:
            return [], "bolt orders table missing driver reference", label
        status_col = _pick_col_exists(conn, table, BOLT_STATUS_COLUMNS)
        earnings_expr = _bolt_earnings_expr(available)
        trips_agg = _bolt_trips_agg(available)

        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        where_clauses = [
            f"{date_col} >= %s",
            f"{date_col} < %s",
        ]
        params: List[Any] = [start_str, end_str]

        if status_col:
            where_clauses.append(f"LOWER({status_col}) IN ({', '.join(['%s'] * len(FINISHED_STATUS_VALUES))})")
            params.extend([s.lower() for s in FINISHED_STATUS_VALUES])

        if scope != "global":
            ids: List[Any] = []
            if _bolt_contact_prefers_driver_id(contact_col) and driver_id is not None:
                ids = [driver_id]
            else:
                ids = list(contact_ids or [])
            if not ids:
                return [], "driver has no linked contact IDs", label
            placeholders = ", ".join(["%s"] * len(ids))
            where_clauses.insert(0, f"{contact_col} IN ({placeholders})")
            params = list(ids) + params

        where_sql = " AND ".join(where_clauses)
        order_clause = "earnings_sum DESC, trips_sum DESC" if earnings_expr != "0" else "trips_sum DESC"

        sql = (
            f"SELECT HOUR({date_col}) AS hour_bucket, "
            f"{trips_agg} AS trips_sum, "
            f"SUM({earnings_expr}) AS earnings_sum "
            f"FROM {table} WHERE {where_sql} "
            f"GROUP BY HOUR({date_col}) "
            f"ORDER BY {order_clause} LIMIT 3"
        )
        if LOG_DB_INSERTS:
            log.info("[BOLT] time_sql=%s params=%s", sql, params)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []

        slots: List[Dict[str, Any]] = []
        use_earnings = earnings_expr != "0"
        for row in rows:
            hour = row.get("hour_bucket")
            if hour is None:
                continue
            try:
                hour_int = int(hour)
            except Exception:
                continue
            slot: Dict[str, Any] = {
                "hour": hour_int,
                "label": _bolt_hour_bucket_label(hour_int),
                "trips": int(round(row.get("trips_sum") or 0)),
                "metric": "earnings" if use_earnings else "trips",
            }
            if use_earnings:
                slot["earnings"] = float(row.get("earnings_sum") or 0.0)
            slots.append(slot)
        if not slots:
            scope_note = " globally" if scope == "global" else ""
            return [], f"no trips recorded {label}{scope_note}", label
        return slots, None, label
    except Exception as e:
        log.warning("get_hotspot_times failed: %s", e)
        return [], f"error querying bolt orders: {e}", label


def get_asset_model_options() -> List[str]:
    if not mysql_available():
        return []
    conn = None
    try:
        conn = get_mysql()
        table = f"{MYSQL_DB}.driver_kpi_summary"
        if not _table_exists(conn, table):
            return sorted({k.title() for k in MODEL_TARGETS.keys()})

        def _fetch_column(col: str) -> List[str]:
            sql = (
                f"SELECT DISTINCT {col} AS model FROM {table} "
                f"WHERE {col} IS NOT NULL AND TRIM({col}) <> '' "
                f"ORDER BY {col} ASC LIMIT 200"
            )
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall() or []
            values: List[str] = []
            for row in rows:
                raw = row.get("model")
                if not raw:
                    continue
                value = str(raw).strip()
                if value:
                    values.append(value)
            return values

        seen: set[str] = set()
        options: List[str] = []
        for model_col in ("asset_model", "model", "vehicle_model"):
            try:
                values = _fetch_column(model_col)
            except Exception:
                continue
            for value in values:
                if value in seen:
                    continue
                seen.add(value)
                options.append(value)
        if options:
            return options
        return sorted({k.title() for k in MODEL_TARGETS.keys()})
    except Exception as exc:
        log.warning("asset model options lookup failed: %s", exc)
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _resolve_orders_filter_ids(
    driver_name: Optional[str],
    asset_model: Optional[str],
) -> Tuple[List[Any], Optional[str], int]:
    name_query = (driver_name or "").strip()
    model_query = (asset_model or "").strip()
    if not name_query and not model_query:
        return [], None, 0
    if not mysql_available():
        return [], "database connection not configured", 0
    conn = None
    try:
        conn = get_mysql()
        orders_table = _detect_bolt_orders_table(conn)
        if not orders_table:
            return [], "no bolt orders table found", 0
        orders_contact_col = _pick_col_exists(conn, orders_table, BOLT_CONTACT_COLUMNS)
        if not orders_contact_col:
            return [], "bolt orders table missing driver reference", 0
        kpi_table = f"{MYSQL_DB}.driver_kpi_summary"
        if not _table_exists(conn, kpi_table):
            return [], "driver_kpi_summary not found", 0
        kpi_available = _get_table_columns(conn, kpi_table)
        name_col = _pick_col_exists(conn, kpi_table, ["name", "driver_name", "full_name", "display_name"])
        model_cols = [c for c in ["model", "asset_model", "vehicle_model"] if c in kpi_available]
        if name_query and not name_col:
            return [], "driver name filter not supported (missing column)", 0
        if model_query and not model_cols:
            return [], "asset model filter not supported (missing column)", 0

        driver_id_col = _pick_col_exists(conn, kpi_table, ["driver_id", "id"])
        wa_col = _pick_col_exists(
            conn,
            kpi_table,
            ["wa_id", "whatsapp", "whatsapp_number", "wa_number", "phone", "phone_number", "contact_number"],
        )
        uuid_col = _pick_col_exists(conn, kpi_table, ["driver_uuid", "uuid"])
        contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in kpi_available]

        select_cols: List[str] = []
        for col in (driver_id_col, wa_col, uuid_col):
            if col and col not in select_cols:
                select_cols.append(col)
        for col in contact_cols:
            if col not in select_cols:
                select_cols.append(col)
        if not select_cols:
            return [], "driver_kpi_summary missing identifiers", 0

        where_clauses: List[str] = []
        params: List[Any] = []
        if name_query and name_col:
            where_clauses.append(f"UPPER({name_col}) LIKE %s")
            params.append(f"%{name_query.upper()}%")
        if model_query and model_cols:
            model_exprs = [f"UPPER({col}) LIKE %s" for col in model_cols]
            where_clauses.append("(" + " OR ".join(model_exprs) + ")")
            params.extend([f"%{model_query.upper()}%"] * len(model_cols))

        sql = f"SELECT DISTINCT {', '.join(select_cols)} FROM {kpi_table}"
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " LIMIT 5000"
        if LOG_DB_INSERTS:
            log.info("[ORDERS] filter_sql=%s params=%s", sql, params)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []

        if not rows:
            return [], "no drivers matched the selected filters", 0

        driver_ids: List[Any] = []
        wa_ids: List[str] = []
        uuid_ids: List[str] = []
        contact_ids: List[str] = []
        for row in rows:
            if driver_id_col and row.get(driver_id_col) is not None:
                driver_ids.append(row.get(driver_id_col))
            if wa_col and row.get(wa_col):
                normalized = _normalize_wa_id(row.get(wa_col))
                if normalized:
                    wa_ids.append(normalized)
            if uuid_col and row.get(uuid_col):
                uuid_ids.append(str(row.get(uuid_col)).strip())
            for col in contact_cols:
                val = row.get(col)
                if not val:
                    continue
                sval = str(val).strip()
                if sval:
                    contact_ids.append(sval)

        def dedup(values: List[Any]) -> List[Any]:
            out: List[Any] = []
            seen: set[str] = set()
            for val in values:
                key = str(val)
                if key in seen:
                    continue
                seen.add(key)
                out.append(val)
            return out

        driver_ids = dedup(driver_ids)
        wa_ids = dedup(wa_ids)
        uuid_ids = dedup(uuid_ids)
        contact_ids = dedup(contact_ids)

        if _bolt_contact_prefers_driver_id(orders_contact_col):
            if driver_ids:
                return driver_ids, None, len(rows)
            if uuid_ids:
                return uuid_ids, None, len(rows)
            return [], "orders table expects driver IDs but none were found", len(rows)
        if orders_contact_col == "wa_id":
            if wa_ids:
                return wa_ids, None, len(rows)
            return [], "orders table expects WhatsApp IDs but none were found", len(rows)
        if contact_ids:
            return contact_ids, None, len(rows)
        return [], "orders table expects contact IDs but none were found", len(rows)
    except Exception as exc:
        log.warning("resolve orders filters failed: %s", exc)
        return [], f"error resolving driver filters: {exc}", 0
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def get_orders_analytics(
    timeframe: str,
    *,
    filter_ids: Optional[List[Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not mysql_available():
        return None, "database connection not configured"
    start_dt, end_dt, label = _resolve_time_range(timeframe or HOTSPOT_TIMEFRAME_DEFAULT)
    conn = None
    try:
        conn = get_mysql()
        table = _detect_bolt_orders_table(conn)
        if not table:
            return None, "no bolt orders table found"
        available = _get_table_columns(conn, table)
        contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS)
        date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
        if not date_col:
            return None, "bolt orders table missing timestamp column"
        status_col = _pick_col_exists(conn, table, BOLT_STATUS_COLUMNS)
        payment_col = "payment_method" if "payment_method" in available else None
        distance_col = _pick_col_exists(conn, table, ["ride_distance", "trip_distance", "distance_km", "distance"])
        duration_col = _pick_col_exists(conn, table, ["ride_duration", "trip_duration", "duration", "duration_min"])
        pickup_col = _pick_col_exists(conn, table, ["pickup_duration", "pickup_time", "pickup_duration_min"])
        earnings_expr = _bolt_earnings_expr(available)
        trips_agg = _bolt_trips_agg(available)

        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        where_clauses = [f"{date_col} >= %s", f"{date_col} < %s"]
        params: List[Any] = [start_str, end_str]
        if "company_id" in available:
            where_clauses.append("(company_id IS NULL OR company_id <> 96165)")
        if filter_ids:
            if not contact_col:
                return None, "bolt orders table missing driver reference"
            placeholders = ", ".join(["%s"] * len(filter_ids))
            where_clauses.insert(0, f"{contact_col} IN ({placeholders})")
            params = list(filter_ids) + params

        select_parts = [
            f"{trips_agg} AS trips_total",
            f"SUM({earnings_expr}) AS earnings_sum",
            f"SUM(CASE WHEN ({earnings_expr}) > 0 THEN 1 ELSE 0 END) AS gmv_positive_count",
        ]
        finished_values = None
        if status_col:
            select_parts.append(
                f"SUM(CASE WHEN LOWER({status_col}) LIKE '%%accept%%' OR LOWER({status_col}) LIKE '%%finish%%' "
                f"THEN 1 ELSE 0 END) AS accepted_count"
            )
            finished_values = ", ".join([f"'{s.lower()}'" for s in FINISHED_STATUS_VALUES])
            select_parts.append(
                f"SUM(CASE WHEN LOWER({status_col}) IN ({finished_values}) THEN 1 ELSE 0 END) AS finished_count"
            )
        if payment_col:
            select_parts.append(
                f"SUM(CASE WHEN {payment_col} = 'cash' THEN 1 ELSE 0 END) AS cash_trips"
            )
            select_parts.append(
                f"SUM(CASE WHEN {payment_col} IN ('in_app','card') THEN 1 ELSE 0 END) AS card_trips"
            )
            select_parts.append(
                f"SUM(CASE WHEN {payment_col} = 'cash' THEN {earnings_expr} ELSE 0 END) AS cash_gmv"
            )
            select_parts.append(
                f"SUM(CASE WHEN {payment_col} IN ('in_app','card') THEN {earnings_expr} ELSE 0 END) AS card_gmv"
            )
        if distance_col:
            select_parts.append(f"SUM({distance_col}) AS distance_sum")
            if finished_values:
                select_parts.append(
                    f"SUM(CASE WHEN LOWER({status_col}) IN ({finished_values}) THEN {distance_col} ELSE 0 END) "
                    f"AS distance_finished_sum"
                )
        if duration_col:
            select_parts.append(f"SUM({duration_col}) AS duration_sum")
        if pickup_col:
            select_parts.append(f"SUM({pickup_col}) AS pickup_sum")

        sql = (
            f"SELECT {', '.join(select_parts)} FROM {table} "
            f"WHERE {' AND '.join(where_clauses)}"
        )
        if LOG_DB_INSERTS:
            log.info("[ORDERS] sql=%s params=%s", sql, params)
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone() or {}

        trips_total = int(float(row.get("trips_total") or 0))
        earnings_sum = _coerce_float(row.get("earnings_sum")) or 0.0
        gmv_positive_count = _coerce_float(row.get("gmv_positive_count"))
        accepted_count = row.get("accepted_count")
        finished_count = row.get("finished_count")
        cash_trips = row.get("cash_trips")
        card_trips = row.get("card_trips")
        cash_gmv = _coerce_float(row.get("cash_gmv"))
        card_gmv = _coerce_float(row.get("card_gmv"))
        distance_sum = _coerce_float(row.get("distance_sum"))
        distance_finished_sum = _coerce_float(row.get("distance_finished_sum"))
        duration_sum = _coerce_float(row.get("duration_sum"))
        pickup_sum = _coerce_float(row.get("pickup_sum"))

        acceptance_rate = None
        if accepted_count is not None and trips_total:
            acceptance_rate = (float(accepted_count) / trips_total) * 100.0

        avg_gmv = None
        if gmv_positive_count is not None and gmv_positive_count > 0:
            avg_gmv = earnings_sum / gmv_positive_count
        elif trips_total:
            avg_gmv = earnings_sum / trips_total
        avg_distance = None
        if distance_finished_sum is not None and finished_count and float(finished_count) > 0:
            avg_distance = distance_finished_sum / float(finished_count)
        elif distance_sum is not None and trips_total:
            avg_distance = distance_sum / trips_total
        avg_duration = (duration_sum / trips_total) if duration_sum is not None and trips_total else None
        avg_pickup = (pickup_sum / trips_total) if pickup_sum is not None and trips_total else None
        avg_duration_min = (avg_duration / 60.0) if avg_duration is not None else None
        avg_pickup_min = (avg_pickup / 60.0) if avg_pickup is not None else None

        cash_pct = None
        card_pct = None
        if cash_trips is not None and card_trips is not None:
            total_pay = float(cash_trips or 0) + float(card_trips or 0)
            if total_pay:
                cash_pct = (float(cash_trips) / total_pay) * 100.0
                card_pct = (float(card_trips) / total_pay) * 100.0

        daily: List[Dict[str, Any]] = []
        daily_params: List[Any] = [start_str, end_str]
        daily_where = [f"{date_col} >= %s", f"{date_col} < %s"]
        if "company_id" in available:
            daily_where.append("(company_id IS NULL OR company_id <> 96165)")
        if filter_ids and contact_col:
            placeholders = ", ".join(["%s"] * len(filter_ids))
            daily_where.insert(0, f"{contact_col} IN ({placeholders})")
            daily_params = list(filter_ids) + daily_params
        daily_sql = (
            f"SELECT DATE({date_col}) AS day, "
            f"{trips_agg} AS trips_sum, "
            f"SUM({earnings_expr}) AS earnings_sum "
            f"FROM {table} WHERE {' AND '.join(daily_where)} "
            f"GROUP BY DATE({date_col}) ORDER BY day DESC LIMIT 14"
        )
        if LOG_DB_INSERTS:
            log.info("[ORDERS] daily_sql=%s params=%s", daily_sql, daily_params)
        with conn.cursor() as cur:
            cur.execute(daily_sql, daily_params)
            rows = cur.fetchall() or []
        for day_row in rows:
            day_val = day_row.get("day")
            if hasattr(day_val, "strftime"):
                day_str = day_val.strftime("%Y-%m-%d")
            else:
                day_str = str(day_val) if day_val is not None else ""
            daily.append(
                {
                    "day": day_str,
                    "trips": int(float(day_row.get("trips_sum") or 0)),
                    "earnings": _coerce_float(day_row.get("earnings_sum")) or 0.0,
                }
            )

        return (
            {
                "label": label,
                "range_start": start_dt,
                "range_end": end_dt,
                "source_table": table,
                "trips_total": trips_total,
                "accepted_total": int(float(accepted_count)) if accepted_count is not None else None,
                "finished_total": int(float(finished_count)) if finished_count is not None else None,
                "acceptance_rate": acceptance_rate,
                "gmv_total": earnings_sum,
                "avg_gmv": avg_gmv,
                "cash_trips": int(float(cash_trips)) if cash_trips is not None else None,
                "card_trips": int(float(card_trips)) if card_trips is not None else None,
                "cash_gmv": cash_gmv,
                "card_gmv": card_gmv,
                "cash_pct": cash_pct,
                "card_pct": card_pct,
                "distance_sum": distance_sum,
                "avg_distance_km": avg_distance,
                "duration_sum": duration_sum,
                "avg_duration_min": avg_duration_min,
                "pickup_sum": pickup_sum,
                "avg_pickup_min": avg_pickup_min,
                "daily": daily,
            },
            None,
        )
    except Exception as e:
        log.warning("get_orders_analytics failed: %s", e)
        return None, f"error querying bolt orders: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_top_oph_drivers(
    timeframe: str,
    *,
    filter_ids: Optional[List[Any]] = None,
    limit: int = 10,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not mysql_available():
        return [], "database connection not configured"
    start_dt, end_dt, _label = _resolve_time_range(timeframe or HOTSPOT_TIMEFRAME_DEFAULT)
    conn = None
    try:
        conn = get_mysql()
        table = _detect_bolt_orders_table(conn)
        if not table:
            return [], "no bolt orders table found"
        available = _get_table_columns(conn, table)
        contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS)
        date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
        if not contact_col:
            return [], "bolt orders table missing driver reference"
        if not date_col:
            return [], "bolt orders table missing timestamp column"

        area_col = _pick_col_exists(conn, table, BOLT_AREA_COLUMNS)
        trips_agg = _bolt_trips_agg(available)

        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

        where_clauses = [
            f"{date_col} >= %s",
            f"{date_col} < %s",
            f"{contact_col} IS NOT NULL",
        ]
        params: List[Any] = [start_str, end_str]
        if "company_id" in available:
            where_clauses.append("(company_id IS NULL OR company_id <> 96165)")
        if filter_ids:
            placeholders = ", ".join(["%s"] * len(filter_ids))
            where_clauses.insert(0, f"{contact_col} IN ({placeholders})")
            params = list(filter_ids) + params

        driver_sql = (
            f"SELECT {contact_col} AS driver_key, "
            f"{trips_agg} AS trips_total, "
            f"COUNT(DISTINCT DATE_FORMAT({date_col}, '%%Y-%%m-%%d %%H')) AS active_hours "
            f"FROM {table} WHERE {' AND '.join(where_clauses)} "
            f"GROUP BY {contact_col}"
        )
        if LOG_DB_INSERTS:
            log.info("[ORDERS] top_oph_sql=%s params=%s", driver_sql, params)
        with conn.cursor() as cur:
            cur.execute(driver_sql, params)
            rows = cur.fetchall() or []

        driver_rows: List[Dict[str, Any]] = []
        for row in rows:
            driver_key = row.get("driver_key")
            if driver_key is None or driver_key == "":
                continue
            trips_total = _coerce_float(row.get("trips_total")) or 0.0
            active_hours = _coerce_float(row.get("active_hours")) or 0.0
            if active_hours <= 0:
                continue
            oph = trips_total / active_hours
            driver_rows.append(
                {
                    "driver_key": driver_key,
                    "driver_key_str": str(driver_key),
                    "orders": int(round(trips_total)),
                    "active_hours": active_hours,
                    "oph": oph,
                }
            )

        if not driver_rows:
            return [], "no driver activity recorded for this timeframe"

        driver_rows.sort(
            key=lambda r: (r.get("oph") or 0.0, r.get("orders") or 0, r.get("active_hours") or 0.0),
            reverse=True,
        )
        top_rows = driver_rows[: max(1, int(limit))]

        def _name_lookup_key(value: Any) -> str:
            if contact_col == "wa_id":
                return re.sub(r"\D", "", str(value or ""))
            return str(value or "").strip()

        def _fetch_driver_name_map(keys: List[Any]) -> Dict[str, str]:
            if not keys:
                return {}
            kpi_table = f"{MYSQL_DB}.driver_kpi_summary"
            if not _table_exists(conn, kpi_table):
                return {}
            kpi_cols = _get_table_columns(conn, kpi_table)
            name_col = _pick_col_exists(conn, kpi_table, ["display_name", "name", "driver_name", "full_name"])
            if not name_col:
                return {}

            name_map: Dict[str, str] = {}

            if _bolt_contact_prefers_driver_id(contact_col):
                id_col = _pick_col_exists(conn, kpi_table, ["driver_id", "driver_uuid", "uuid", "id"])
                if not id_col:
                    return {}
                id_vals: List[Any] = []
                for val in keys:
                    if val in (None, ""):
                        continue
                    try:
                        id_vals.append(int(val))
                    except Exception:
                        id_vals.append(str(val).strip())
                if not id_vals:
                    return {}
                placeholders = ", ".join(["%s"] * len(id_vals))
                sql = (
                    f"SELECT {id_col} AS match_key, {name_col} AS display_name "
                    f"FROM {kpi_table} WHERE {id_col} IN ({placeholders})"
                )
                with conn.cursor() as cur:
                    cur.execute(sql, id_vals)
                    rows = cur.fetchall() or []
                for row in rows:
                    key_val = row.get("match_key")
                    name_val = row.get("display_name")
                    if key_val is None or not name_val:
                        continue
                    name_map[str(key_val)] = str(name_val).strip()
                return name_map

            if contact_col == "wa_id":
                phone_col = _pick_col_exists(
                    conn,
                    kpi_table,
                    ["phone", "wa_id", "whatsapp", "whatsapp_number", "wa_number", "phone_number", "contact_number"],
                )
                if not phone_col:
                    return {}
                normalized_keys = [_name_lookup_key(val) for val in keys if _name_lookup_key(val)]
                if not normalized_keys:
                    return {}
                placeholders = ", ".join(["%s"] * len(normalized_keys))
                phone_expr = _sanitize_phone_expr(phone_col)
                sql = (
                    f"SELECT {phone_expr} AS match_key, {name_col} AS display_name "
                    f"FROM {kpi_table} WHERE {phone_expr} IN ({placeholders})"
                )
                with conn.cursor() as cur:
                    cur.execute(sql, normalized_keys)
                    rows = cur.fetchall() or []
                for row in rows:
                    key_val = row.get("match_key")
                    name_val = row.get("display_name")
                    if key_val is None or not name_val:
                        continue
                    name_map[str(key_val)] = str(name_val).strip()
                return name_map

            contact_cols = [c for c in KPI_CONTACT_COLUMNS if c in kpi_cols]
            if not contact_cols:
                contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in kpi_cols]
            if not contact_cols:
                return {}
            cleaned_keys = [str(val).strip() for val in keys if val not in (None, "")]
            if not cleaned_keys:
                return {}
            placeholders = ", ".join(["%s"] * len(cleaned_keys))
            where_parts = [f"{col} IN ({placeholders})" for col in contact_cols]
            params: List[Any] = []
            for _ in contact_cols:
                params.extend(cleaned_keys)
            sql = (
                f"SELECT {name_col} AS display_name, {', '.join(contact_cols)} "
                f"FROM {kpi_table} WHERE {' OR '.join(where_parts)}"
            )
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() or []
            for row in rows:
                name_val = row.get("display_name")
                if not name_val:
                    continue
                display = str(name_val).strip()
                for col in contact_cols:
                    val = row.get(col)
                    if not val:
                        continue
                    key = str(val).strip()
                    if key and key not in name_map:
                        name_map[key] = display
            return name_map

        name_map = _fetch_driver_name_map([row["driver_key"] for row in top_rows])

        area_map: Dict[str, str] = {}
        if area_col and top_rows:
            top_keys = [row["driver_key"] for row in top_rows]
            placeholders = ", ".join(["%s"] * len(top_keys))
            area_where = [
                f"{contact_col} IN ({placeholders})",
                f"{date_col} >= %s",
                f"{date_col} < %s",
                f"{area_col} IS NOT NULL",
                f"{area_col} <> ''",
            ]
            area_params: List[Any] = list(top_keys) + [start_str, end_str]
            if "company_id" in available:
                area_where.append("(company_id IS NULL OR company_id <> 96165)")
            area_sql = (
                f"SELECT {contact_col} AS driver_key, {area_col} AS area, "
                f"{trips_agg} AS trips_sum "
                f"FROM {table} WHERE {' AND '.join(area_where)} "
                f"GROUP BY {contact_col}, {area_col}"
            )
            if LOG_DB_INSERTS:
                log.info("[ORDERS] top_oph_area_sql=%s params=%s", area_sql, area_params)
            with conn.cursor() as cur:
                cur.execute(area_sql, area_params)
                area_rows = cur.fetchall() or []

            area_counts: Dict[str, Dict[str, float]] = {}
            for row in area_rows:
                key = str(row.get("driver_key"))
                raw_area = (row.get("area") or "").strip()
                normalized = _normalize_pickup_area(raw_area)
                if not normalized:
                    continue
                trips_val = _coerce_float(row.get("trips_sum")) or 0.0
                bucket = area_counts.setdefault(key, {})
                bucket[normalized] = bucket.get(normalized, 0.0) + trips_val
            for key, bucket in area_counts.items():
                top_area = max(bucket.items(), key=lambda item: (item[1], item[0]))[0]
                area_map[key] = top_area

        results: List[Dict[str, Any]] = []
        for row in top_rows:
            driver_key = row["driver_key"]
            key_str = row["driver_key_str"]
            lookup_key = _name_lookup_key(driver_key)
            driver_name = name_map.get(lookup_key) or name_map.get(key_str) or key_str
            results.append(
                {
                    "driver_name": driver_name or key_str,
                    "oph": row["oph"],
                    "oph_display": f"{row['oph']:.1f}",
                    "area": area_map.get(key_str) or "—",
                }
            )
        return results, None
    except Exception as exc:
        log.warning("get_top_oph_drivers failed: %s", exc)
        return [], f"error querying bolt orders: {exc}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

KPI_PRIORITY = ["online_hours", "acceptance_rate", "gross_earnings", "trip_count", "earnings_per_hour"]
KPI_GAP_THRESHOLDS = {
    "online_hours": 1.0,
    "acceptance_rate": 1.0,
    "gross_earnings": 150.0,
    "trip_count": 3.0,
    "earnings_per_hour": 5.0,
}

def _fmt_hours(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    return f"{value:.0f}h" if value >= 10 else f"{value:.1f}h"

def _fmt_percent(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    return f"{float(value):.0f}%"

def _fmt_trips(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    return f"{int(round(value))} trips"

KPI_POSITIVE_MESSAGES = {
    "online_hours": "Nice work meeting the online-hours goal 👏",
    "acceptance_rate": "Acceptance rate looks strong—keep responding quickly!",
    "gross_earnings": "Weekly gross is on track—great momentum!",
    "trip_count": "Trip count is on pace for the target.",
    "earnings_per_hour": "Earnings per hour is solid—stick with surge zones.",
}

KPI_NEGATIVE_FEEDBACK = {
    "online_hours": lambda value, target: f"You're about {max(1, int(round(target - value)))}h short of the {int(round(target))}h goal—add one more long shift.",
    "acceptance_rate": lambda value, target: f"Acceptance is around {_fmt_percent(value)}; aim for {_fmt_percent(target)} by responding faster.",
    "gross_earnings": lambda value, target: f"Gross sits at {fmt_rands(value or 0)}; push toward {fmt_rands(target)} with peak-hour runs.",
    "trip_count": lambda value, target: f"Trips are {int(round(value or 0))} vs {int(round(target))} goal—stay online through evening peak.",
    "earnings_per_hour": lambda value, target: f"Earnings per hour is {fmt_rands(value or 0)}; chase surge windows to reach {fmt_rands(target)}.",
}

KPI_TIPS = {
    "online_hours": lambda diff, target: f"Add an extra long shift (~{max(1, int(round(diff)))}h) to reach {int(round(target))}h online.",
    "acceptance_rate": lambda diff, target: f"Respond within 15s and decline slow pings fast to climb toward {int(round(target))}% acceptance.",
    "gross_earnings": lambda diff, target: f"Stack surge slots to push gross closer to {fmt_rands(target)}.",
    "trip_count": lambda diff, target: f"Stay online through the evening rush to close the gap to {int(round(target))} trips.",
    "earnings_per_hour": lambda diff, target: f"Focus on high-demand zones to lift earnings toward {fmt_rands(target)} per hour.",
}

MISSING_KPI_MESSAGES = [
    "I don't have your latest stats yet. Open the driver app, sync, and message me again.",
    "I can't see fresh stats yet - please sync the driver app, then ping me.",
    "Still waiting on your latest stats. If you sync the app, I can pull them.",
]
MISSING_KPI_MSG = MISSING_KPI_MESSAGES[0]

def _missing_kpi_message() -> str:
    return _pick_phrase(MISSING_KPI_MESSAGES) or MISSING_KPI_MSG

def _target_update_hint() -> str:
    return _pick_phrase(
        [
            "If you want to tweak your target, just send the hours or trips.",
            "Want to change the target? Send the hours or trips.",
            "You can update the target anytime - just send the hours or trips.",
        ]
    )

def _daily_update_hint() -> str:
    return _pick_phrase(
        [
            "Reply 'daily update' any time for your live count.",
            "Send 'daily update' whenever you want a quick check-in.",
            "Type 'daily update' for a quick live update.",
        ]
    )

def _is_no_trips_metrics(metrics: Optional[Dict[str, Any]]) -> bool:
    if not metrics:
        return False
    trip_count = _coerce_float(metrics.get("trip_count"))
    if trip_count is None:
        return False
    return trip_count <= 0

def _is_missing_kpi_reason(reason: Optional[str]) -> bool:
    if not reason:
        return False
    r = reason.lower()
    tokens = [
        "no driver_kpi_summary records matched",
        "no driver matched",
        "no driver identifiers available",
        "no kpi data returned",
        "no driver matched the provided identifiers",
    ]
    return any(token in r for token in tokens)

def _kpi_no_trips_commitment_message(targets: Dict[str, float]) -> str:
    hours_target = targets.get("online_hours") or ENGAGEMENT_TARGET_ONLINE_HOURS_MIN
    trips_target = targets.get("trip_count") or ENGAGEMENT_TARGET_TRIPS
    hours_text = int(round(hours_target))
    trips_text = int(round(trips_target))
    return (
        "It looks like we haven’t captured any trips yet. "
        f"Can you commit to getting online for about {hours_text} hours and {trips_text}+ trips this week? "
        "If you reply YES, I’ll keep you updated on your progress."
    )

def _primary_gap(metrics: Dict[str, Any], targets: Dict[str, float]) -> Optional[Tuple[str, float]]:
    for key in KPI_PRIORITY:
        value = metrics.get(key)
        target = targets.get(key)
        if value is None or target is None:
            continue
        diff = target - value if key != "acceptance_rate" else target - value
        threshold = KPI_GAP_THRESHOLDS.get(key, 0)
        if diff > threshold:
            return key, diff
    return None

def _kpi_feedback(metrics: Dict[str, Any], targets: Dict[str, float]) -> str:
    gap = _primary_gap(metrics, targets)
    if gap:
        key, _ = gap
        value = metrics.get(key)
        target = targets.get(key)
        func = KPI_NEGATIVE_FEEDBACK.get(key)
        if func and value is not None and target is not None:
            return func(value, target)
    for key in KPI_PRIORITY:
        value = metrics.get(key)
        target = targets.get(key)
        if value is None or target is None:
            continue
        msg = KPI_POSITIVE_MESSAGES.get(key)
        if msg:
            return msg
    return "Let’s keep pushing steadily this week."

def _pretty_reason(reason: Optional[str]) -> Optional[str]:
    if not reason:
        return None
    r = reason.lower()
    if "no kpi records" in r:
        return "no performance entries matched your driver in the reporting tables"
    if "no kpi table" in r or "missing driver/contact" in r:
        return "the reporting tables aren’t set up with the expected columns"
    if "no linked contact" in r:
        return "this number has no driver account linked"
    if "database connection" in r:
        return "the performance database credentials aren’t configured"
    if "error querying" in r:
        return "there was an error while reading the performance tables"
    if "bolt orders" in r:
        return "I couldn’t read the Bolt orders feed"
    if "no trips recorded" in r:
        suffix = r.split("no trips recorded", 1)[1].strip()
        if suffix:
            pretty_suffix = suffix.replace("globally", "across the fleet")
            return f"there were no Bolt trips captured {pretty_suffix}"
        return "there were no Bolt trips captured today"
    return reason


def _compose_kpi_ai_reply(
    *,
    intent: str,
    user_message: str,
    name: str,
    metrics: Dict[str, Any],
    targets: Dict[str, float],
    ctx: Dict[str, Any],
    hotspots: Optional[List[Dict[str, Any]]] = None,
    hotspots_reason: Optional[str] = None,
    hotspots_timeframe: Optional[str] = None,
    hotspots_scope: str = HOTSPOT_SCOPE_DEFAULT,
    oph_areas: Optional[List[Dict[str, Any]]] = None,
    oph_reason: Optional[str] = None,
    oph_timeframe: Optional[str] = None,
    oph_scope: str = HOTSPOT_SCOPE_DEFAULT,
    hotspot_times: Optional[List[Dict[str, Any]]] = None,
    hotspot_times_timeframe: Optional[str] = None,
    hotspot_times_scope: str = HOTSPOT_SCOPE_DEFAULT,
) -> Optional[str]:
    if intent in {"progress_update", "daily_target_status"}:
        return None
    key_metrics = {k: v for k, v in metrics.items() if k in {
        "period_label","online_hours","acceptance_rate","earnings_per_hour","gross_earnings","trip_count"
    }}
    context_notes = {}
    if metrics.get("period_label"):
        context_notes["period_label"] = metrics.get("period_label")
    if ctx.get("_last_kpi_reason"):
        pretty = _pretty_reason(ctx.get("_last_kpi_reason"))
        if pretty:
            context_notes["data_issue"] = pretty
    if ctx.get("_last_user_message") and ctx.get("_last_user_message") != user_message:
        context_notes["previous_user_message"] = ctx.get("_last_user_message")
    if hotspots:
        context_notes["hotspots"] = {
            "scope": hotspots_scope,
            "timeframe": hotspots_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT),
            "areas": hotspots,
        }
    elif hotspots_reason:
        pretty_hot = _pretty_reason(hotspots_reason) or hotspots_reason
        context_notes["hotspots_issue"] = pretty_hot
        context_notes["hotspots_scope"] = hotspots_scope
        if hotspots_timeframe:
            context_notes["hotspots_timeframe"] = hotspots_timeframe
    if hotspot_times:
        context_notes["best_times"] = {
            "scope": hotspot_times_scope,
            "timeframe": hotspot_times_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT),
            "hours": hotspot_times,
        }
    if oph_areas:
        context_notes["oph_areas"] = {
            "scope": oph_scope,
            "timeframe": oph_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT),
            "areas": oph_areas,
        }
    elif oph_reason:
        pretty_oph = _pretty_reason(oph_reason) or oph_reason
        context_notes["oph_areas_issue"] = pretty_oph
        context_notes["oph_areas_scope"] = oph_scope
        if oph_timeframe:
            context_notes["oph_areas_timeframe"] = oph_timeframe

    prompt = (
        f"Driver first name: {name or 'Driver'}\n"
        f"Intent: {intent}\n"
        f"User message: {user_message}\n"
        f"Latest metrics: {json.dumps(key_metrics, ensure_ascii=False)}\n"
        f"Targets by vehicle: {json.dumps(targets, ensure_ascii=False)}\n"
        f"Context notes: {json.dumps(context_notes, ensure_ascii=False)}\n\n"
        "Write one short WhatsApp reply that \n"
        "- references the relevant metrics or the lack of data,\n"
        "- keeps the warm coaching tone, and\n"
        "- offers a practical next step."
    )
    return nlg_with_openai(DINEO_TONE, prompt)


def _compose_llm_fallback_reply(
    *,
    user_message: str,
    name: str,
    ctx: Dict[str, Any],
    driver: Dict[str, Any],
) -> Optional[str]:
    if not OPENAI_API_KEY:
        return None
    msg = (user_message or "").strip()
    if not msg:
        return None
    lowered = _normalize_text(msg).lower().strip()
    allow_repossession = _is_vehicle_repossession(lowered)
    targets = _recent_goal_targets(ctx) or get_model_targets(driver.get("asset_model")) or {}
    target_notes = {k: v for k, v in targets.items() if k in {
        "online_hours", "trip_count", "acceptance_rate", "earnings_per_hour", "gross_earnings"
    }}
    context_notes: Dict[str, Any] = {}
    last_reason = ctx.get("_last_kpi_reason")
    if last_reason:
        pretty = _pretty_reason(last_reason)
        if pretty:
            context_notes["last_kpi_issue"] = pretty
    active_concern = ctx.get("_active_concern")
    if isinstance(active_concern, dict):
        context_notes["active_concern"] = {
            k: active_concern.get(k)
            for k in ("type", "message")
            if active_concern.get(k)
        }
    last_template = ctx.get("_last_outbound_template")
    if isinstance(last_template, dict) and last_template.get("id"):
        context_notes["last_template"] = {
            "id": last_template.get("id"),
            "params": last_template.get("params_named") or last_template.get("params"),
        }
    oph_areas = ctx.get("_oph_areas_cache") or []
    if oph_areas:
        context_notes["oph_areas"] = oph_areas[:3]
    hotspots = ctx.get("_hotspots_cache") or []
    if hotspots:
        context_notes["hotspots"] = hotspots[:3]

    login_url = "https://60b9b868ac0b.ngrok-free.app/driver/login"
    prompt = (
        f"Driver first name: {name or 'Driver'}\n"
        f"User message: {msg}\n"
        f"Targets: {json.dumps(target_notes, ensure_ascii=False)}\n"
        f"Context notes: {json.dumps(context_notes, ensure_ascii=False)}\n"
        f"Repossession mentioned by user: {'yes' if allow_repossession else 'no'}\n\n"
        "Write one short WhatsApp reply (1-2 sentences). Follow these rules:\n"
        "- Friendly, human, not scripted. Use contractions.\n"
        "- If the message is a greeting, greet by name and ask what they need.\n"
        "- If about balance/owe/outstanding, send this login link: "
        f"{login_url} and mention personal code (SA ID or TRN from PrDP).\n"
        "- If about not getting trips/low demand, ask which suburb/landmark; if OPH areas exist, suggest up to 3.\n"
        "- If about account suspended/blocked, ask what message they see and offer to log a ticket.\n"
        "- If about no car, ask the reason (workshop, replacement, balance, medical, other). "
        "If medical, advise returning the vehicle to the office so ops can assist and ask if they plan to continue or hand back; "
        "ask for a medical certificate image.\n"
        "- Do not mention repossession unless the user already mentioned it.\n"
        "- Do not claim you opened a ticket unless context notes say one is open; ask if they want you to log one.\n"
        "- If the question is short/ambiguous, ask one clarifying question; if it is a number, ask if hours or trips.\n"
        "- If the topic is performance goals, end with a commitment question (hours or trips) "
        "unless the user already stated a commitment in this message.\n"
    )
    return nlg_with_openai(DINEO_TONE, prompt)


def _format_hotspot_sentence(
    hotspots: Optional[List[Dict[str, Any]]],
    hotspots_reason: Optional[str],
    hotspots_timeframe: Optional[str],
    hotspots_scope: str,
) -> str:
    scope = hotspots_scope or HOTSPOT_SCOPE_DEFAULT
    label = hotspots_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT)
    if scope == "global":
        prefix = "Top earning areas today" if label == "today" else f"Top earning areas {label}"
    else:
        if label == "today":
            prefix = "Top earning areas today for you"
        else:
            prefix = f"Top earning areas {label} for you"

    if hotspots:
        bits = []
        for h in hotspots[:3]:
            area = h.get("area")
            if not area:
                continue
            part = area
            trips = h.get("trips")
            earnings = h.get("earnings")
            if trips:
                part += f" ({int(trips)} trips)"
            if earnings:
                part += f", {fmt_rands(earnings)}"
            bits.append(part)
        if bits:
            return f"{prefix}: " + ", ".join(bits) + "."
    if hotspots_reason:
        pretty = _pretty_reason(hotspots_reason) or hotspots_reason
        return f"Couldn’t fetch hotspot stats because {pretty}."
    return ""


def _format_oph_area_sentence(
    oph_areas: Optional[List[Dict[str, Any]]],
    oph_reason: Optional[str],
    oph_timeframe: Optional[str],
    oph_scope: str,
) -> str:
    scope = oph_scope or HOTSPOT_SCOPE_DEFAULT
    label = oph_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT)
    if scope == "global":
        prefix = "Fleet-wide top OPH suburbs today" if label == "today" else f"Fleet-wide top OPH suburbs {label}"
    else:
        prefix = "Top OPH suburbs today for you" if label == "today" else f"Top OPH suburbs {label} for you"

    if oph_areas:
        bits = []
        for area in oph_areas[:3]:
            name = area.get("area")
            oph = area.get("oph_display") or (f"{area['oph']:.1f}" if area.get("oph") is not None else None)
            if not name or not oph:
                continue
            bits.append(f"{name} ({oph} OPH)")
        if bits:
            return f"{prefix}: " + ", ".join(bits) + "."
    if oph_reason:
        pretty = _pretty_reason(oph_reason) or oph_reason
        return f"Couldn’t fetch OPH areas because {pretty}."
    return ""


def _oph_quality_label(oph: Optional[float]) -> str:
    if oph is None:
        return "unknown"
    if oph >= 10.0:
        return "busy"
    if oph >= 7.0:
        return "steady"
    if oph >= 5.0:
        return "slow"
    return "very quiet"


def _format_area_oph_sentence(
    area_oph: Optional[List[Dict[str, Any]]],
    area_oph_reason: Optional[str],
    area_oph_timeframe: Optional[str],
    requested: Optional[List[str]] = None,
) -> str:
    label = area_oph_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT)
    if area_oph:
        if len(area_oph) == 1:
            entry = area_oph[0]
            name = entry.get("area")
            oph = entry.get("oph")
            oph_display = entry.get("oph_display") or (f"{oph:.1f}" if oph is not None else None)
            if name and oph_display:
                quality = _oph_quality_label(oph)
                return f"{name} is around {oph_display} OPH {label} — {quality} right now."
        bits: List[str] = []
        for entry in area_oph[:3]:
            name = entry.get("area")
            oph = entry.get("oph")
            oph_display = entry.get("oph_display") or (f\"{oph:.1f}\" if oph is not None else None)
            if not name or not oph_display:
                continue
            quality = _oph_quality_label(oph)
            bits.append(f\"{name} {oph_display} OPH ({quality})\")
        if bits:
            return f\"OPH {label} by area: \" + \", \".join(bits) + \".\"
    if area_oph_reason and requested:
        req = \", \".join([r for r in requested[:3] if r])
        if req:
            pretty = _pretty_reason(area_oph_reason) or area_oph_reason
            return f\"I couldn't pull OPH for {req} because {pretty}. Want me to check another area?\"
    if area_oph_reason and not requested:
        pretty = _pretty_reason(area_oph_reason) or area_oph_reason
        return f\"I couldn't pull OPH for that area because {pretty}. Want me to check another area?\"
    return ""


def _format_hotspot_times_sentence(
    hotspot_times: Optional[List[Dict[str, Any]]],
    hotspot_times_reason: Optional[str],
    hotspot_times_timeframe: Optional[str],
    hotspots_scope: str,
) -> str:
    scope = hotspots_scope or HOTSPOT_SCOPE_DEFAULT
    label = hotspot_times_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT)
    if scope == "global":
        prefix = "Best earning times today" if label == "today" else f"Best earning times {label}"
    else:
        prefix = "Best earning times today for you" if label == "today" else f"Best earning times {label} for you"

    if hotspot_times:
        bits = []
        for slot in hotspot_times[:3]:
            slot_label = slot.get("label")
            if not slot_label and slot.get("hour") is not None:
                slot_label = _bolt_hour_bucket_label(slot["hour"])
            if not slot_label:
                continue
            part = slot_label
            metric = slot.get("metric")
            if metric == "earnings" and slot.get("earnings") is not None:
                part += f" ({fmt_rands(slot.get('earnings'))})"
            else:
                trips = slot.get("trips")
                if trips:
                    part += f" ({int(trips)} trips)"
            bits.append(part)
        if bits:
            return f"{prefix}: " + ", ".join(bits) + "."
    if hotspot_times_reason:
        pretty = _pretty_reason(hotspot_times_reason) or hotspot_times_reason
        return f"Couldn’t fetch best-time stats because {pretty}."
    return ""


def render_kpi_reply(
    intent: str,
    metrics: Optional[Dict[str, Any]],
    targets: Dict[str, float],
    reason: Optional[str] = None,
    hotspots: Optional[List[Dict[str, Any]]] = None,
    hotspots_reason: Optional[str] = None,
    hotspots_timeframe: Optional[str] = None,
    hotspots_scope: str = HOTSPOT_SCOPE_DEFAULT,
    oph_areas: Optional[List[Dict[str, Any]]] = None,
    oph_reason: Optional[str] = None,
    oph_timeframe: Optional[str] = None,
    oph_scope: str = HOTSPOT_SCOPE_DEFAULT,
    area_oph: Optional[List[Dict[str, Any]]] = None,
    area_oph_reason: Optional[str] = None,
    area_oph_timeframe: Optional[str] = None,
    area_oph_requested: Optional[List[str]] = None,
    hotspot_times: Optional[List[Dict[str, Any]]] = None,
    hotspot_times_reason: Optional[str] = None,
    hotspot_times_timeframe: Optional[str] = None,
    daily_target: Optional[int] = None,
    requested_hour: Optional[int] = None,
    include_hours: bool = False,
    now: Optional[datetime] = None,
    commitment_prompt: bool = False,
) -> str:
    scope = hotspots_scope or HOTSPOT_SCOPE_DEFAULT
    label = hotspots_timeframe or _timeframe_label(HOTSPOT_TIMEFRAME_DEFAULT)
    times_label = hotspot_times_timeframe or label
    oph_label = oph_timeframe or label

    def hotspot_extra() -> str:
        parts: List[str] = []
        area_sentence = _format_area_oph_sentence(area_oph, area_oph_reason, area_oph_timeframe, area_oph_requested)
        if area_sentence:
            parts.append(area_sentence)
        if oph_areas:
            if not area_sentence or any((entry.get("oph") or 0.0) < 7.0 for entry in (area_oph or [])):
                oph_sentence = _format_oph_area_sentence(oph_areas, None, oph_label, oph_scope)
                if oph_sentence:
                    parts.append(oph_sentence)
        if hotspots:
            if not oph_areas and not area_sentence:
                area = _format_hotspot_sentence(hotspots, None, label, scope)
                if area:
                    parts.append(area)
        if hotspot_times:
            times = _format_hotspot_times_sentence(hotspot_times, None, times_label, scope)
            if times:
                parts.append(times)
        if parts:
            return " ".join(parts)
        if oph_reason:
            return _format_oph_area_sentence(None, oph_reason, oph_label, oph_scope)
        if hotspots_reason:
            return _format_hotspot_sentence(None, hotspots_reason, label, scope)
        if hotspot_times_reason:
            return _format_hotspot_times_sentence(None, hotspot_times_reason, times_label, scope)
        return ""

    if intent == "daily_target_status":
        now = now or jhb_now()
        target_daily = daily_target
        if target_daily is None:
            weekly_trips = targets.get("trip_count") or DEFAULT_TARGETS["trip_count"]
            try:
                weekly_trips_val = float(weekly_trips)
            except Exception:
                weekly_trips_val = float(DEFAULT_TARGETS["trip_count"])
            target_daily = int(math.ceil(weekly_trips_val / 5.0))
            target_daily = max(INTRADAY_DAILY_MIN_FINISHED_ORDERS, target_daily)
        else:
            weekly_trips = targets.get("trip_count") or DEFAULT_TARGETS["trip_count"]
            try:
                weekly_trips_val = float(weekly_trips)
            except Exception:
                weekly_trips_val = float(DEFAULT_TARGETS["trip_count"])

        weekly_hours = targets.get("online_hours") or ENGAGEMENT_TARGET_ONLINE_HOURS_MIN
        try:
            weekly_hours_val = float(weekly_hours)
        except Exception:
            weekly_hours_val = float(ENGAGEMENT_TARGET_ONLINE_HOURS_MIN)

        daily_hours_goal = max(1.0, weekly_hours_val / 5.0)
        monthly_trips_goal = int(math.ceil(weekly_trips_val * 4.0))
        monthly_hours_goal = int(math.ceil(weekly_hours_val * 4.0))

        if requested_hour is not None:
            slot_hour, target_by = _intraday_target_for_hour(requested_hour, target_daily)
        else:
            slot_hour, target_by = _intraday_next_checkpoint(now, target_daily)

        finished = None
        acceptance_text = None
        if metrics:
            finished_val = metrics.get("today_finished_trips")
            if finished_val is None:
                finished_val = metrics.get("today_trips_sent")
            if finished_val is not None:
                try:
                    finished = int(round(float(finished_val)))
                except Exception:
                    finished = None
            acceptance_text = _fmt_percent(_today_acceptance_rate(metrics))

        target_line = f"By {slot_hour:02d}:00 aim for {int(target_by)} finished trips (daily goal {int(target_daily)})."
        if finished is not None:
            diff = int(target_by) - finished
            shortfall_daily = max(int(INTRADAY_DAILY_MIN_FINISHED_ORDERS) - finished, 0)
            praise_hours = {10, 12, 14, 18}
            if diff <= 0:
                praise = "Well done! " if slot_hour in praise_hours else ""
                status_line = f"{praise}You’re at {finished} now—on pace."
            else:
                status_line = f"You’re at {finished} now—need {diff} more by {slot_hour:02d}:00."
                status_line += f" Shortfall to {int(INTRADAY_DAILY_MIN_FINISHED_ORDERS)} today: {shortfall_daily} trips."
        else:
            status_line = _daily_update_hint()

        acceptance_line = f"Acceptance today: {acceptance_text}." if acceptance_text else ""

        goals_line = (
            f"Goals: daily {int(target_daily)} trips (~{int(round(daily_hours_goal))}h), "
            f"weekly {int(round(weekly_trips_val))} trips ({int(round(weekly_hours_val))}h), "
            f"monthly ~{monthly_trips_goal} trips ({monthly_hours_goal}h)."
        )

        progress_line = ""
        if metrics:
            week_trips_val = metrics.get("trip_count")
            progress_bits: List[str] = []
            if finished is not None:
                progress_bits.append(f"{finished} trips today")
            if week_trips_val is not None:
                try:
                    week_trips = int(round(float(week_trips_val)))
                except Exception:
                    week_trips = None
                if week_trips is not None:
                    progress_bits.append(f"{week_trips} trips this week")
            if progress_bits:
                progress_line = f"So far: {', '.join(progress_bits)}."
            else:
                progress_line = "So far: today's trips aren't available yet."
        else:
            progress_line = "So far: today's trips aren't available yet."

        hours_line = ""
        if include_hours:
            ratio = target_by / float(target_daily or 1)
            hours_by = max(1, int(math.ceil(daily_hours_goal * ratio)))
            hours_line = f"Hours target by {slot_hour:02d}:00: about {hours_by}h (daily goal {int(round(daily_hours_goal))}h)."

        checkpoints_line = ""
        if requested_hour is None and not metrics:
            checkpoints = _intraday_checkpoint_targets(target_daily)
            sample = ", ".join([f"{h:02d}:00→{t}" for h, t in checkpoints[:3]])
            checkpoints_line = f"Checkpoints: {sample}."

        return " ".join(
            part for part in [
                goals_line,
                progress_line,
                target_line,
                status_line,
                acceptance_line,
                hours_line,
                checkpoints_line,
            ] if part
        ).strip()

    if not metrics:
        if intent == "progress_update":
            target_hours = targets.get("online_hours")
            target_trips = targets.get("trip_count")
            target_parts: List[str] = []
            val = _fmt_hours(target_hours)
            if val:
                target_parts.append(val)
            val = _fmt_trips(target_trips)
            if val:
                target_parts.append(val)
            target_line = " / ".join(target_parts)
            plan_parts: List[str] = []
            if target_hours:
                hours_per_day = target_hours / 5.0
                hours_day_text = _coerce_goal_value(hours_per_day)
                if hours_day_text:
                    plan_parts.append(f"{hours_day_text}h/day x 5 days")
            if target_hours and target_trips:
                plan_parts.append("aim for 2+ trips/hour")
            plan_line = f"Plan: {', '.join(plan_parts)}." if plan_parts else ""
            pretty = _pretty_reason(reason)
            if pretty:
                prefix = f"I’m still syncing your performance data because {pretty}."
            else:
                prefix = "I’m still syncing your performance data."
            body_parts = [prefix]
            if target_line:
                body_parts.append(f"Target: {target_line}.")
            if plan_line:
                body_parts.append(plan_line)
            if commitment_prompt:
                body_parts.append(_commitment_prompt_line(targets, include_update_hint=True))
            else:
                body_parts.append(f"{_target_update_hint()} {_daily_update_hint()}")
            return " ".join(body_parts).strip()
        pretty = _pretty_reason(reason)
        if pretty:
            return f"I couldn’t pull performance data because {pretty}."
        extra = hotspot_extra()
        if extra:
            return extra
        return _missing_kpi_message()
    period = metrics.get("period_label") or "Latest"

    if intent == "hotspot_summary":
        extra = hotspot_extra()
        if extra:
            return extra
        pretty = _pretty_reason(hotspots_reason) if hotspots_reason else None
        if pretty:
            return (
                f"I couldn’t pull hotspot stats because {pretty}. "
                "In general, busy zones are CBDs, malls, transport hubs, and taxi ranks. "
                "Tell me your usual area and I’ll narrow it down."
            )
        return (
            "I don’t have hotspot stats yet. In general, busy zones are CBDs, malls, "
            "transport hubs, and taxi ranks. Share your usual area and I’ll narrow it down."
        )

    if intent == "progress_update":
        hours = metrics.get("online_hours")
        trips = metrics.get("trip_count")
        acceptance = metrics.get("acceptance_rate")
        summary_parts: List[str] = []
        val = _fmt_hours(hours)
        if val:
            summary_parts.append(f"{val} online")
        val = _fmt_trips(trips)
        if val:
            summary_parts.append(val)
        val = _fmt_percent(acceptance)
        if val:
            summary_parts.append(f"{val} acceptance")
        summary = ", ".join(summary_parts) or "no KPI data yet"

        target_hours = targets.get("online_hours")
        target_trips = targets.get("trip_count")
        target_parts: List[str] = []
        val = _fmt_hours(target_hours)
        if val:
            target_parts.append(val)
        val = _fmt_trips(target_trips)
        if val:
            target_parts.append(val)
        target_line = " / ".join(target_parts)

        progress_parts: List[str] = []
        if hours is not None and target_hours:
            hours_pct = int(round((hours / target_hours) * 100)) if target_hours else None
            if hours_pct is not None:
                progress_parts.append(f"{hours_pct}% hours")
        if trips is not None and target_trips:
            trips_pct = int(round((trips / target_trips) * 100)) if target_trips else None
            if trips_pct is not None:
                progress_parts.append(f"{trips_pct}% trips")
        progress_line = ", ".join(progress_parts)

        remaining_parts: List[str] = []
        if hours is not None and target_hours:
            remaining_hours = max(target_hours - hours, 0)
            remaining_parts.append(f"{int(round(remaining_hours))}h left")
        if trips is not None and target_trips:
            remaining_trips = max(target_trips - trips, 0)
            remaining_parts.append(f"{int(round(remaining_trips))} trips left")
        remaining_line = ", ".join(remaining_parts)

        today_parts: List[str] = []
        today_trips = metrics.get("today_finished_trips")
        if today_trips is None:
            today_trips = metrics.get("today_trips_sent")
        if today_trips is not None:
            today_parts.append(f"{int(round(today_trips))} trips today")
        today_gmv = metrics.get("today_gross_earnings")
        if today_gmv is not None:
            today_parts.append(f"{fmt_rands(today_gmv)} today")
        today_line = f"Today so far: {', '.join(today_parts)}." if today_parts else ""

        lines = [f"This week so far: {summary}."]
        if today_line:
            lines.append(today_line)
        if target_line:
            target_sentence = f"Target: {target_line}"
            if progress_line:
                target_sentence += f" ({progress_line})"
            if remaining_line:
                target_sentence += f"; remaining {remaining_line}"
            lines.append(target_sentence + ".")
        elif progress_line:
            lines.append(f"Progress: {progress_line}.")

        plan_parts: List[str] = []
        if target_hours:
            hours_per_day = target_hours / 5.0
            hours_day_text = _coerce_goal_value(hours_per_day)
            if hours_day_text:
                plan_parts.append(f"{hours_day_text}h/day x 5 days")
        if target_hours and target_trips:
            plan_parts.append("aim for 2+ trips/hour")
        if plan_parts:
            lines.append(f"Plan: {', '.join(plan_parts)}.")
        if commitment_prompt:
            lines.append(_commitment_prompt_line(targets, include_update_hint=True))
        else:
            lines.append(f"{_target_update_hint()} {_daily_update_hint()}")
        return " ".join(lines).strip()

    if intent == "performance_summary":
        parts: List[str] = []
        val = _fmt_hours(metrics.get("online_hours"))
        if val: parts.append(f"{val} online")
        val = _fmt_percent(metrics.get("acceptance_rate"))
        if val: parts.append(f"{val} acceptance")
        val = metrics.get("gross_earnings")
        if val is not None: parts.append(f"{fmt_rands(val)} gross")
        val = metrics.get("earnings_per_hour")
        if val is not None: parts.append(f"{fmt_rands(val)} per hour")
        val = _fmt_trips(metrics.get("trip_count"))
        if val: parts.append(val)
        summary = ", ".join(parts) or "no KPI data yet"
        feedback = _kpi_feedback(metrics, targets)
        extra = hotspot_extra()
        return f"{period} snapshot: {summary}. {feedback} {extra}".strip()

    def combine(msg1: str, msg2: str) -> str:
        extra = hotspot_extra()
        return f"{msg1} {msg2} {extra}".strip()

    if intent == "online_hours_status":
        value = metrics.get("online_hours")
        if value is None:
            extra = hotspot_extra()
            return extra or _missing_kpi_message()
        msg1 = f"You’re on {_fmt_hours(value)} online so far."
        target = targets.get("online_hours")
        if target:
            gap = target - value
            msg2 = f"Try to add about {max(1, int(round(gap)))}h to hit {int(round(target))}h." if gap > KPI_GAP_THRESHOLDS["online_hours"] else "That’s on pace for the weekly goal 👏"
        else:
            msg2 = "Keep the wheel turning and I’ll keep tracking it."
        return combine(msg1, msg2)

    if intent == "acceptance_rate_status":
        value = metrics.get("acceptance_rate")
        if value is None:
            extra = hotspot_extra()
            return extra or _missing_kpi_message()
        if _mentions_long_pickup_issue(msg_lower):
            target = targets.get("acceptance_rate")
            target_hint = f" Aim for {_fmt_percent(target)} by reducing long pickups." if target else ""
            msg1 = f"Acceptance is around {_fmt_percent(value)}—long pickups can burn fuel and drag it down.{target_hint}"
            msg2 = (
                "Try staying closer to hotspots and repositioning after each drop to shorten pickup distance; "
                "tell me your area and I’ll suggest zones."
            )
            return f"{msg1} {msg2}".strip()
        msg1 = f"Acceptance is tracking about {_fmt_percent(value)}."
        target = targets.get("acceptance_rate")
        if target:
            gap = target - value
            msg2 = f"Let’s aim for {_fmt_percent(target)} by replying within 15s and declining slow pings fast." if gap > KPI_GAP_THRESHOLDS["acceptance_rate"] else "That keeps you in the safe zone."
        else:
            msg2 = "Stay quick on your responses and you’ll be good."
        return combine(msg1, msg2)

    if intent == "earnings_per_hour_status":
        value = metrics.get("earnings_per_hour")
        if value is None:
            extra = hotspot_extra()
            return extra or _missing_kpi_message()
        msg1 = f"Earnings per hour sits around {fmt_rands(value)}."
        target = targets.get("earnings_per_hour")
        if target:
            gap = target - value
            msg2 = f"Let’s chase surge zones to lift that toward {fmt_rands(target)}." if gap > KPI_GAP_THRESHOLDS["earnings_per_hour"] else "That’s a solid hourly rate."
        else:
            msg2 = "Stay close to high-demand pockets to keep it swinging upward."
        return combine(msg1, msg2)

    if intent == "weekly_earnings_status":
        value = metrics.get("gross_earnings")
        if value is None:
            extra = hotspot_extra()
            return extra or _missing_kpi_message()
        msg1 = f"Gross earnings total {fmt_rands(value)} so far."
        target = targets.get("gross_earnings")
        if target:
            gap = target - value
            msg2 = f"One more strong day should get you near {fmt_rands(target)}." if gap > KPI_GAP_THRESHOLDS["gross_earnings"] else "That’s right on track—nice momentum!"
        else:
            msg2 = "Keep stacking peak hours to grow it."
        return combine(msg1, msg2)

    if intent == "trip_count_status":
        value = metrics.get("trip_count")
        if value is None:
            extra = hotspot_extra()
            return extra or _missing_kpi_message()
        msg1 = f"You’ve completed {_fmt_trips(value)} this week."
        target = targets.get("trip_count")
        if target:
            gap = target - value
            msg2 = f"Stay online through the evening rush to reach {int(round(target))}." if gap > KPI_GAP_THRESHOLDS["trip_count"] else "That’s on pace for the target."
        else:
            msg2 = "Keep stacking rides when demand spikes."
        return combine(msg1, msg2)

    if intent == "top_driver_tips":
        gap = _primary_gap(metrics, targets)
        if gap:
            key, diff = gap
            tipper = KPI_TIPS.get(key)
            target = targets.get(key)
            if tipper and target is not None:
                tip = tipper(diff, target)
                extra = hotspot_extra()
                return f"{period} focus tip: {tip} Keep logging your runs and I’ll keep an eye on the numbers. {extra}".strip()
        extra = hotspot_extra()
        return f"You’re hitting the key targets—keep stacking peak slots and bonus runs 🚀 {extra}".strip()

    extra = hotspot_extra()
    if extra:
        return extra
    return _missing_kpi_message()


# -----------------------------------------------------------------------------
# Greeting controls + softening
# -----------------------------------------------------------------------------
_GREETING_RE = re.compile(
    r"^\s*(hi|hey|hello|hola|holla|hi there|hey there|hello there)\b[!,.:\-\s]*",
    re.IGNORECASE,
)

_GREETING_TOKENS = {
    "hi","hey","hello","hola","holla","morning","afternoon","evening","there",
    "hiya","hiya","howzit","howdy"
}

_GREETING_PHRASES = {
    "hi there","hey there","hello there","good morning","good afternoon","good evening"
}

GREETING_RESPONSES = [
    "How can I help today?",
    "What do you want to check in on?",
    "I'm here - what should we look at?",
    "Ready when you are. What do you need?",
]

GREETING_RESPONSES_WITH_REASON = [
    "Still couldn't reach the stats earlier. Want me to try a different metric?",
    "I'm still missing that data from earlier. Want to dig into something else?",
    "No update on the stats yet, but I can help with the next steps.",
]

SMALLTALK_RESPONSES = [
    "Doing okay here. How's the shift going?",
    "All good here. What do you want to tackle?",
    "I'm here. What's the main thing you need?",
    "I'm around if you need anything right now.",
]

def _is_greeting_message(raw: str) -> bool:
    if not raw:
        return False
    stripped = raw.strip().lower()
    if not stripped:
        return False
    if stripped in {":)",":-)","😊","👋","👋🏻","👋🏾"}:
        return True
    words = re.sub(r"[^\w\s]", " ", stripped)
    words = re.sub(r"\s+", " ", words).strip()
    if not words:
        return False
    if words in _GREETING_PHRASES:
        return True
    tokens = words.split()
    if len(tokens) <= 3 and all(tok in _GREETING_TOKENS for tok in tokens):
        return True
    return False

def _strip_leading_greeting_or_name(body: str, full_name: str, first_name: str) -> str:
    if not body: return body
    candidates = [full_name or "", first_name or ""]
    patterns = []
    for n in {c.strip() for c in candidates if c}:
        patterns += [
            rf"^\s*(?:hi|hey|hello|hola|holla)\s+{re.escape(n)}\b[!,.:\-\s]*",
            rf"^\s*{re.escape(n)}\b[!,.:\-\s]*",
        ]
    patterns += [r"^\s*(?:hi|hey|hello|hola|holla)\b[!,.:\-\s]*"]
    for pat in patterns:
        body = re.sub(pat, "", body, flags=re.IGNORECASE)
    return body.lstrip()

def _jhb_today_str() -> str:
    try:
        return jhb_now().date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()

def should_greet(ctx: dict) -> bool:
    return ctx.get("_last_greet_date") != _jhb_today_str()

def with_greet(ctx: dict, name: str, body: str, intent: str, *, force: bool = False) -> str:
    first = (name or "").split()[0] or "there"
    body = _strip_leading_greeting_or_name(body or "", name or "", first)
    if force or should_greet(ctx):
        ctx["_last_greet_date"] = _jhb_today_str()
        return f"Hi {first}, {body}"
    return body

def soften_reply(text: str, first_name: str = "") -> str:
    if not text: return text
    s = text
    rep = {
        r"\bensure\b": "try to",
        r"\byou must\b": "you could",
        r"\byou should\b": "you could",
        r"\byou need to\b": "you might want to",
        r"\balways\b": "often",
        r"\bnever\b": "usually not",
        r"\bcheck\b": "take a look at",
    }
    for pat, sub in rep.items():
        s = re.sub(pat, sub, s, flags=re.IGNORECASE)
    contractions = {
        r"\bI am\b": "I'm",
        r"\bI will\b": "I'll",
        r"\bI have\b": "I've",
        r"\bwe are\b": "we're",
        r"\bwe will\b": "we'll",
        r"\byou are\b": "you're",
        r"\byou will\b": "you'll",
        r"\bdo not\b": "don't",
        r"\bcannot\b": "can't",
        r"\bcan not\b": "can't",
        r"\bit is\b": "it's",
        r"\bthat is\b": "that's",
        r"\bthere is\b": "there's",
    }
    for pat, sub in contractions.items():
        s = re.sub(pat, sub, s, flags=re.IGNORECASE)
    s = re.sub(r"([\U0001F300-\U0001FAFF]){2,}", r"\1", s)
    return s.strip()

# -----------------------------------------------------------------------------
# Logging to whatsapp_message_logs (schema-aware)
# -----------------------------------------------------------------------------
SYNONYMS = {
    "message_text":      ["message_text", "message"],
    "message_direction": ["message_direction", "direction"],
    "timestamp":         ["timestamp", "logged_at", "message_timestamp"],
    "origin_type":       ["origin_type", "origin", "source"],
    "wa_message_id":     ["wa_message_id", "wa_msg_id", "whatsapp_message_id"],
    "business_number":   ["business_number", "display_phone_number"],
    "phone_number_id":   ["phone_number_id", "waba_phone_id", "phone_id"],
    "created_at":        ["created_at", "logged_at", "createdAt"],
    "ai_raw_json":       ["ai_raw_json","raw_ai","ai_json"],
}

def _detect_logs_table(conn) -> str:
    plural = f"{MYSQL_DB}.whatsapp_message_logs"
    singular = f"{MYSQL_DB}.whatsapp_message_log"
    if _table_exists(conn, plural): return plural
    if _table_exists(conn, singular): return singular
    return plural

def _debug_log_table_columns(conn, table: str, available: set[str]):
    if LOG_DB_INSERTS:
        log.info("[DB-LOG] using table=%s available_cols=%s", table, ", ".join(sorted(available)))

def log_message(
    *,
    direction: str,
    wa_id: str,
    text: Optional[str],
    intent: Optional[str]=None,
    status: Optional[str]=None,
    wa_message_id: Optional[str]=None,
    driver_id: Optional[int]=None,
    message_id: Optional[str]=None,
    business_number: Optional[str]=None,
    phone_number_id: Optional[str]=None,
    origin_type: Optional[str]=None,
    media_url: Optional[str]=None,
    raw_json: Optional[dict]=None,
    timestamp_unix: Optional[str]=None,
    sentiment: Optional[str]=None,
    sentiment_score: Optional[float]=None,
    intent_label: Optional[str]=None,
    ai_raw: Optional[Dict[str, Any]]=None,
    conversation_id: Optional[str]=None,
    response_time_sec: Optional[float]=None,
) -> None:
    if not mysql_available():
        return
    try:
        conn = get_mysql()
        table = _detect_logs_table(conn)
        available = _get_table_columns(conn, table)
        _debug_log_table_columns(conn, table, available)

        ts_str = unix_to_jhb_str(timestamp_unix) or jhb_now().strftime("%Y-%m-%d %H:%M:%S")

        payload = {
            "message_id":        message_id,
            "wa_message_id":     wa_message_id or message_id,
            "conversation_id":   conversation_id,
            "wa_id":             wa_id,
            "driver_id":         driver_id,
            "phone":             wa_id,
            "message_direction": direction,
            "message_text":      text,
            "intent":            intent,
            "media_url":         media_url,
            "status":            status,
            "origin_type":       origin_type or "whatsapp",
            "timestamp":         ts_str,
            "business_number":   business_number,
            "phone_number_id":   phone_number_id,
            "response_time_sec": response_time_sec,
            "created_at":        ts_str,
            "sentiment":         sentiment,
            "sentiment_score":   sentiment_score,
            "intent_label":      intent_label,
            "ai_raw_json":       json.dumps(ai_raw, ensure_ascii=False) if ai_raw else None,
            "raw_json":          json.dumps(raw_json, ensure_ascii=False) if raw_json else None,
        }

        cols, vals = [], []
        for key, value in payload.items():
            if key in available:
                cols.append(key); vals.append(value); continue
            if key in SYNONYMS:
                pick = next((c for c in SYNONYMS[key] if c in available), None)
                if pick:
                    cols.append(pick); vals.append(value)

        if not cols:
            log.error("[DB-LOG] No matching columns found for %s", table)
            return

        if LOG_DB_INSERTS:
            safe_dbg = {k: payload.get(k) for k in SAFE_LOG_COLUMNS if k in payload}
            log.info("[DB-LOG] INSERT %s cols=%s safe_vals=%s", table, cols, safe_dbg)

        placeholders = ", ".join(["%s"] * len(cols))
        collist = ", ".join(cols)
        sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders})"
        with conn.cursor() as cur:
            cur.execute(sql, vals)

        # verify by message_id if present
        verify_value = wa_message_id or message_id
        verify_col = None
        if "wa_message_id" in available and verify_value: verify_col = "wa_message_id"
        elif "message_id" in available and verify_value: verify_col = "message_id"
        if LOG_DB_INSERTS and verify_col:
            with conn.cursor() as cur:
                cur.execute(f"SELECT id, {verify_col}, message_direction, status FROM {table} "
                            f"WHERE {verify_col}=%s ORDER BY id DESC LIMIT 1", (verify_value,))
                row = cur.fetchone()
            log.info("[DB-LOG] verify %s=%s -> %s", verify_col, verify_value, row)

    except Exception as e:
        log.error("Failed to log message: %s", e)

# -----------------------------------------------------------------------------
# WhatsApp send
# -----------------------------------------------------------------------------
def send_whatsapp_text(wa_id: str, text: str) -> Optional[str]:
    if not (WABA_TOKEN and WABA_PHONE_ID):
        log.warning("Skipping send — WABA creds missing.")
        return None
    headers = {"Authorization": f"Bearer {WABA_TOKEN}", "Content-Type":"application/json"}
    payload = {"messaging_product":"whatsapp","to":wa_id,"type":"text","text":{"preview_url":False,"body":text}}
    try:
        r = requests.post(GRAPH_URL, headers=headers, json=payload, timeout=30)
    except requests.RequestException as exc:
        log.error("WA send request failed: %s", exc)
        return None
    try:
        data = r.json()
    except Exception:
        data = {}
    if r.status_code != 200:
        log.error("WA send failed: %s %s", r.status_code, r.text)
        return None
    log.info("✅ WhatsApp API: %s sent.", r.status_code)
    try:
        return data.get("messages", [{}])[0].get("id")
    except Exception:
        return None


def upload_whatsapp_media(file: UploadFile) -> Optional[str]:
    if not (WABA_TOKEN and WABA_PHONE_ID):
        log.warning("Skipping media upload — WABA creds missing.")
        return None
    try:
        content = file.file.read()
    except Exception as exc:
        log.error("Media read failed: %s", exc)
        return None
    if not content:
        return None
    if len(content) > 10 * 1024 * 1024:  # 10 MB cap
        log.warning("Media too large to upload (>10MB)")
        return None
    content_type = file.content_type or mimetypes.guess_type(file.filename or "")[0] or "application/octet-stream"
    media_endpoint = f"https://graph.facebook.com/v19.0/{WABA_PHONE_ID}/media"
    headers = {"Authorization": f"Bearer {WABA_TOKEN}"}
    files = {"file": (file.filename or "attachment", content, content_type)}
    data = {"messaging_product": "whatsapp"}
    try:
        resp = requests.post(media_endpoint, headers=headers, files=files, data=data, timeout=30)
    except Exception as exc:
        log.error("Media upload failed: %s", exc)
        return None
    if resp.status_code != 200:
        log.error("Media upload failed: %s %s", resp.status_code, resp.text)
        return None
    try:
        return resp.json().get("id")
    except Exception:
        return None


def send_whatsapp_template(
    wa_id: str,
    template_name: str,
    language: str = "en",
    body_params: Optional[Any] = None,
    media_id: Optional[str] = None,
    *,
    param_names: Optional[List[str]] = None,
    parameter_format: Optional[str] = None,
) -> Optional[str]:
    if not (WABA_TOKEN and WABA_PHONE_ID):
        log.warning("Skipping send — WABA creds missing.")
        return None
    headers = {"Authorization": f"Bearer {WABA_TOKEN}", "Content-Type": "application/json"}
    components: List[Dict[str, Any]] = []
    if body_params:
        params_payload: List[Dict[str, Any]] = []
        param_format = str(parameter_format or "").upper()
        use_named = param_format == "NAMED"
        if not use_named and not param_format and param_names:
            use_named = True
        if use_named:
            if isinstance(body_params, dict):
                for key, value in body_params.items():
                    name = str(key or "").strip()
                    if not name:
                        continue
                    params_payload.append({"type": "text", "text": str(value), "parameter_name": name})
            elif param_names:
                for idx, value in enumerate(body_params):
                    name = str(param_names[idx] if idx < len(param_names) else "").strip()
                    if not name:
                        continue
                    params_payload.append({"type": "text", "text": str(value), "parameter_name": name})
            else:
                log.error("Template %s requires named params but none were provided.", template_name)
                return None
        else:
            params_payload = [{"type": "text", "text": str(v)} for v in body_params]
        components.append(
            {
                "type": "body",
                "parameters": params_payload,
            }
        )
    if media_id:
        components.append(
            {
                "type": "header",
                "parameters": [{"type": "document", "document": {"id": media_id}}],
            }
        )
    payload = {
        "messaging_product": "whatsapp",
        "to": wa_id,
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": language or "en"},
            "components": components,
        },
    }
    try:
        r = requests.post(GRAPH_URL, headers=headers, json=payload, timeout=30)
    except requests.RequestException as exc:
        log.error("WA template send failed: %s", exc)
        return None
    try:
        data = r.json()
    except Exception:
        data = {}
    if r.status_code != 200:
        log.error(
            "WA template send failed: %s %s | template=%s format=%s names=%s params=%s",
            r.status_code,
            r.text,
            template_name,
            parameter_format,
            param_names,
            body_params,
        )
        return None
    log.info("✅ WhatsApp template sent: %s", template_name)
    try:
        return data.get("messages", [{}])[0].get("id")
    except Exception:
        return None


def trigger_click_to_dial(number: str) -> bool:
    if not (ISSABEL_CLICK2CALL_URL and ISSABEL_USERNAME and ISSABEL_PASSWORD and ISSABEL_EXTENSION):
        log.warning("Click-to-dial skipped — Issabel config missing.")
        return False
    payload = {"ext": ISSABEL_EXTENSION, "number": number}
    try:
        resp = requests.post(
            ISSABEL_CLICK2CALL_URL,
            auth=(ISSABEL_USERNAME, ISSABEL_PASSWORD),
            json=payload,
            timeout=15,
            verify=ISSABEL_VERIFY_TLS,
        )
    except Exception as exc:
        log.error("Click-to-dial failed: %s", exc)
        return False
    if resp.status_code not in (200, 201, 204):
        log.error("Click-to-dial failed: %s %s", resp.status_code, resp.text)
        return False
    return True
def fetch_media_url(media_id: Optional[str]) -> Optional[str]:
    if not media_id:
        return None
    if not WABA_TOKEN:
        log.warning("Cannot fetch media URL without WABA token.")
        return None
    media_endpoint = f"https://graph.facebook.com/v19.0/{media_id}"
    headers = {"Authorization": f"Bearer {WABA_TOKEN}"}
    try:
        resp = requests.get(media_endpoint, headers=headers, params={"fields": "url"}, timeout=30)
    except Exception as exc:
        log.warning("Media metadata fetch failed: %s", exc)
        return None
    if resp.status_code != 200:
        log.warning("Media metadata fetch failed: %s %s", resp.status_code, resp.text)
        return None
    try:
        data = resp.json()
    except Exception as exc:
        log.warning("Media metadata JSON decode failed: %s", exc)
        return None
    return data.get("url")

def download_media_bytes(media_url: Optional[str]) -> Optional[bytes]:
    if not media_url:
        return None
    if not WABA_TOKEN:
        log.warning("Cannot download media without WABA token.")
        return None
    headers = {"Authorization": f"Bearer {WABA_TOKEN}"}
    try:
        resp = requests.get(media_url, headers=headers, timeout=30)
    except Exception as exc:
        log.warning("Media download failed: %s", exc)
        return None
    if resp.status_code != 200:
        log.warning("Media download failed: %s %s", resp.status_code, resp.text)
        return None
    return resp.content

def transcribe_audio_bytes(data: bytes, *, mime_type: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    if not data:
        return None, "empty_audio"
    if not OPENAI_API_KEY:
        return None, "missing_api_key"
    try:
        from openai import OpenAI
    except Exception as exc:
        log.warning("OpenAI module unavailable for transcription: %s", exc)
        return None, f"module_error:{exc}"

    client = OpenAI(api_key=OPENAI_API_KEY)
    tmp_path = None
    try:
        suffix = _suffix_for_mime(mime_type)
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp.write(data)
            tmp.flush()
            tmp_path = tmp.name
        with open(tmp_path, "rb") as fh:
            resp = client.audio.transcriptions.create(
                model=OPENAI_TRANSCRIBE_MODEL,
                file=fh,
                response_format="text",
            )
        transcript = (resp or "").strip()
        if transcript:
            return transcript, None
    except Exception as exc:
        log.warning("Audio transcription failed: %s", exc)
        return None, f"api_error:{exc}"
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass
    return None, "unknown_error"

def _suffix_for_mime(mime_type: Optional[str]) -> str:
    if not mime_type:
        return ".ogg"
    clean = mime_type.split(";", 1)[0].strip().lower()
    if clean in MIME_SUFFIX_MAP:
        return MIME_SUFFIX_MAP[clean]
    if "/" in clean:
        candidate = clean.split("/", 1)[1]
        if candidate:
            candidate = candidate.strip()
            if candidate:
                return f".{candidate}"
    return ".ogg"

# -----------------------------------------------------------------------------
# Intents
# -----------------------------------------------------------------------------
def detect_intent(text: str, ctx: Optional[Dict[str, Any]] = None) -> str:
    ctx = ctx or {}
    pending = ctx.get("_pending_intent")
    awaiting_projection = pending in {PENDING_EARNINGS_TRIPS, PENDING_EARNINGS_AVG}
    awaiting_repossession = pending == PENDING_REPOSSESSION_REASON
    awaiting_no_vehicle = pending == PENDING_NO_VEHICLE_REASON
    awaiting_target_update = bool(ctx.get("_awaiting_target_update"))
    accident_ctx = ctx.get("_accident_case") if isinstance(ctx.get("_accident_case"), dict) else None
    accident_status = accident_ctx.get("status") if accident_ctx else None
    accident_awaiting = accident_ctx.get("awaiting") if accident_ctx else None
    accident_closed = bool(accident_ctx.get("closed")) if accident_ctx else False
    accident_waiting = (accident_status in {"collecting", "pending_ops"}) and bool(accident_awaiting) and not accident_closed
    cash_ticket_ctx = ctx.get("_cash_ticket") if isinstance(ctx.get("_cash_ticket"), dict) else None
    cash_waiting = bool(
        cash_ticket_ctx
        and not cash_ticket_ctx.get("closed")
        and cash_ticket_ctx.get("awaiting_pop", True)
    )

    text = _normalize_text(text or "")
    raw = text.strip()
    lowered = raw.lower()
    if not lowered:
        if awaiting_projection:
            return "earnings_projection"
        if accident_waiting:
            return ACCIDENT_REPORT_INTENT
        if cash_waiting:
            return CASH_BALANCE_UPDATE_INTENT
        return "unknown"

    if _is_global_opt_out(raw):
        return "opt_out"
    if _is_global_opt_in(raw):
        return "opt_in"

    if _is_name_question(raw):
        return "assistant_identity"

    if _is_oph_definition_question(raw):
        return "oph_definition"
    if _is_platform_exclusivity_question(raw):
        return "platform_exclusivity"

    if _is_account_suspension(raw):
        return "account_suspension"
    if _is_app_issue(raw):
        return "app_issue"
    if _is_balance_dispute(raw):
        return "balance_dispute"
    if _is_low_demand_issue(raw):
        return "low_demand"
    if _is_hotspot_query(raw):
        return "hotspot_summary"

    if _is_vehicle_back(raw) and not _is_no_vehicle(raw):
        return "vehicle_back"

    if ctx.get("_medical_pending_location"):
        return "medical_issue"
    if ctx.get("_no_vehicle_pending"):
        return "no_vehicle"
    if ctx.get("_balance_dispute_pending"):
        return "balance_dispute"
    if ctx.get("_low_demand_pending"):
        return "low_demand"
    area_followup = _extract_area_candidates(raw)
    if ctx.get("_low_demand_ticket") and area_followup:
        return "low_demand"
    if ctx.get("_low_demand_areas") and area_followup:
        return "low_demand"
    if (ctx.get("_active_concern") or {}).get("type") == "low_demand" and area_followup:
        return "low_demand"
    if ctx.get("_low_demand_ticket") and _looks_like_address(raw):
        return "low_demand"

    digits_only = re.sub(r"\D", "", lowered)
    if "personal code" in lowered or (digits_only and (digits_only == lowered or lowered.replace(" ", "") == digits_only)):
        if 8 <= len(digits_only) <= 16:
            return "provide_personal_code"

    if awaiting_projection:
        if _extract_trip_count(raw, allow_plain=True) is not None:
            return "earnings_projection"
        if _extract_earnings_per_trip(raw, allow_plain=True) is not None:
            return "earnings_projection"
    if _is_kpi_daily_target_query(raw):
        return "daily_target_status"
    if _is_remaining_kpi_query(raw):
        recent_intent = ctx.get("_last_kpi_intent")
        recent_at = ctx.get("_last_kpi_intent_at") or 0
        if recent_intent in KPI_INTENTS and (time.time() - recent_at) < 6 * 3600:
            return "daily_target_status"
        if ctx.get("_intraday_updates_enabled"):
            return "daily_target_status"
    if awaiting_repossession:
        if _is_greeting_message(raw):
            return "greeting"
        if lowered in ACKNOWLEDGEMENT_TOKENS or _is_positive_confirmation(raw):
            return "acknowledgement"
        recent_intent = ctx.get("_last_kpi_intent")
        recent_at = ctx.get("_last_kpi_intent_at") or 0
        if _is_positive_confirmation(raw) and recent_intent in KPI_INTENTS and (time.time() - recent_at) < 6 * 3600:
            return "acknowledgement"
        if awaiting_target_update and _has_goal_update_values(raw, allow_plain=True):
            return "target_update"
        if _parse_repossession_reason(raw):
            return "vehicle_repossession"
        if _is_target_benefit_query(raw):
            return "target_benefit"

    if awaiting_no_vehicle:
        if _is_vehicle_back(raw) and not _is_no_vehicle(raw):
            return "vehicle_back"
        if _is_medical_issue(raw):
            return "medical_issue"
        if _is_vehicle_repossession(raw):
            return "vehicle_repossession"
        if _parse_no_vehicle_reason(raw):
            return "no_vehicle"

    if accident_waiting:
        stripped = lowered.strip()
        if stripped:
            stripped = re.sub(r"[.!?]+$", "", stripped)
        if stripped in ACKNOWLEDGEMENT_TOKENS or stripped in YES_TOKENS or stripped in NO_TOKENS:
            return ACCIDENT_REPORT_INTENT

    if any(kw in lowered for kw in BOT_IDENTITY_KEYWORDS):
        return "bot_identity"

    if any(kw in lowered for kw in FUEL_INTENT_KEYWORDS):
        return "fuel_estimate"

    if any(kw in lowered for kw in EARNINGS_TIPS_KEYWORDS):
        return "earnings_tips"
    if (("tip" in lowered) or ("tips" in lowered)) and re.search(r"\b(earn|earnings|earning|income|money)\b", lowered):
        return "earnings_tips"

    if any(kw in lowered for kw in EARNINGS_INTENT_KEYWORDS):
        return "earnings_projection"

    if "acceptance" in lowered:
        return "acceptance_rate_status"

    if _is_schedule_update(raw):
        return "schedule_update"

    if _is_target_benefit_query(raw):
        return "target_benefit"
    if awaiting_target_update and _has_goal_update_values(raw, allow_plain=True):
        return "target_update"

    if re.search(r"\b\d+(?:\.\d+)?\s*(?:per\s*hour|/hour|ph)\b", lowered):
        return "earnings_per_hour_status" if not awaiting_projection else "earnings_projection"

    if any(kw in lowered for kw in CONCERN_COST_KEYWORDS):
        return "cost_concern"

    if any(kw in lowered for kw in CASH_ENABLE_KEYWORDS):
        return CASH_ENABLE_INTENT

    if any(kw in lowered for kw in CASH_BALANCE_UPDATE_KEYWORDS):
        return CASH_BALANCE_UPDATE_INTENT

    if any(kw in lowered for kw in BRANDING_BONUS_KEYWORDS):
        return BRANDING_BONUS_INTENT

    if _is_accident_message(raw):
        return ACCIDENT_REPORT_INTENT

    if _is_medical_issue(raw):
        return "medical_issue"

    if _is_no_vehicle(raw):
        return "no_vehicle"

    if _is_vehicle_repossession(raw):
        return "vehicle_repossession"

    if _is_car_problem(raw):
        return "car_problem"

    if any(kw in lowered for kw in CONCERN_GENERAL_KEYWORDS):
        return "raise_concern"

    if lowered in ACKNOWLEDGEMENT_TOKENS:
        return CASH_BALANCE_UPDATE_INTENT if cash_waiting else "acknowledgement"

    kpi_match = _match_kpi_intent(lowered)
    if kpi_match:
        return kpi_match

    if re.search(r"\bhow\s+are\s+(you|u)\b", lowered) or "how are you today" in lowered or "how you doing" in lowered:
        return "smalltalk"
    if any(k in lowered for k in ["what is the date","date today","today's date","current date"]): return "current_date"
    if any(k in lowered for k in ["what is the time","time now","current time","time today"]):    return "current_time"

    if _is_greeting_message(text):
        return "greeting"
    if any(k in lowered for k in ["account balance","current balance","my balance","outstanding balance",
                            "how much do i owe","what do i owe","balance please","amount due",
                            "account due","what is my balance","my account balance"]):
        return "account_inquiry"
    return "unknown"

# -----------------------------------------------------------------------------
# Xero account inquiry + Personal code support
# -----------------------------------------------------------------------------
PERSONAL_CODE_RE = re.compile(r"\b\d{8,16}\b")

def extract_personal_code(text: str) -> Optional[str]:
    if not text:
        return None
    t = re.sub(r"[^\d]", "", text)
    if 8 <= len(t) <= 16:
        return t
    m = PERSONAL_CODE_RE.search(text)
    return m.group(0) if m else None

def lookup_xero_contact_ids_by_personal_code(code: str) -> list[str]:
    if not (mysql_available() and code):
        return []
    try:
        conn = get_mysql()
        sql = f"""
            SELECT xero_contact_id, xero_contact_id_bjj, xero_contact_id_hakki, xero_contact_id_a49
            FROM {MYSQL_DB}.simplyfleet_driver_backup
            WHERE personal_code = %s
            ORDER BY backup_date DESC
            LIMIT 1
        """
        with conn.cursor() as cur:
            cur.execute(sql, (code,))
            row = cur.fetchone() or {}
        ids = [row.get("xero_contact_id"), row.get("xero_contact_id_bjj"),
               row.get("xero_contact_id_hakki"), row.get("xero_contact_id_a49")]
        ids = [str(x).strip() for x in ids if x]
        seen=set(); out=[]
        for x in ids:
            if x and x not in seen:
                out.append(x); seen.add(x)
        return out
    except Exception as e:
        log.warning("lookup_xero_contact_ids_by_personal_code failed: %s", e)
        return []

def lookup_xero_contact_ids_by_wa(wa_id: str) -> List[str]:
    d = lookup_driver_by_wa(wa_id)
    return d.get("xero_contact_ids", []) or []

def get_latest_xero_outstanding(contact_ids: List[str]) -> Optional[dict]:
    if not (mysql_available() and contact_ids):
        return None
    try:
        conn = get_mysql()
        table = f"{MYSQL_DB}.xero_daily_balance"
        out_col  = _pick_col_exists(conn, table, ["outstanding","amount_due","balance","outstanding_amount"])
        date_col = _pick_col_exists(conn, table, ["as_of_date","date","balance_date","createdAt","created_at",
                                                  "imported_at","timestamp","ts","logged_at"])
        if not out_col: return None
        placeholders = ", ".join(["%s"] * len(contact_ids))
        base = f"SELECT contact_id, {out_col} AS outstanding"
        if date_col: base += f", {date_col} AS as_of"
        sql  = f"{base} FROM {table} WHERE contact_id IN ({placeholders})"
        if date_col: sql += f" ORDER BY {date_col} DESC"
        sql += " LIMIT 1"
        with conn.cursor() as cur:
            cur.execute(sql, contact_ids)
            row = cur.fetchone()
        if not row: return None
        amt = None
        try: amt = float(row.get("outstanding"))
        except Exception: pass
        return {"contact_id": row.get("contact_id"), "outstanding": amt, "as_of": row.get("as_of")}
    except Exception as e:
        log.warning("get_latest_xero_outstanding failed: %s", e)
        return None

def fmt_rands(amount: float) -> str:
    try: return f"R{float(amount):,.2f}"
    except Exception: return "R0.00"

def jhb_today_range() -> Tuple[datetime, datetime]:
    now = jhb_now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def _normalize_wa_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    digits = re.sub(r"\D", "", str(raw))
    if not digits:
        return None
    if digits.startswith("0") and len(digits) >= 10:
        digits = "27" + digits.lstrip("0")
    if digits.startswith("27") and len(digits) == 11:
        return digits
    if len(digits) >= 7:
        return digits
    return None

def _wa_number_variants(raw: Optional[str]) -> List[str]:
    digits = re.sub(r"\D", "", str(raw or ""))
    if not digits:
        return []
    variants: set[str] = set()

    def add_variant(val: Optional[str]) -> None:
        val = (val or "").strip()
        if not val:
            return
        clean = re.sub(r"\D", "", val)
        if clean and len(clean) >= 7:
            variants.add(clean)

    add_variant(digits)

    if digits.startswith("27") and len(digits) > 2:
        local = digits[2:]
        add_variant(local)
        add_variant("0" + local if not local.startswith("0") else local)
    elif digits.startswith("0"):
        stripped = digits.lstrip("0")
        add_variant(stripped)
        add_variant("27" + stripped)
    else:
        add_variant("0" + digits)
        add_variant("27" + digits)

    if len(digits) >= 9:
        last9 = digits[-9:]
        add_variant(last9)
        add_variant("0" + last9)
        add_variant("27" + last9)

    return list(sorted(variants))


def _collect_simplyfleet_statuses(wa_ids: List[str]) -> Dict[str, str]:
    if not wa_ids or not mysql_available():
        return {}
    try:
        conn = get_mysql()
    except Exception:
        return {}
    status_map: Dict[str, str] = {}
    try:
        table = _detect_simplyfleet_table(conn)
        if not table or not _table_exists(conn, table):
            return status_map
        available = _get_table_columns(conn, table)
        wa_col = next((c for c in SIMPLYFLEET_WHATSAPP_COLUMNS if c in available), None)
        status_col = next((c for c in SIMPLYFLEET_STATUS_COLUMNS if c in available), None)
        date_col = next((c for c in SIMPLYFLEET_BACKUP_DATE_COLUMNS if c in available), None)
        if not wa_col or not status_col:
            return status_map
        sanitized_expr = _sanitize_phone_expr(wa_col)
        # Match the same way driver profile lookups do (support 27 / 0 prefixes, etc.)
        digits: set[str] = set()
        for wa in wa_ids:
            if not wa:
                continue
            raw_digits = re.sub(r"\D", "", str(wa))
            if raw_digits:
                digits.add(raw_digits)
            for variant in _wa_number_variants(wa):
                v_digits = re.sub(r"\D", "", str(variant))
                if v_digits:
                    digits.add(v_digits)
        digits = sorted({d for d in digits if len(d) >= 7})
        if not digits:
            return status_map
        placeholders = ", ".join(["%s"] * len(digits))
        if date_col:
            # Fetch the most recent status per WhatsApp number (much cheaper than scanning all backups).
            sanitized_s = _sanitize_phone_expr(f"s.{wa_col}")
            sanitized_t = _sanitize_phone_expr(f"t.{wa_col}")
            inner = (
                f"SELECT {sanitized_s} AS wa_key, MAX(s.{date_col}) AS max_dt "
                f"FROM {table} s "
                f"WHERE {sanitized_s} IN ({placeholders}) "
                f"GROUP BY {sanitized_s}"
            )
            sql = (
                f"SELECT t.{wa_col} AS wa_raw, t.{status_col} AS status "
                f"FROM {table} t "
                f"JOIN ({inner}) latest "
                f"  ON {sanitized_t} = latest.wa_key AND t.{date_col} = latest.max_dt"
            )
        else:
            sql = f"SELECT {wa_col} AS wa_raw, {status_col} AS status FROM {table} WHERE {sanitized_expr} IN ({placeholders})"
        with conn.cursor() as cur:
            cur.execute(sql, digits)
            for row in cur.fetchall() or []:
                wa_raw = row.get("wa_raw")
                normalized = _normalize_wa_id(wa_raw) if wa_raw else None
                if not normalized:
                    continue
                # Keep the most recent status per driver (mirrors driver detail lookup ordering).
                if normalized in status_map:
                    continue
                status_val = row.get("status")
                if status_val:
                    status_map[normalized] = str(status_val)
    except Exception:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return status_map

def _detect_simplyfleet_table(conn) -> Optional[str]:
    for builder in SIMPLYFLEET_TABLE_CANDIDATES:
        try:
            candidate = builder(MYSQL_DB)
            if _table_exists(conn, candidate):
                return candidate
        except Exception:
            continue
    return None


def _fetch_latest_simplyfleet_backup_contacts(conn, driver_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not driver_ids:
        return {}
    table = _detect_simplyfleet_table(conn)
    if not table or not _table_exists(conn, table):
        return {}
    available = _get_table_columns(conn, table)
    driver_col = _pick_col_exists(conn, table, ["bolt_driver_id", "driver_id", "id"])
    if not driver_col:
        return {}
    wa_col = _pick_col_exists(conn, table, SIMPLYFLEET_WHATSAPP_COLUMNS)
    contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]
    collections_col = _pick_col_exists(conn, table, ["collections_agent", "collection_agent", "collections_manager", "collections_owner"])
    date_col = _pick_col_exists(conn, table, SIMPLYFLEET_BACKUP_DATE_COLUMNS)

    cleaned_ids: List[int] = []
    for value in driver_ids:
        if value is None:
            continue
        try:
            cleaned_ids.append(int(value))
        except Exception:
            continue
    if not cleaned_ids:
        return {}
    unique_ids = sorted(set(cleaned_ids))

    select_cols = [driver_col]
    if wa_col:
        select_cols.append(wa_col)
    select_cols.extend(contact_cols)
    if collections_col:
        select_cols.append(collections_col)

    placeholders = ", ".join(["%s"] * len(unique_ids))
    order_clause = f" ORDER BY {driver_col} ASC"
    if date_col:
        order_clause += f", {date_col} DESC"
    sql = (
        f"SELECT {', '.join(select_cols)} FROM {table} "
        f"WHERE {driver_col} IN ({placeholders}){order_clause}"
    )

    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(unique_ids))
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("Failed to fetch backup contacts: %s", exc)
        return {}

    results: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        driver_id_val = row.get(driver_col)
        if driver_id_val is None:
            continue
        try:
            driver_key = int(driver_id_val)
        except Exception:
            continue
        if driver_key in results:
            continue
        wa_raw = row.get(wa_col) if wa_col else None
        wa_id = _normalize_wa_id(wa_raw) if wa_raw else None
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)
        results[driver_key] = {
            "wa_id": wa_id,
            "contact_ids": contact_ids,
            "collections_agent": row.get(collections_col) if collections_col else None,
        }
    return results


def _collect_active_drivers(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    wa_col_candidates = ["whatsapp", "wa_id", "whatsapp_number", "driver_phone", "phone", "phone_number"]
    wa_col = next((c for c in wa_col_candidates if c in available), None)
    if not wa_col:
        return []
    contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]
    name_col = next((c for c in ["display_name", "driver_name", "name", "full_name"] if c in available), None)
    status_col = next((c for c in ["sf_status", "driver_status", "status", "portal_status"] if c in available), None)
    kpi_cols = [
        "total_online_hours",
        "online_hours",
        "total_gmv",
        "gross_earnings",
        "total_finished_orders",
        "total_trips_sent",
        "trip_count",
        "finished_trips",
        "total_trips_accepted",
        "acceptance_rate",
        "acceptance_pct",
        "eph",
        "xero_balance",
        "payments",
        "7D_payments",
        "total_payments",
        "bolt_wallet_payouts",
        "yday_wallet_balance",
        "total_trip_distance",
        "total_tracking_distance",
    ]

    select_cols: List[str] = []
    def add_col(col: Optional[str]) -> None:
        if not col or col in select_cols:
            return
        if col not in available:
            return
        select_cols.append(col)

    add_col(wa_col)
    add_col(name_col)
    add_col(status_col)
    add_col("sf_status")
    add_col("report_date")
    add_col("driver_id")
    add_col("model")
    add_col("car_reg_number")
    add_col("contract_start_date")
    collections_col = next((c for c in ["collections_agent", "collection_agent", "collections_manager", "collections_owner"] if c in available), None)
    add_col(collections_col)
    for col in contact_cols:
        add_col(col)
    for col in kpi_cols:
        add_col(col)
    add_col("rental_balance")

    order_clause = f" ORDER BY report_date DESC" if "report_date" in available else ""
    where_clauses: List[str] = []
    where_params: List[Any] = []
    if status_col:
        placeholders = ", ".join(["%s"] * len(ACTIVE_STATUS_VALUES))
        where_clauses.append(f"LOWER({status_col}) IN ({placeholders})")
        where_params.extend([status.lower() for status in ACTIVE_STATUS_VALUES])
    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    sql = f"SELECT {', '.join(select_cols)} FROM {table}{where_sql}{order_clause}"
    params = where_params
    if max_rows and max_rows > 0:
        sql = f"{sql} LIMIT %s"
        params = params + [max_rows]

    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("Failed to collect active drivers: %s", exc)
        return []

    driver_ids: List[int] = []
    for row in rows:
        driver_id = row.get("driver_id")
        if driver_id is None:
            continue
        try:
            driver_ids.append(int(driver_id))
        except Exception:
            continue

    backup_contacts = _fetch_latest_simplyfleet_backup_contacts(conn, driver_ids)

    drivers: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        driver_id_val = row.get("driver_id")
        driver_id: Optional[int]
        if driver_id_val is None:
            driver_id = None
        else:
            try:
                driver_id = int(driver_id_val)
            except Exception:
                driver_id = None

        backup = backup_contacts.get(driver_id) if driver_id is not None else None

        wa_raw = row.get(wa_col)
        wa_id = _normalize_wa_id(wa_raw)
        if not wa_id and backup and backup.get("wa_id"):
            wa_id = backup["wa_id"]
        if not wa_id:
            continue

        def _to_date(val):
            if not val:
                return None
            try:
                return datetime.fromisoformat(str(val)[:10])
            except Exception:
                try:
                    return datetime.strptime(str(val)[:10], "%Y-%m-%d")
                except Exception:
                    return None

        existing = drivers.get(wa_id)
        existing_date = _to_date(existing.get("last_synced_at")) if existing else None
        new_date = _to_date(row.get("report_date"))
        use_row = True
        if existing and existing_date and new_date and new_date <= existing_date:
            use_row = False

        if not existing or use_row:
            entry = drivers.setdefault(wa_id, {
                "wa_id": wa_id,
                "contact_ids": [],
                "display_name": "",
                "status": "unknown",
                "sf_status": None,
                "last_synced_at": None,
                "collections_agent": None,
                "xero_balance": None,
                "payments": None,
                "bolt_wallet_payouts": None,
                "yday_wallet_balance": None,
                "total_trip_distance": None,
                "total_tracking_distance": None,
            })
        else:
            entry = existing
            continue

        contacts = entry.get("contact_ids") or []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contacts:
                contacts.append(sval)
        if not contacts and backup:
            contacts.extend(backup.get("contact_ids") or [])
        entry["contact_ids"] = contacts
        if not entry.get("display_name"):
            entry["display_name"] = str(row.get(name_col) or "").strip() or entry["display_name"]

        entry["sf_status"] = row.get("sf_status") or entry.get("sf_status")
        if entry["sf_status"]:
            entry["status"] = str(entry["sf_status"]).strip()
        elif status_col and row.get(status_col):
            entry["status"] = str(row.get(status_col)).strip()

        if row.get("report_date"):
            entry["last_synced_at"] = row.get("report_date")

        for label in ["total_online_hours", "online_hours"]:
            if label in available and row.get(label) is not None:
                entry["online_hours"] = _coerce_float(row.get(label))
                break
        gross = None
        for label in ["total_gmv", "gross_earnings"]:
            if label in available and row.get(label) is not None:
                gross = _coerce_float(row.get(label))
                break
        if gross is not None:
            entry["gross_earnings"] = gross

        for label in ["total_finished_orders", "finished_trips", "trip_count", "total_trips_sent"]:
            if label in available and row.get(label) is not None:
                entry["trip_count"] = _coerce_float(row.get(label))
                break
        if "total_finished_orders" in available and row.get("total_finished_orders") is not None:
            entry["finished_trips"] = _coerce_float(row.get("total_finished_orders"))
        elif "finished_trips" in available and row.get("finished_trips") is not None:
            entry["finished_trips"] = _coerce_float(row.get("finished_trips"))
        if "model" in available and row.get("model"):
            entry["model"] = row.get("model")
        if "car_reg_number" in available and row.get("car_reg_number"):
            entry["car_reg_number"] = row.get("car_reg_number")
        if "contract_start_date" in available and row.get("contract_start_date"):
            entry["contract_start_date"] = row.get("contract_start_date")
        if collections_col and row.get(collections_col):
            entry["collections_agent"] = row.get(collections_col)
        if not entry.get("collections_agent") and backup and backup.get("collections_agent"):
            entry["collections_agent"] = backup.get("collections_agent")

        if "acceptance_rate" in available and row.get("acceptance_rate") is not None:
            entry["acceptance_rate"] = _coerce_pct(row.get("acceptance_rate"))
        elif "acceptance_pct" in available and row.get("acceptance_pct") is not None:
            entry["acceptance_rate"] = _coerce_pct(row.get("acceptance_pct"))

        if "eph" in available and row.get("eph") is not None:
            entry["earnings_per_hour"] = _coerce_float(row.get("eph"))

        for col_key, target_key in [
            ("xero_balance", "xero_balance"),
            ("bolt_wallet_payouts", "bolt_wallet_payouts"),
            ("yday_wallet_balance", "yday_wallet_balance"),
            ("total_trip_distance", "total_trip_distance"),
            ("total_tracking_distance", "total_tracking_distance"),
            ("rental_balance", "rental_balance"),
        ]:
            if col_key in available and row.get(col_key) is not None:
                entry[target_key] = _coerce_float(row.get(col_key))

        payments_val = None
        for key in ("payments", "7D_payments", "total_payments"):
            if key in available and row.get(key) is not None:
                payments_val = _coerce_float(row.get(key))
                break
        if payments_val is not None:
            entry["payments"] = payments_val

        entry["driver_id"] = row.get("driver_id") or entry.get("driver_id")
        if row.get("report_date"):
            entry["last_synced_at"] = row.get("report_date")

        # derive payer badge
        pay_total = (_coerce_float(entry.get("payments")) or 0.0) + (_coerce_float(entry.get("bolt_wallet_payouts")) or 0.0)
        badge_label, badge_state = _payer_badge(entry.get("xero_balance"), pay_total, entry.get("yday_wallet_balance"), entry.get("rental_balance"))
        entry["payer_badge_label"] = badge_label
        entry["payer_badge_state"] = badge_state

    def _coerce_metric_value(row: Dict[str, Any], columns: List[str]) -> Optional[float]:
        for key in columns:
            if key not in row:
                continue
            val = row.get(key)
            if val is None:
                continue
            text = str(val).strip()
            if not text:
                continue
            if text.endswith("%"):
                text = text[:-1]
            coerced = _coerce_float(text)
            if coerced is not None:
                return coerced
        return None

    def _apply_delta(entry: Dict[str, Any], history: List[Dict[str, Any]], delta_key: str, columns: List[str]):
        if len(history) < 2:
            return
        current = _coerce_metric_value(history[0], columns)
        previous = _coerce_metric_value(history[1], columns)
        if current is None or previous is None:
            return
        entry[delta_key] = current - previous

    history_by_driver: Dict[int, List[Dict[str, Any]]] = {}
    if driver_ids:
        unique_ids = sorted(set([d for d in driver_ids if d is not None]))
        if unique_ids:
            placeholders = ", ".join(["%s"] * len(unique_ids))
            if "report_date" in available:
                history_sql = f"""
                    SELECT * FROM (
                        SELECT {', '.join(select_cols)}, ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY report_date DESC) AS rn
                        FROM {table}
                        WHERE driver_id IN ({placeholders})
                    ) t
                    WHERE rn <= 2
                """
            else:
                history_sql = f"""
                    SELECT * FROM (
                        SELECT {', '.join(select_cols)}, ROW_NUMBER() OVER (PARTITION BY driver_id ORDER BY driver_id ASC) AS rn
                        FROM {table}
                        WHERE driver_id IN ({placeholders})
                    ) t
                    WHERE rn <= 2
                """
            try:
                with conn.cursor() as cur:
                    cur.execute(history_sql, tuple(unique_ids))
                    history_rows = cur.fetchall() or []
                for row in history_rows:
                    driver_id_val = row.get("driver_id")
                    if driver_id_val is None:
                        continue
                    try:
                        key = int(driver_id_val)
                    except Exception:
                        continue
                    history_by_driver.setdefault(key, []).append(row)
            except Exception as exc:
                log.warning("Failed to fetch driver deltas batch: %s", exc)

    delta_map = [
        ("delta_online_hours", ["total_online_hours", "online_hours"]),
        ("delta_trip_count", ["total_finished_orders", "trip_count", "finished_trips", "total_trips_sent"]),
        ("delta_gross_earnings", ["total_gmv", "gross_earnings"]),
        ("delta_earnings_per_hour", ["eph"]),
        ("delta_acceptance_rate", ["acceptance_rate", "acceptance_pct"]),
        ("delta_xero_balance", ["xero_balance"]),
        ("delta_payments", ["payments", "7D_payments", "total_payments"]),
        ("delta_bolt_wallet_payouts", ["bolt_wallet_payouts"]),
        ("delta_yday_wallet_balance", ["yday_wallet_balance"]),
        ("delta_total_trip_distance", ["total_trip_distance"]),
        ("delta_total_tracking_distance", ["total_tracking_distance"]),
    ]
    for wa_id, entry in drivers.items():
        driver_id = entry.get("driver_id")
        if driver_id is None:
            continue
        history = history_by_driver.get(driver_id) or []
        if len(history) < 2:
            continue
        for delta_key, columns in delta_map:
            _apply_delta(entry, history, delta_key, columns)

    return list(drivers.values())

def _collect_driver_kpi_rows(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    wa_col = _pick_col_exists(conn, table, ["wa_id", "whatsapp_number", "whatsapp", "phone", "phone_number"])
    if not wa_col:
        return []
    name_col = _pick_col_exists(conn, table, ["name", "full_name", "driver_name"])
    sf_status_col = _pick_col_exists(conn, table, ["sf_status", "status", "driver_status", "account_status"])
    last_synced_col = _pick_col_exists(conn, table, ["report_date", "snapshot_date", "created_at", "updated_at"])
    contact_cols = [c for c in BOLT_CONTACT_COLUMNS if c in available]

    select_cols: List[str] = [wa_col]
    if name_col:
        select_cols.append(name_col)
    if sf_status_col:
        select_cols.append(sf_status_col)
    if last_synced_col:
        select_cols.append(last_synced_col)
    for col in contact_cols:
        if col not in select_cols:
            select_cols.append(col)

    sql = f"SELECT {', '.join(select_cols)} FROM {table}"
    order_clause = ""
    if last_synced_col:
        order_clause = f" ORDER BY {last_synced_col} DESC"
    if max_rows and max_rows > 0:
        sql = f"{sql}{order_clause} LIMIT %s"
        params = [max_rows]
    else:
        sql = f"{sql}{order_clause}"
        params: List[Any] = []

    drivers: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
    for row in rows:
        wa_raw = row.get(wa_col)
        wa_id = _normalize_wa_id(wa_raw)
        if not wa_id:
            continue
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)
        display_name = row.get(name_col) if name_col else None
        status = row.get(sf_status_col) if sf_status_col else None
        last_synced = row.get(last_synced_col) if last_synced_col else None
        driver = {
            "wa_id": wa_id,
            "display_name": display_name or "",
            "contact_ids": contact_ids,
            "sf_status": status,
            "status": status,
            "last_synced_at": last_synced,
        }
        drivers.append(driver)
    return drivers


def _collect_kpi_summary_drivers(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    phone_col = next((c for c in ["phone", "wa_id", "whatsapp_number", "whatsapp", "contact_number"] if c in available), None)
    if not phone_col:
        return []
    name_cols = [c for c in ["name", "driver_name", "full_name"] if c in available]
    name_col = name_cols[0] if name_cols else None
    status_col = next((c for c in ["status", "driver_status", "sf_status"] if c in available), None)
    report_col = "report_date" if "report_date" in available else None
    contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]

    select_cols: List[str] = [phone_col]
    if name_col:
        select_cols.append(name_col)
    if status_col:
        select_cols.append(status_col)
    if report_col:
        select_cols.append(report_col)
    for col in contact_cols:
        select_cols.append(col)

    sql = f"SELECT {', '.join(select_cols)} FROM {table} WHERE {phone_col} IS NOT NULL AND {phone_col} <> ''"
    sql += " ORDER BY " + (report_col or phone_col) + " DESC"
    params: List[Any] = []
    if max_rows and max_rows > 0:
        sql += " LIMIT %s"
        params.append(max_rows)

    drivers: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("Failed to collect KPI drivers: %s", exc)
        return []

    seen_contact_ids: set[str] = set()
    for row in rows:
        raw_phone = row.get(phone_col)
        wa_id = _normalize_wa_id(raw_phone)
        if not wa_id or wa_id in drivers:
            continue
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)
        if not contact_ids:
            continue
        display_name = row.get(name_col) if name_col else None
        status = row.get(status_col) or "unknown"
        last_synced = row.get(report_col) if report_col else None
        drivers[wa_id] = {
            "wa_id": wa_id,
            "display_name": display_name or "",
            "status": status,
            "contact_ids": contact_ids,
            "last_synced_at": last_synced,
        }
    return list(drivers.values())


def _collect_kpi_drivers(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    phone_col = next(
        (c for c in ["phone", "whatsapp", "wa_id", "driver_phone", "driver_whatsapp"] if c in available),
        None,
    )
    if not phone_col:
        return []
    name_col = next((c for c in ["name", "driver_name", "full_name"] if c in available), None)
    contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]
    select_cols = {phone_col}
    if name_col:
        select_cols.add(name_col)
    select_cols.update(contact_cols)
    date_col = "report_date" if "report_date" in available else None
    if date_col:
        select_cols.add(date_col)

    order_column = date_col or phone_col
    sql = f"SELECT {', '.join(sorted(select_cols))} FROM {table} WHERE {phone_col} IS NOT NULL ORDER BY {order_column} DESC"
    params: List[Any] = []
    if max_rows and max_rows > 0:
        sql += " LIMIT %s"
        params.append(max_rows)

    drivers: List[Dict[str, Any]] = []
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
    for row in rows:
        wa_raw = row.get(phone_col)
        wa_id = _normalize_wa_id(wa_raw)
        if not wa_id:
            continue
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            cleaned = str(val).strip()
            if cleaned and cleaned not in contact_ids:
                contact_ids.append(cleaned)
        drivers.append(
            {
                "wa_id": wa_id,
                "display_name": (row.get(name_col) or "").strip() if name_col else "",
                "status": row.get("status"),
                "driver_status": row.get("status"),
                "last_synced_at": row.get(date_col) if date_col else None,
                "contact_ids": contact_ids,
            }
        )
    return drivers


def _merge_driver_collections(
    roster: List[Dict[str, Any]],
    kpi_entries: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    def add(entry: Dict[str, Any]) -> None:
        wa_id = _normalize_wa_id(entry.get("wa_id") or entry.get("phone"))
        if not wa_id:
            return
        base = merged.get(wa_id)
        if not base:
            base = {
                "wa_id": wa_id,
                "display_name": (entry.get("display_name") or entry.get("name") or entry.get("driver_name") or "Driver").strip(),
                "status": entry.get("status") or entry.get("driver_status") or "active",
                "driver_status": entry.get("driver_status") or entry.get("status"),
                "last_synced_at": entry.get("last_synced_at"),
                "contact_ids": [],
            }
            merged[wa_id] = base
        else:
            display = entry.get("display_name") or entry.get("name") or entry.get("driver_name")
            if display:
                base["display_name"] = display.strip()
            if not base.get("status") and entry.get("status"):
                base["status"] = entry.get("status")
            if entry.get("last_synced_at") and not base.get("last_synced_at"):
                base["last_synced_at"] = entry.get("last_synced_at")
        for cid in entry.get("contact_ids") or []:
            cleaned = str(cid).strip()
            if cleaned and cleaned not in base["contact_ids"]:
                base["contact_ids"].append(cleaned)

    for item in roster:
        add(item)
    for item in kpi_entries:
        add(item)
    return list(merged.values())


def _collect_kpi_drivers(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    wa_col = next((c for c in SIMPLYFLEET_WHATSAPP_COLUMNS if c in available), None)
    name_col = next((c for c in SIMPLYFLEET_DRIVER_NAME_COLUMNS if c in available), None)
    status_col = next((c for c in SIMPLYFLEET_STATUS_COLUMNS if c in available), None)
    driver_id_col = _pick_col_exists(conn, table, ["driver_id", "id"])
    report_col = "report_date" if "report_date" in available else None
    contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]

    select_cols: List[str] = []
    for col in (driver_id_col, wa_col, name_col, status_col, report_col):
        if col and col not in select_cols:
            select_cols.append(col)
    for col in contact_cols:
        if col not in select_cols:
            select_cols.append(col)
    if not select_cols:
        select_clause = "*"
    else:
        select_clause = ", ".join(select_cols)

    sql = f"SELECT {select_clause} FROM {table} ORDER BY "
    if report_col:
        sql += f"{report_col} DESC"
    else:
        sql += "created_at DESC"
    params: List[Any] = []
    if max_rows and max_rows > 0:
        sql += " LIMIT %s"
        params.append(max_rows)

    drivers: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("Failed to collect KPI drivers: %s", exc)
        return []

    for row in rows:
        wa_raw = row.get(wa_col) if wa_col else None
        wa_id = _normalize_wa_id(wa_raw) or ""
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)
        display_name = (row.get(name_col) or "").strip() if name_col else ""
        if not display_name:
            display_name = "Driver"
        driver_id = row.get(driver_id_col) if driver_id_col else None
        status = row.get(status_col) if status_col else None
        last_synced = None
        if report_col:
            last_synced = row.get(report_col)
        if not last_synced:
            last_synced = row.get("created_at")
        driver = {
            "wa_id": wa_id,
            "contact_ids": contact_ids,
            "display_name": display_name,
            "status": status or "kpi",
            "driver_status": status,
            "last_synced_at": last_synced,
            "driver_id": driver_id,
        }
        drivers.append(driver)
    return drivers


def _merge_driver_lists(roster: List[Dict[str, Any]], kpi_only: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_wa: set[str] = set()
    seen_contacts: set[str] = set()
    seen_driver_ids: set[str] = set()
    merged: List[Dict[str, Any]] = []

    def _mark(driver: Dict[str, Any]) -> None:
        wa = driver.get("wa_id")
        if wa:
            seen_wa.add(wa)
        driver_id = driver.get("driver_id")
        if driver_id is not None:
            seen_driver_ids.add(str(driver_id))
        for cid in driver.get("contact_ids") or []:
            if cid:
                seen_contacts.add(cid)

    for driver in roster:
        merged.append(driver)
        _mark(driver)

    for driver in kpi_only:
        wa = driver.get("wa_id")
        if wa and wa in seen_wa:
            continue
        driver_id = driver.get("driver_id")
        if driver_id is not None and str(driver_id) in seen_driver_ids:
            continue
        contacts = [str(cid).strip() for cid in (driver.get("contact_ids") or []) if cid]
        if any(cid in seen_contacts for cid in contacts if cid):
            continue
        merged.append(driver)
        _mark(driver)

    return merged


def _collect_kpi_drivers(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    wa_col = next((c for c in BOLT_CONTACT_COLUMNS if c in available), None)
    if not wa_col:
        wa_col = next((c for c in SIMPLYFLEET_WHATSAPP_COLUMNS if c in available), None)
    name_col = next((c for c in ["name", "display_name", "driver_name", "full_name"] if c in available), None)
    contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]
    select_cols = []
    def add_col(col: Optional[str]) -> None:
        if col and col not in select_cols:
            select_cols.append(col)

    add_col(wa_col)
    add_col(name_col)
    add_col("driver_id")
    for col in contact_cols:
        add_col(col)

    if not select_cols:
        return []

    params: List[Any] = []
    sql = f"SELECT {', '.join(select_cols)} FROM {table}"
    if max_rows and max_rows > 0:
        sql = f"{sql} LIMIT %s"
        params.append(max_rows)

    drivers: Dict[str, Dict[str, Any]] = {}
    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall() or []
    for row in rows:
        wa_raw = row.get(wa_col) if wa_col else None
        wa_id = _normalize_wa_id(wa_raw)
        if not wa_id:
            continue
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)
        display_name = row.get(name_col) if name_col else None
        drivers[wa_id] = {
            "wa_id": wa_id,
            "contact_ids": contact_ids,
            "status": "kpi",
            "display_name": display_name or "",
            "driver_id": row.get("driver_id"),
        }
    return list(drivers.values())


def _merge_driver_lists(primary: List[Dict[str, Any]], secondary: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    def add_driver(driver: Dict[str, Any]) -> None:
        wa_id = driver.get("wa_id")
        if not wa_id:
            return
        normalized_contacts = [str(cid).strip() for cid in (driver.get("contact_ids") or []) if cid and str(cid).strip()]
        driver["contact_ids"] = normalized_contacts
        if wa_id in merged:
            existing = merged[wa_id]
            combined = list(dict.fromkeys(existing.get("contact_ids", []) + normalized_contacts))
            existing["contact_ids"] = combined
            if not existing.get("display_name"):
                existing["display_name"] = driver.get("display_name")
            if not existing.get("status"):
                existing["status"] = driver.get("status")
        else:
            merged[wa_id] = driver

    for driver in primary:
        add_driver(driver)
    for driver in secondary:
        add_driver(driver)
    return list(merged.values())


def _collect_kpi_drivers(conn) -> List[Dict[str, Any]]:
    table = KPI_SUMMARY_TABLE
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    wa_col = next((c for c in KPI_WHATSAPP_COLUMNS if c in available), None)
    name_col = next((c for c in KPI_NAME_COLUMNS if c in available), None)
    status_col = next((c for c in KPI_STATUS_COLUMNS if c in available), None)
    sync_col = next((c for c in KPI_SYNC_COLUMNS if c in available), None)
    contact_cols = [c for c in KPI_CONTACT_COLUMNS if c in available]
    select_cols: List[str] = []
    if wa_col:
        select_cols.append(f"{wa_col} AS wa_id")
    if name_col:
        select_cols.append(f"{name_col} AS display_name")
    if status_col:
        select_cols.append(f"{status_col} AS status")
    if sync_col:
        select_cols.append(f"{sync_col} AS last_synced_at")
    for col in contact_cols:
        select_cols.append(f"{col}")
    if not select_cols:
        return []
    sql = f"SELECT DISTINCT {', '.join(select_cols)} FROM {table}"
    drivers: List[Dict[str, Any]] = []
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("Failed to collect KPI drivers: %s", exc)
        return []

    for row in rows:
        wa_raw = row.get("wa_id")
        wa_id = _normalize_wa_id(wa_raw)
        if not wa_id:
            continue
        contact_ids: List[str] = []
        seen: set[str] = set()
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in seen:
                seen.add(sval)
                contact_ids.append(sval)
        driver: Dict[str, Any] = {
            "wa_id": wa_id,
            "display_name": row.get("display_name") or "Driver",
            "status": (row.get("status") or "unknown"),
            "last_synced_at": row.get("last_synced_at"),
            "contact_ids": contact_ids,
        }
        drivers.append(driver)
    return drivers


def _merge_driver_lists(roster: List[Dict[str, Any]], kpi: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    def _add_entry(entry: Dict[str, Any]) -> None:
        wa_id = entry.get("wa_id")
        if not wa_id:
            return
        existing = merged.get(wa_id)
        if not existing:
            merged[wa_id] = dict(entry)
            return
        if not existing.get("display_name") and entry.get("display_name"):
            existing["display_name"] = entry["display_name"]
        if not existing.get("status") and entry.get("status"):
            existing["status"] = entry["status"]
        if not existing.get("last_synced_at") and entry.get("last_synced_at"):
            existing["last_synced_at"] = entry["last_synced_at"]
        base_contacts = existing.get("contact_ids") or []
        additions = [cid for cid in (entry.get("contact_ids") or []) if cid and cid not in base_contacts]
        if additions:
            existing["contact_ids"] = base_contacts + additions

    for driver in roster:
        _add_entry(driver)
    for driver in kpi:
        _add_entry(driver)
    return list(merged.values())


def _collect_kpi_drivers(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    try:
        if not _table_exists(conn, table):
            return []
    except Exception:
        return []

    available = _get_table_columns(conn, table)
    phone_col = _pick_col_exists(conn, table, ["phone", "wa_id", "whatsapp", "whatsapp_number", "wa_number", "phone_number", "contact_number"])
    if not phone_col:
        return []

    name_col = _pick_col_exists(conn, table, ["name", "driver_name", "full_name", "display_name"])
    sf_status_col = _pick_col_exists(conn, table, ["sf_status", "status"])
    portal_status_col = _pick_col_exists(conn, table, ["portal_status"])
    report_col = _pick_col_exists(conn, table, ["report_date", "snapshot_date", "created_at", "updated_at"])
    contact_cols = [col for col in SIMPLYFLEET_CONTACT_ID_COLUMNS if col in available]

    select_cols: List[str] = [phone_col]
    for col in (name_col, sf_status_col, portal_status_col, report_col):
        if col and col not in select_cols:
            select_cols.append(col)
    for col in contact_cols:
        if col not in select_cols:
            select_cols.append(col)

    if not select_cols:
        return []

    order_clause = f" ORDER BY {report_col} DESC" if report_col else ""
    limit_clause = " LIMIT %s" if max_rows else ""
    sql = f"SELECT {', '.join(select_cols)} FROM {table}{order_clause}{limit_clause}"
    params: List[Any] = []
    if max_rows:
        params.append(max_rows)

    drivers: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
    except Exception as exc:
        log.warning("Failed to collect KPI drivers: %s", exc)
        return []

    for row in rows:
        raw_phone = row.get(phone_col)
        wa_id = _normalize_wa_id(raw_phone)
        if not wa_id or wa_id in drivers:
            continue

        contact_ids: List[str] = []
        for col in contact_cols:
            value = row.get(col)
            if not value:
                continue
            sval = str(value).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)

        status_value = row.get(sf_status_col) if sf_status_col else None
        if not status_value and portal_status_col:
            status_value = row.get(portal_status_col)

        drivers[wa_id] = {
            "wa_id": wa_id,
            "display_name": (row.get(name_col) or "").strip(),
            "status": (status_value or "unknown") or "unknown",
            "contact_ids": contact_ids,
            "last_synced_at": row.get(report_col),
        }
    return list(drivers.values())


def _collect_kpi_drivers(conn, *, max_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_kpi_summary"
    if not _table_exists(conn, table):
        return []
    available = _get_table_columns(conn, table)
    wa_col = _pick_col_exists(conn, table, ["wa_id", "phone", "whatsapp", "whatsapp_number", "wa_number", "phone_number"])
    name_col = _pick_col_exists(conn, table, ["name", "full_name", "driver_name"])
    status_col = _pick_col_exists(conn, table, ["portal_status", "sf_status", "status"])
    report_col = _pick_col_exists(conn, table, ["report_date", "created_at", "imported_at", "logged_at"])
    contact_cols = [c for c in ["xero_contact_id", "contact_id", "account_id", "xero_contact_id_bjj", "xero_contact_id_hakki", "xero_contact_id_a49"] if c in available]
    driver_id_col = _pick_col_exists(conn, table, ["driver_id", "id"])
    if not wa_col:
        return []

    select_cols: List[str] = []
    for col in [wa_col, name_col, status_col, report_col]:
        if col and col not in select_cols:
            select_cols.append(col)
    if driver_id_col:
        select_cols.append(driver_id_col)
    for col in contact_cols:
        if col not in select_cols:
            select_cols.append(col)

    sql = f"SELECT {', '.join(select_cols)} FROM {table}"
    params: List[Any] = []
    if report_col:
        sql += f" ORDER BY {report_col} DESC"
    if max_rows and max_rows > 0:
        sql += " LIMIT %s"
        params.append(max_rows)

    drivers: Dict[str, Dict[str, Any]] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall() or []:
                wa_raw = row.get(wa_col)
                wa_id = _normalize_wa_id(wa_raw)
                if not wa_id or wa_id in drivers:
                    continue
                contact_ids: List[str] = []
                for col in contact_cols:
                    val = row.get(col)
                    if not val:
                        continue
                    sval = str(val).strip()
                    if sval and sval not in contact_ids:
                        contact_ids.append(sval)
                display_name = row.get(name_col) if name_col else None
                status_val = row.get(status_col) if status_col else None
                last_synced = row.get(report_col) if report_col else None
                driver_id = row.get(driver_id_col) if driver_id_col else None
                drivers[wa_id] = {
                    "wa_id": wa_id,
                    "contact_ids": contact_ids,
                    "status": str(status_val).strip() if status_val else "unknown",
                    "driver_status": status_val,
                    "display_name": display_name or "",
                    "last_synced_at": last_synced,
                    "driver_id": driver_id,
                }
    except Exception as exc:
        log.warning("Failed to collect KPI drivers: %s", exc)
    return list(drivers.values())


def _build_driver_filter_query(
    *,
    name: Optional[str] = None,
    model: Optional[str] = None,
    reg: Optional[str] = None,
    phone: Optional[str] = None,
    collections_agent: Optional[str] = None,
    status: Optional[str] = None,
    driver_type: Optional[str] = None,
    payer_type: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
) -> str:
    params: Dict[str, str] = {}

    def add_str(key: str, value: Optional[str]) -> None:
        if value is None:
            return
        cleaned = str(value).strip()
        if not cleaned:
            return
        params[key] = cleaned

    def add_int(key: str, value: Optional[int]) -> None:
        if value is None:
            return
        try:
            int_val = int(value)
        except (TypeError, ValueError):
            return
        if int_val <= 0 and key == "offset":
            return
        if int_val <= 0 and key == "limit":
            return
        params[key] = str(int_val)

    add_str("name", name)
    add_str("model", model)
    add_str("reg", reg)
    add_str("phone", phone)
    add_str("collections_agent", collections_agent)
    add_str("status", status)
    add_str("driver_type", driver_type)
    add_str("payer_type", payer_type)
    add_int("limit", limit)
    add_int("offset", offset)

    if not params:
        return ""
    return urlencode(params)


def fetch_active_driver_profiles(
    name: Optional[str] = None,
    model: Optional[str] = None,
    reg: Optional[str] = None,
    phone: Optional[str] = None,
    collections_agent: Optional[str] = None,
    status: Optional[str] = None,
    driver_type: Optional[str] = None,
    payer_type: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    paginate: bool = True,
) -> Tuple[List[Dict[str, Any]], Optional[str], int, List[str], List[str], List[str], List[str]]:
    limit = max(10, min(limit, 1000)) if paginate and limit else 200
    offset = max(0, offset)
    # Pull full roster (no DB LIMIT) while keeping UI pagination intact.
    row_limit = 0

    (
        drivers,
        error,
        collections_options,
        driver_type_options,
        payer_type_options,
    ) = _load_cached_driver_roster(row_limit)
    if error:
        return [], error, 0, [], [], [], []

    def _status_bucket(value: Optional[str]) -> str:
        txt = (value or "").strip().lower()
        if not txt or txt == "unknown":
            return "unknown"
        if "blacklist" in txt:
            return "blacklisted"
        if "inactive" in txt:
            return "inactive"
        if "pause" in txt:
            return "pause"
        if txt == "active" or txt.startswith("active"):
            return "active"
        return txt

    status_bucket_label = {
        "active": "Active",
        "pause": "Pause",
        "inactive": "Inactive",
        "blacklisted": "Blacklisted",
        "unknown": "Unknown",
    }

    # Dropdown options (stable, canonical buckets)
    bucket_counts: Dict[str, int] = {}
    for d in drivers:
        bucket = _status_bucket(d.get("status"))
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1

    preferred_buckets = ["active", "pause", "inactive", "blacklisted", "unknown"]
    status_options: List[str] = []
    for bucket in preferred_buckets:
        if bucket_counts.get(bucket):
            status_options.append(status_bucket_label.get(bucket, bucket.title()))

    # quick filters
    name_q = (name or "").strip()
    # allow comma-separated or repeated values for multi filters
    def _split_multi(val: Optional[str]) -> List[str]:
        if not val:
            return []
        parts = []
        for chunk in str(val).split(","):
            c = chunk.strip()
            if c:
                parts.append(c)
        return parts

    model_list = _split_multi(model)
    collections_list = _split_multi(collections_agent)
    status_list = _split_multi(status)

    model_q = (model or "").strip()
    reg_q = (reg or "").strip()
    phone_q = (phone or "").strip()
    collections_q = (collections_agent or "").strip()
    status_q = (status or "").strip()
    driver_type_q = (driver_type or "").strip()
    payer_type_q = (payer_type or "").strip()

    if any([name_q, model_q, reg_q, phone_q, collections_q, status_q, driver_type_q, payer_type_q]):
        q_name = name_q.lower()
        q_model = model_q.lower()
        q_reg = reg_q.lower()
        q_phone = phone_q.lower()
        q_collections = collections_q.lower()
        q_status = status_q.lower()
        q_driver_type = driver_type_q.lower()
        q_payer_type = payer_type_q.lower()
        model_list_lc = [m.lower() for m in model_list]
        collections_list_lc = [c.lower() for c in collections_list]
        desired_buckets = {_status_bucket(s) for s in status_list if s and str(s).strip()}
        if not desired_buckets and q_status:
            desired_buckets = {_status_bucket(q_status)}

        def matches(d: Dict[str, Any]) -> bool:
            display = (d.get("display_name") or "").lower()
            wa_id = (d.get("wa_id") or "").lower()
            status_value = (d.get("status") or "").strip()
            status_bucket = _status_bucket(status_value)
            contacts = [c.lower() for c in (d.get("contact_ids") or [])]
            model_val = (d.get("model") or "").lower()
            reg_val = (d.get("car_reg_number") or "").lower()
            phone_val = (d.get("driver_phone") or "").lower()
            collections_val = (d.get("collections_agent") or "").lower()
            driver_type_val = (d.get("efficiency_badge_label") or "").lower()
            payer_type_val = (d.get("payer_badge_label") or "").lower()

            def contains(needle: str, haystack: str) -> bool:
                return bool(needle) and needle in haystack

            if q_name and q_name not in display:
                return False
            if model_list_lc:
                if model_val not in model_list_lc:
                    return False
            elif q_model and q_model not in model_val:
                return False
            if q_reg and q_reg not in reg_val:
                return False
            if q_phone:
                if q_phone not in phone_val and not any(q_phone in cid for cid in contacts):
                    return False
            if collections_list_lc:
                if collections_val not in collections_list_lc:
                    return False
            elif q_collections and q_collections not in collections_val:
                return False
            if desired_buckets:
                if status_bucket not in desired_buckets:
                    return False
            if q_driver_type and q_driver_type not in driver_type_val:
                return False
            if q_payer_type and q_payer_type not in payer_type_val:
                return False
            return True

        drivers = [d for d in drivers if matches(d)]

    total = len(drivers)
    if paginate:
        if offset >= total:
            drivers = []
        else:
            drivers = drivers[offset : offset + limit]

    return drivers, None, total, collections_options, status_options, driver_type_options, payer_type_options


def _sanitize_phone_expr(column: str) -> str:
    return f"REPLACE(REPLACE(REPLACE({column}, '+',''), ' ', ''), '-', '')"

def _format_profile_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return str(value)

def fetch_driver_profile(wa_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not wa_id:
        return None, "Driver number missing."
    if not mysql_available():
        return None, "database connection not configured"
    variants = _wa_number_variants(wa_id)
    if not variants:
        normalized = _normalize_wa_id(wa_id)
        if normalized:
            variants = [normalized]
    if not variants:
        return None, "could not normalise the WhatsApp number"
    try:
        conn = get_mysql()
    except Exception as exc:
        return None, f"database unavailable: {exc}"
    try:
        table = _detect_simplyfleet_table(conn)
        if not table:
            return None, "driver roster table not found"
        available = _get_table_columns(conn, table)
        wa_col = next((c for c in SIMPLYFLEET_WHATSAPP_COLUMNS if c in available), None)
        if not wa_col:
            return None, "driver roster missing WhatsApp column"
        backup_col = next((c for c in SIMPLYFLEET_BACKUP_DATE_COLUMNS if c in available), None)
        status_col = next((c for c in SIMPLYFLEET_STATUS_COLUMNS if c in available), None)
        name_col = next((c for c in SIMPLYFLEET_DRIVER_NAME_COLUMNS if c in available), None)
        contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]
        sanitized_expr = _sanitize_phone_expr(wa_col)
        placeholders = ", ".join(["%s"] * len(variants))
        order_clause = f" ORDER BY {backup_col} DESC" if backup_col else ""
        sql = f"SELECT * FROM {table} WHERE {sanitized_expr} IN ({placeholders}){order_clause} LIMIT 1"
        with conn.cursor() as cur:
            cur.execute(sql, variants)
            row = cur.fetchone()
        if not row:
            return None, "Driver not found in roster."
        profile = dict(row)
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)
        profile["contact_ids"] = contact_ids
        profile["display_name"] = profile.get("display_name") or profile.get(name_col) or profile.get("full_name") or "Driver"
        profile["status"] = profile.get(status_col) if status_col else profile.get("status")
        profile["last_synced_at"] = profile.get(backup_col) if backup_col else profile.get("last_synced_at")
        profile["wa_id"] = _normalize_wa_id(wa_id) or wa_id
        profile["_source_table"] = table
        return profile, None
    except Exception as exc:
        log.warning("fetch_driver_profile failed: %s", exc)
        return None, "error retrieving driver profile."
    finally:
        try:
            conn.close()
        except Exception:
            pass

def fetch_driver_profile_by_personal_code(code: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    clean = re.sub(r"\D", "", code or "")
    if not clean:
        return None, "Personal code missing."
    if not mysql_available():
        return None, "database connection not configured"
    try:
        conn = get_mysql()
    except Exception as exc:
        return None, f"database unavailable: {exc}"
    try:
        table = _detect_simplyfleet_table(conn)
        if not table:
            return None, "driver roster table not found"
        available = _get_table_columns(conn, table)
        personal_col = _pick_col_exists(conn, table, SIMPLYFLEET_PERSONAL_CODE_COLUMNS)
        if not personal_col:
            return None, "driver roster missing personal code column"
        wa_col = next((c for c in SIMPLYFLEET_WHATSAPP_COLUMNS if c in available), None)
        backup_col = next((c for c in SIMPLYFLEET_BACKUP_DATE_COLUMNS if c in available), None)
        status_col = next((c for c in SIMPLYFLEET_STATUS_COLUMNS if c in available), None)
        name_col = next((c for c in SIMPLYFLEET_DRIVER_NAME_COLUMNS if c in available), None)
        contact_cols = [c for c in SIMPLYFLEET_CONTACT_ID_COLUMNS if c in available]
        order_clause = f" ORDER BY {backup_col} DESC" if backup_col else ""
        sql = f"SELECT * FROM {table} WHERE {personal_col} = %s{order_clause} LIMIT 1"
        with conn.cursor() as cur:
            cur.execute(sql, (clean,))
            row = cur.fetchone()
        if not row:
            return None, "Driver not found in roster."
        profile = dict(row)
        contact_ids: List[str] = []
        for col in contact_cols:
            val = row.get(col)
            if not val:
                continue
            sval = str(val).strip()
            if sval and sval not in contact_ids:
                contact_ids.append(sval)
        profile["contact_ids"] = contact_ids
        profile["display_name"] = profile.get("display_name") or profile.get(name_col) or profile.get("full_name") or "Driver"
        profile["status"] = profile.get(status_col) if status_col else profile.get("status")
        profile["last_synced_at"] = profile.get(backup_col) if backup_col else profile.get("last_synced_at")
        if wa_col:
            wa_raw = row.get(wa_col)
            wa_norm = _normalize_wa_id(wa_raw) if wa_raw else None
            if wa_norm:
                profile["wa_id"] = wa_norm
            elif wa_raw:
                profile["wa_id"] = str(wa_raw).strip()
        profile["personal_code"] = profile.get(personal_col) or clean
        profile["_source_table"] = table
        return profile, None
    except Exception as exc:
        log.warning("fetch_driver_profile_by_personal_code failed: %s", exc)
        return None, "error retrieving driver profile."
    finally:
        try:
            conn.close()
        except Exception:
            pass

def summarize_profile_fields(profile: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    if not profile:
        return []
    fields: List[Dict[str, str]] = []
    used: set[str] = set()
    for key, label in DRIVER_PROFILE_FIELD_LABELS.items():
        val = profile.get(key)
        if val in (None, "", []):
            continue
        fields.append({"label": label, "key": key, "value": _format_profile_value(val)})
        used.add(key)
    for key, val in sorted(profile.items()):
        if key in used or key in PROFILE_SILENT_KEYS:
            continue
        if val in (None, "", []):
            continue
        fields.append({"label": key.replace("_", " ").title(), "key": key, "value": _format_profile_value(val)})
    return fields


def _filter_roster_fields(fields: List[Dict[str, str]]) -> List[Dict[str, str]]:
    if not fields:
        return []
    filtered: List[Dict[str, str]] = []
    for field in fields:
        label = str(field.get("label") or "")
        normalized = re.sub(r"[^a-z0-9]+", "", label.lower())
        if normalized in ROSTER_DETAIL_LABEL_BLOCK_SET:
            continue
        if normalized in ROSTER_DETAIL_LABEL_SET:
            filtered.append(field)
    return filtered

def _query_driver_order_stats(
    contact_ids: List[str],
    *,
    driver_id: Optional[int] = None,
    wa_id: Optional[str] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not contact_ids and driver_id is None and not wa_id:
        return None, "No driver identifiers available for this driver."
    if not mysql_available():
        return None, "database connection not configured"
    try:
        conn = get_mysql()
    except Exception as exc:
        return None, f"database unavailable: {exc}"
    try:
        table = _detect_bolt_orders_table(conn)
        if not table:
            return None, "orders table not found"
        contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS)
        date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
        if not contact_col or not date_col:
            return None, "orders table missing contact/date columns"
        status_col = _pick_col_exists(conn, table, BOLT_STATUS_COLUMNS)
        earnings_col = _pick_col_exists(conn, table, BOLT_EARNINGS_COLUMNS)
        trip_col = _pick_col_exists(conn, table, BOLT_TRIP_COLUMNS)

        identifier_values: List[Any] = []
        normalized_wa = _normalize_wa_id(wa_id) if wa_id else None
        if _bolt_contact_prefers_driver_id(contact_col):
            if driver_id is None:
                return None, "orders table expects driver IDs but driver_id is missing."
            identifier_values = [driver_id]
        elif contact_col == "wa_id":
            if not normalized_wa:
                return None, "orders table expects WhatsApp IDs but wa_id is missing."
            identifier_values = [normalized_wa]
        else:
            ids = _normalize_contact_ids(contact_ids)
            if not ids:
                return None, "No contact IDs available for this driver."
            identifier_values = list(ids)

        contact_clause = f"{contact_col} IN ({', '.join(['%s'] * len(identifier_values))})"

        def aggregate(start_dt: datetime, end_dt: datetime) -> Dict[str, Any]:
            clauses = [contact_clause, f"{date_col} >= %s", f"{date_col} < %s"]
            params: List[Any] = list(identifier_values) + [
                start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            ]
            if status_col:
                status_values = [s.lower() for s in FINISHED_STATUS_VALUES]
                placeholders = ", ".join(["%s"] * len(status_values))
                clauses.append(f"LOWER({status_col}) IN ({placeholders})")
                params.extend(status_values)
            select_parts = ["COUNT(*) AS order_rows"]
            if trip_col:
                select_parts.append(f"SUM(COALESCE({trip_col},0)) AS trip_sum")
            if earnings_col:
                select_parts.append(f"SUM(COALESCE({earnings_col},0)) AS earnings_sum")
            sql = f"SELECT {', '.join(select_parts)} FROM {table} WHERE {' AND '.join(clauses)}"
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone() or {}
            orders = row.get("trip_sum")
            if orders is None:
                orders = row.get("order_rows")
            orders_int = int(float(orders or 0))
            earnings = float(row.get("earnings_sum") or 0.0)
            return {"orders": orders_int, "earnings": earnings}

        today_start, tomorrow = jhb_today_range()
        last7_start = today_start - timedelta(days=7)
        stats = {
            "today": aggregate(today_start, tomorrow),
            "last_7_days": aggregate(last7_start, tomorrow),
            "table": table,
        }
        return stats, None
    except Exception as exc:
        log.warning("get_driver_order_stats failed: %s", exc)
        return None, "error while querying driver orders."
    finally:
        try:
            conn.close()
        except Exception:
            pass

def get_driver_order_stats(
    contact_ids: List[str],
    *,
    driver_id: Optional[int] = None,
    wa_id: Optional[str] = None,
    use_cache: bool = True,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    ids = _normalize_contact_ids(contact_ids)
    key_parts: List[Tuple[str, Any]] = []
    if driver_id is not None:
        key_parts.append(("driver_id", str(driver_id)))
    if wa_id:
        normalized_wa = _normalize_wa_id(wa_id)
        if normalized_wa:
            key_parts.append(("wa_id", normalized_wa))
    if ids:
        key_parts.append(("contact_ids", tuple(ids)))
    if not key_parts:
        return None, "No driver identifiers available for this driver."
    if not use_cache:
        return _query_driver_order_stats(ids, driver_id=driver_id, wa_id=wa_id)
    key = tuple(key_parts)
    now = time.time()
    with _driver_order_stats_cache_lock:
        entry = _driver_order_stats_cache.get(key)
        if entry and entry[0] > now:
            return entry[1], entry[2]
    stats, reason = _query_driver_order_stats(ids, driver_id=driver_id, wa_id=wa_id)
    with _driver_order_stats_cache_lock:
        _driver_order_stats_cache[key] = (time.time() + DRIVER_ORDER_STATS_CACHE_TTL_SECONDS, stats, reason)
    return stats, reason


def _efficiency_badge_for_driver(trips: int, gmv: float, online_hours: float = 0.0) -> Tuple[str, str]:
    trips = max(0, int(trips or 0))
    gmv_val = float(gmv or 0.0)
    hours = float(online_hours or 0.0)

    if trips == 0:
        return "No trips yet", "alert"
    # Efficient: meets original threshold (hours >=50 and gross >=5500) OR very high gross >=6000 even if hours <50.
    if (hours >= 50 and gmv_val >= 5500.0) or gmv_val >= 6000.0:
        return "Efficient driver", "efficient"
    if hours >= 40.0 and gmv_val >= 4500.0:
        return "Medium pace", "tracking"
    if hours < 40.0 or gmv_val < 4500.0:
        return "Needs attention", "alert"
    return "Needs attention", "alert"


def _build_badge_summary(drivers: List[Dict[str, Any]], label_key: str) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}
    for d in drivers:
        label = (d.get(label_key) or "").strip() or "N/A"
        bucket = buckets.setdefault(label, {"label": label, "values": []})
        bucket["values"].append(d)

    summary: List[Dict[str, Any]] = []
    total_count = len(drivers)
    for label, data in buckets.items():
        vals = data["values"]
        def collect(key):
            return [_coerce_float(v.get(key)) for v in vals if v.get(key) is not None]
        summary.append(
            {
                "label": label,
                "count": len(vals),
                "count_pct": (len(vals) / total_count * 100.0) if total_count else None,
                "delta_count": 0.0,
                "total_xero_balance": sum([_coerce_float(v.get("xero_balance")) or 0.0 for v in vals]),
                "delta_total_xero_balance": sum([_coerce_float(v.get("delta_xero_balance")) or 0.0 for v in vals]) if any(v.get("delta_xero_balance") is not None for v in vals) else None,
                "avg_online_hours": _avg(collect("online_hours")),
                "avg_gross_earnings": _avg(collect("gross_earnings")),
                "avg_eph": _avg(collect("earnings_per_hour")),
                "avg_acceptance_rate": _avg(collect("acceptance_rate")),
                "delta_online_hours": _avg(collect("delta_online_hours")),
                "delta_gross_earnings": _avg(collect("delta_gross_earnings")),
                "delta_earnings_per_hour": _avg(collect("delta_earnings_per_hour")),
                "delta_acceptance_rate": _avg(collect("delta_acceptance_rate")),
            }
        )
    def _label_order(x: Dict[str, Any]) -> Tuple[int, str]:
        label = (x.get("label") or "").lower()
        if label == "good standing":
            return (0, label)
        return (1, label)
    summary.sort(key=_label_order)
    # Totals row
    def collect_all(key):
        return [_coerce_float(v.get(key)) for v in drivers if v.get(key) is not None]
    total_row = {
        "label": "Total",
        "count": len(drivers),
        "count_pct": 100.0 if drivers else None,
        "delta_count": 0.0,
        "avg_online_hours": _avg(collect_all("online_hours")),
        "avg_gross_earnings": _avg(collect_all("gross_earnings")),
        "avg_eph": _avg(collect_all("earnings_per_hour")),
        "avg_acceptance_rate": _avg(collect_all("acceptance_rate")),
        "delta_online_hours": _avg(collect_all("delta_online_hours")),
        "delta_gross_earnings": _avg(collect_all("delta_gross_earnings")),
        "delta_earnings_per_hour": _avg(collect_all("delta_earnings_per_hour")),
        "delta_acceptance_rate": _avg(collect_all("delta_acceptance_rate")),
        "total_xero_balance": sum([_coerce_float(v.get("xero_balance")) or 0.0 for v in drivers]),
        "delta_total_xero_balance": sum([_coerce_float(v.get("delta_xero_balance")) or 0.0 for v in drivers]) if any(v.get("delta_xero_balance") is not None for v in drivers) else None,
        "is_total": True,
    }
    summary.append(total_row)
    return summary

def _count_completed_trips(
    conn,
    contact_ids: List[str],
    start_dt: datetime,
    end_dt: datetime,
) -> int:
    if not contact_ids:
        return 0
    table = _detect_bolt_orders_table(conn)
    if not table:
        return 0
    contact_col = _pick_col_exists(conn, table, BOLT_CONTACT_COLUMNS)
    date_col = _pick_col_exists(conn, table, BOLT_DATE_COLUMNS)
    if not contact_col or not date_col:
        return 0
    status_col = _pick_col_exists(conn, table, BOLT_STATUS_COLUMNS)

    start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    clauses = [f"{contact_col} IN ({', '.join(['%s'] * len(contact_ids))})",
               f"{date_col} >= %s",
               f"{date_col} < %s"]
    params: List[Any] = list(contact_ids) + [start_str, end_str]

    if status_col:
        clauses.append(f"LOWER({status_col}) IN ({', '.join(['%s'] * len(FINISHED_STATUS_VALUES))})")
        params.extend([s.lower() for s in FINISHED_STATUS_VALUES])

    sql = f"SELECT COUNT(*) AS cnt FROM {table} WHERE {' AND '.join(clauses)}"

    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            if row and row.get("cnt") is not None:
                return int(row["cnt"])
    except Exception as exc:
        log.warning("Trip count lookup failed: %s", exc)
    return 0


def _merge_driver_lists(roster: List[Dict[str, Any]], kpi: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}

    def _merge_kpi_metrics(target: Dict[str, Any], source: Dict[str, Any]) -> None:
        for key in DRIVER_KPI_MERGE_KEYS:
            if key not in source:
                continue
            value = source.get(key)
            if value in (None, ""):
                continue
            if target.get(key) in (None, ""):
                target[key] = value

    def add_or_merge(entry: Dict[str, Any]) -> None:
        wa_id = entry.get("wa_id")
        if not wa_id:
            return
        existing = merged.get(wa_id)
        if not existing:
            dedup_contacts = list(dict.fromkeys([str(c).strip() for c in (entry.get("contact_ids") or []) if str(c).strip()]))
            entry = dict(entry)
            entry["contact_ids"] = dedup_contacts
            _merge_kpi_metrics(entry, entry)
            merged[wa_id] = entry
            return

        combined_contacts = list(dict.fromkeys(
            [str(c).strip() for c in (existing.get("contact_ids") or []) + (entry.get("contact_ids") or []) if str(c).strip()]
        ))
        existing["contact_ids"] = combined_contacts

        if not (existing.get("display_name") or "").strip():
            existing["display_name"] = entry.get("display_name")

        if not existing.get("status") or existing.get("status") == "unknown":
            existing["status"] = entry.get("status")
        if not existing.get("driver_status"):
            existing["driver_status"] = entry.get("driver_status")

        if entry.get("last_synced_at") and not existing.get("last_synced_at"):
            existing["last_synced_at"] = entry.get("last_synced_at")

        _merge_kpi_metrics(existing, entry)

    for d in roster:
        add_or_merge(d)
    for d in kpi:
        add_or_merge(d)

    return list(merged.values())


def _apply_latest_simplyfleet_statuses(drivers: List[Dict[str, Any]]) -> None:
    wa_ids = [d.get("wa_id") for d in drivers if d.get("wa_id")]
    if not wa_ids:
        return
    status_map = _collect_simplyfleet_statuses(wa_ids)
    if not status_map:
        return
    for driver in drivers:
        wa_id = driver.get("wa_id")
        sf_status = status_map.get(wa_id)
        if sf_status:
            driver["status"] = sf_status


def _build_driver_roster(
    max_rows: int,
) -> Tuple[
    List[Dict[str, Any]],
    List[str],
    List[str],
    List[str],
    Optional[str],
]:
    if not mysql_available():
        return [], [], [], [], "MySQL not configured."
    try:
        conn = get_mysql()
    except Exception as exc:
        return [], [], [], [], f"Database unavailable: {exc}"

    try:
        roster_drivers = _collect_active_drivers(conn, max_rows=max_rows)
        kpi_drivers = _collect_kpi_drivers(conn, max_rows=max_rows)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    drivers = _merge_driver_lists(roster_drivers, kpi_drivers)
    _apply_latest_simplyfleet_statuses(drivers)
    drivers.sort(key=lambda d: ((d.get("display_name") or "").lower(), d.get("wa_id") or ""))
    collections_set: set[str] = set()
    driver_type_set: set[str] = set()
    payer_type_set: set[str] = set()
    for driver in drivers:
        trips = int(_coerce_float(driver.get("trip_count")) or 0)
        gmv = _coerce_float(driver.get("gross_earnings")) or 0.0
        hours = _coerce_float(driver.get("online_hours")) or 0.0
        label, state = _efficiency_badge_for_driver(trips, gmv, hours)
        driver["efficiency_badge_label"] = label
        driver["efficiency_badge_state"] = state
        if label:
            driver_type_set.add(label)

        collections_agent = driver.get("collections_agent")
        if collections_agent:
            agent = str(collections_agent).strip()
            if agent:
                collections_set.add(agent)

        payer_label = driver.get("payer_badge_label")
        if not payer_label:
            pay_total = (
                (_coerce_float(driver.get("payments")) or 0.0)
                + (_coerce_float(driver.get("bolt_wallet_payouts")) or 0.0)
            )
            badge_label, badge_state = _payer_badge(
                driver.get("xero_balance"),
                pay_total,
                driver.get("yday_wallet_balance"),
                driver.get("rental_balance"),
            )
            driver["payer_badge_label"] = badge_label
            driver["payer_badge_state"] = badge_state
            payer_label = badge_label
        if payer_label:
            payer_type_set.add(str(payer_label).strip())

    collections_options = sorted([val for val in collections_set if val])
    driver_type_options = sorted([val for val in driver_type_set if val])
    payer_type_options = sorted([val for val in payer_type_set if val])
    return drivers, collections_options, driver_type_options, payer_type_options, None


def _load_cached_driver_roster(
    max_rows: int,
) -> Tuple[List[Dict[str, Any]], Optional[str], List[str], List[str], List[str]]:
    now = time.time()
    with _driver_roster_cache_lock:
        cache = _driver_roster_cache
        if cache["drivers"] and cache["expiry"] > now:
            if max_rows == 0 and cache["max_rows"] == 0:
                cached = [dict(d) for d in cache["drivers"]]
                if DRIVER_ROSTER_REFRESH_STATUSES_ON_CACHE_HIT:
                    _apply_latest_simplyfleet_statuses(cached)
                return (
                    cached,
                    None,
                    list(cache["collections"]),
                    list(cache["driver_types"]),
                    list(cache["payer_types"]),
                )
            if max_rows > 0 and (cache["max_rows"] == 0 or cache["max_rows"] >= max_rows):
                cached = [dict(d) for d in cache["drivers"]]
                if DRIVER_ROSTER_REFRESH_STATUSES_ON_CACHE_HIT:
                    _apply_latest_simplyfleet_statuses(cached)
                return (
                    cached,
                    None,
                    list(cache["collections"]),
                    list(cache["driver_types"]),
                    list(cache["payer_types"]),
                )
    drivers, collections_options, driver_type_options, payer_type_options, error = _build_driver_roster(max_rows)
    if not error:
        with _driver_roster_cache_lock:
            cache["drivers"] = drivers
            cache["max_rows"] = max_rows
            cache["expiry"] = time.time() + DRIVER_ROSTER_CACHE_TTL_SECONDS
            cache["collections"] = collections_options
            cache["driver_types"] = driver_type_options
            cache["payer_types"] = payer_type_options
    if error:
        return [], error, [], [], []
    return (
        [dict(d) for d in drivers],
        None,
        collections_options,
        driver_type_options,
        payer_type_options,
    )


def _get_cached_roster_driver(wa_id: str) -> Optional[Dict[str, Any]]:
    """Return a driver from the warm roster cache without hitting the DB."""
    normalized = _normalize_wa_id(wa_id)
    if not normalized:
        return None
    now = time.time()
    with _driver_roster_cache_lock:
        cache = _driver_roster_cache
        if not cache["drivers"] or cache["expiry"] <= now:
            return None
        for d in cache["drivers"]:
            if _normalize_wa_id(d.get("wa_id")) == normalized:
                return dict(d)
    return None


def _get_cached_driver_detail(wa_id: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    with _driver_detail_cache_lock:
        entry = _driver_detail_cache.get(wa_id)
        if not entry:
            return None
        expiry, payload = entry
        if expiry < now:
            _driver_detail_cache.pop(wa_id, None)
            return None
        return dict(payload)


def _set_cached_driver_detail(wa_id: str, payload: Dict[str, Any]) -> None:
    with _driver_detail_cache_lock:
        _driver_detail_cache[wa_id] = (time.time() + DRIVER_DETAIL_CACHE_TTL_SECONDS, dict(payload))


def _account_statement_cache_key(account_id: str, limit: int) -> Tuple[str, int]:
    return (account_id, limit)


def _sanitize_statement_limit(
    raw_value: Optional[str],
    *,
    default: int,
    max_limit: int,
    min_limit: int = 10,
) -> int:
    limit = default
    if raw_value is not None and str(raw_value).strip():
        try:
            limit = int(str(raw_value).strip())
        except Exception:
            limit = default
    return max(min_limit, min(limit, max_limit))


def _get_cached_account_statement(account_id: str, limit: int) -> Optional[List[Dict[str, Any]]]:
    key = _account_statement_cache_key(account_id, limit)
    now = time.time()
    with _account_statement_cache_lock:
        entry = _account_statement_cache.get(key)
        if entry and entry[0] > now:
            return [dict(row) for row in entry[1]]
        if entry:
            _account_statement_cache.pop(key, None)
    return None


def _set_cached_account_statement(account_id: str, limit: int, rows: List[Dict[str, Any]]) -> None:
    key = _account_statement_cache_key(account_id, limit)
    expiry = time.time() + ACCOUNT_STATEMENT_CACHE_TTL_SECONDS
    with _account_statement_cache_lock:
        _account_statement_cache[key] = (expiry, [dict(row) for row in rows])


def _set_engagement_preview_cache(preview_id: str, payload: Dict[str, Any]) -> None:
    expiry = time.time() + ENGAGEMENT_PREVIEW_TTL_SECONDS
    with _engagement_preview_cache_lock:
        _engagement_preview_cache[preview_id] = {"expiry": expiry, **payload}


def _get_engagement_preview_cache(preview_id: str) -> Optional[Dict[str, Any]]:
    with _engagement_preview_cache_lock:
        entry = _engagement_preview_cache.get(preview_id)
        if not entry:
            return None
        if entry.get("expiry", 0) < time.time():
            _engagement_preview_cache.pop(preview_id, None)
            return None
        return dict(entry)


def _prune_engagement_preview_cache() -> None:
    now = time.time()
    with _engagement_preview_cache_lock:
        for key, entry in list(_engagement_preview_cache.items()):
            if entry.get("expiry", 0) < now:
                _engagement_preview_cache.pop(key, None)


def _set_engagement_send_progress(campaign_id: str, payload: Dict[str, Any]) -> None:
    with _engagement_send_progress_lock:
        _engagement_send_progress[campaign_id] = dict(payload)


def _get_engagement_send_progress(campaign_id: str) -> Optional[Dict[str, Any]]:
    with _engagement_send_progress_lock:
        entry = _engagement_send_progress.get(campaign_id)
        return dict(entry) if entry else None


def _update_engagement_send_progress(campaign_id: str, **updates: Any) -> None:
    with _engagement_send_progress_lock:
        entry = _engagement_send_progress.get(campaign_id, {})
        entry.update(updates)
        entry["updated_at"] = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
        _engagement_send_progress[campaign_id] = entry


def _prune_engagement_send_progress(max_age_seconds: int = 3600) -> None:
    cutoff = time.time() - max_age_seconds
    with _engagement_send_progress_lock:
        for key, entry in list(_engagement_send_progress.items()):
            started_at = entry.get("started_at_unix") or 0
            try:
                started_at = float(started_at)
            except Exception:
                started_at = 0
            if started_at and started_at < cutoff:
                _engagement_send_progress.pop(key, None)


def _fetch_account_statement_with_connection(
    conn,
    account_id: str,
    limit: int,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    sanitized_limit = max(1, min(limit, 500))
    cached = _get_cached_account_statement(account_id, sanitized_limit)
    if cached is not None:
        return cached, None
    sql = """
    SELECT date, reference, total AS debit, NULL AS credit, 'Invoice' AS source
    FROM mnc_report.xero_invoices
    WHERE account_id = %s AND COALESCE(status, '') NOT IN ('DELETED', 'VOIDED')
    UNION ALL
    SELECT date, 'Payments' AS reference, NULL AS debit, SUM(amount) AS credit, 'Payment' AS source
    FROM (
        SELECT date, amount FROM mnc_report.xero_payments
        WHERE account_id = %s AND COALESCE(status, '') <> 'DELETED'
        UNION ALL
        SELECT date, amount FROM mnc_report.xero_overpayments
        WHERE account_id = %s AND COALESCE(UPPER(status), '') <> 'VOIDED'
    ) p
    GROUP BY date
    UNION ALL
    SELECT date, reference, NULL AS debit, total AS credit, 'Credit Note' AS source
    FROM mnc_report.xero_credit_notes
    WHERE account_id = %s AND COALESCE(UPPER(status), '') NOT IN ('DELETED', 'VOIDED')
    ORDER BY date DESC
    LIMIT %s
    """
    params = (account_id, account_id, account_id, account_id, sanitized_limit)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() or []
        result = [dict(r) for r in rows]
        _set_cached_account_statement(account_id, sanitized_limit, result)
        return result, None
    except Exception as exc:
        log.warning("account statement query failed: %s", exc)
        return [], "could not load account statement"


def get_account_statement(account_id: str, limit: int = 100) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Return a combined account statement for a Xero contact."""
    if not mysql_available():
        return [], "database not configured"
    if not account_id:
        return [], "missing account_id"
    try:
        conn = get_mysql()
    except Exception as exc:
        return [], f"database unavailable: {exc}"
    try:
        return _fetch_account_statement_with_connection(conn, account_id, limit)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _pick_statement_account_sync(
    candidates: List[str],
    limit: int = 200,
) -> Tuple[Optional[str], List[Dict[str, Any]], Optional[str]]:
    if not candidates:
        return None, [], None
    if not mysql_available():
        return None, [], "database not configured"
    try:
        conn = get_mysql()
    except Exception as exc:
        return None, [], f"database unavailable: {exc}"
    try:
        seen: set[str] = set()
        best_id: Optional[str] = None
        best_stmt: List[Dict[str, Any]] = []
        best_err: Optional[str] = None
        for cid in candidates:
            if not cid:
                continue
            cid = str(cid).strip()
            if not cid or cid in seen:
                continue
            seen.add(cid)
            if best_id is None:
                best_id = cid
            stmt, err = _fetch_account_statement_with_connection(conn, cid, limit)
            if stmt:
                return cid, stmt, err
            if not best_stmt:
                best_stmt = stmt
                best_err = err
        return best_id, best_stmt, best_err
    finally:
        try:
            conn.close()
        except Exception:
            pass


def warm_driver_roster_cache() -> None:
    try:
        drivers, error, *_ = _load_cached_driver_roster(DRIVER_ROSTER_WARM_LIMIT)
        if error:
            log.warning("Driver roster cache warm failed: %s", error)
        else:
            log.info("Driver roster cache warmed with %s drivers", len(drivers))
    except Exception as exc:
        log.warning("Driver roster cache warm exception: %s", exc)


def _get_nudge_record(conn, wa_id: str, nudge_date: datetime.date) -> Optional[Dict[str, Any]]:
    table = f"{MYSQL_DB}.driver_zero_trip_nudges"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT nudge_count, last_nudge_at FROM {table} WHERE wa_id=%s AND nudge_date=%s",
                (wa_id, nudge_date),
            )
            row = cur.fetchone()
            return row
    except Exception as exc:
        log.warning("Failed to fetch nudge record for %s: %s", wa_id, exc)
    return None

def _upsert_nudge_record(conn, wa_id: str, nudge_date: datetime.date, count: int) -> None:
    table = f"{MYSQL_DB}.driver_zero_trip_nudges"
    now_str = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table} (wa_id, nudge_date, nudge_count, last_nudge_at)
                VALUES (%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  nudge_count=VALUES(nudge_count),
                  last_nudge_at=VALUES(last_nudge_at)
                """,
                (wa_id, nudge_date, count, now_str),
            )
    except Exception as exc:
        log.warning("Failed to upsert nudge record for %s: %s", wa_id, exc)

def _clear_nudge_record(conn, wa_id: str, nudge_date: datetime.date) -> None:
    table = f"{MYSQL_DB}.driver_zero_trip_nudges"
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE wa_id=%s AND nudge_date=%s",
                (wa_id, nudge_date),
            )
    except Exception as exc:
        log.warning("Failed to clear nudge record for %s: %s", wa_id, exc)

def _build_zero_trip_nudge_message(nudge_number: int) -> Tuple[str, int]:
    if nudge_number <= 0:
        nudge_number = 1
    idx = min(nudge_number - 1, len(ZERO_TRIP_NUDGE_MESSAGES) - 1)
    base = ZERO_TRIP_NUDGE_MESSAGES[idx]
    message = f"{base} {ZERO_TRIP_SUPPORT_HINT}".strip()
    return message, idx

def _log_zero_trip_nudge(
    wa_id: str,
    message: str,
    status: str,
    outbound_id: Optional[str],
    *,
    nudge_number: int,
    template_index: int,
) -> None:
    send_dt = jhb_now()
    timestamp_unix = str(int(send_dt.timestamp()))
    event_id = _insert_nudge_event(
        wa_id=wa_id,
        nudge_number=nudge_number,
        template_index=template_index,
        template_message=message,
        send_status=status,
        whatsapp_message_id=outbound_id,
        send_ts=send_dt,
    )

    s_label, s_score, s_raw = analyze_sentiment(message)
    conversation_id = outbound_id or f"{ZERO_TRIP_NUDGE_INTENT}-{timestamp_unix}"
    log_message(
        direction="OUTBOUND",
        wa_id=wa_id,
        text=message,
        intent=ZERO_TRIP_NUDGE_INTENT,
        status=status,
        wa_message_id=outbound_id,
        message_id=outbound_id,
        business_number=None,
        phone_number_id=None,
        origin_type="whatsapp",
        raw_json={"nudge": True, "status": status},
        timestamp_unix=timestamp_unix,
        sentiment=s_label,
        sentiment_score=s_score,
        intent_label=ZERO_TRIP_NUDGE_INTENT,
        ai_raw=s_raw,
        conversation_id=conversation_id,
    )
    if status == "sent":
        ctx = load_context_file(wa_id)
        ctx["_last_nudge_message"] = message
        ctx["_last_nudge_at"] = timestamp_unix
        ctx["_last_nudge_outbound_id"] = outbound_id
        if event_id is not None:
            ctx["_last_nudge_event_id"] = event_id
        ctx["_last_nudge_response_logged"] = False
        save_context_file(wa_id, ctx)
        save_context_db(wa_id, ZERO_TRIP_NUDGE_INTENT, message, ctx)

def _process_driver_nudge(
    conn,
    driver: Dict[str, Any],
    start_dt: datetime,
    end_dt: datetime,
) -> None:
    wa_id = driver.get("wa_id")
    contact_ids = driver.get("contact_ids") or []
    if not wa_id or not contact_ids:
        return
    ctx = load_context_file(wa_id)
    if ctx.get("_global_opt_out"):
        return

    trip_count = _count_completed_trips(conn, contact_ids, start_dt, end_dt)
    today = start_dt.date()
    record = _get_nudge_record(conn, wa_id, today)

    if trip_count > 0:
        if record:
            _clear_nudge_record(conn, wa_id, today)
        return

    current_count = int(record["nudge_count"]) if record and record.get("nudge_count") is not None else 0
    if current_count >= ZERO_TRIP_MAX_NUDGES_PER_DAY:
        return

    nudge_number = current_count + 1
    message, template_index = _build_zero_trip_nudge_message(nudge_number)
    outbound_id = send_whatsapp_text(wa_id, message)

    if outbound_id:
        _upsert_nudge_record(conn, wa_id, today, current_count + 1)
        _log_zero_trip_nudge(
            wa_id,
            message,
            "sent",
            outbound_id,
            nudge_number=nudge_number,
            template_index=template_index,
        )
        log.info("[NUDGE] Sent zero-trip nudge #%s to %s", current_count + 1, wa_id)
    else:
        _log_zero_trip_nudge(
            wa_id,
            message,
            "send_failed",
            outbound_id,
            nudge_number=nudge_number,
            template_index=template_index,
        )
        log.warning("[NUDGE] Failed to send zero-trip nudge to %s", wa_id)

def _run_zero_trip_nudge_cycle() -> None:
    if not ZERO_TRIP_NUDGES_ENABLED:
        return
    if not mysql_available():
        log.debug("Skipping zero-trip nudge cycle: MySQL unavailable.")
        return
    try:
        conn = get_mysql()
    except Exception as exc:
        log.warning("Cannot start zero-trip nudge cycle: %s", exc)
        return

    try:
        drivers = _collect_active_drivers(conn)
        if not drivers:
            log.debug("Zero-trip nudge cycle: no active drivers found.")
            return
        start_dt, end_dt = jhb_today_range()
        for driver in drivers:
            try:
                _process_driver_nudge(conn, driver, start_dt, end_dt)
            except Exception as exc:
                log.warning("Failed to process nudge for %s: %s", driver.get("wa_id"), exc)
    finally:
        try:
            conn.close()
        except Exception:
            pass

_zero_trip_worker_started = False

def _zero_trip_nudge_worker():
    while True:
        try:
            now = jhb_now()
            if ZERO_TRIP_NUDGE_SKIP_SUNDAYS and now.weekday() == 6:
                next_run = (now + timedelta(days=1)).replace(
                    hour=ZERO_TRIP_NUDGE_START_HOUR,
                    minute=ZERO_TRIP_NUDGE_START_MINUTE,
                    second=0,
                    microsecond=0,
                )
                wait_seconds = max((next_run - now).total_seconds(), 60.0)
                log.info("[NUDGE] Sunday detected; sleeping until %s before sending nudges.", next_run.isoformat(sep=" ", timespec="seconds"))
                time.sleep(wait_seconds)
                continue

            first_run_today = now.replace(
                hour=ZERO_TRIP_NUDGE_START_HOUR,
                minute=ZERO_TRIP_NUDGE_START_MINUTE,
                second=0,
                microsecond=0,
            )
            if now < first_run_today:
                wait_seconds = max((first_run_today - now).total_seconds(), 60.0)
                time.sleep(wait_seconds)
                continue

            _run_zero_trip_nudge_cycle()
        except Exception as exc:
            log.error("Zero-trip nudge cycle crashed: %s", exc)

        post_cycle_now = jhb_now()
        next_run = post_cycle_now + timedelta(seconds=ZERO_TRIP_NUDGE_INTERVAL_SECONDS)
        if ZERO_TRIP_NUDGE_SKIP_SUNDAYS:
            while next_run.weekday() == 6:
                next_run = (next_run + timedelta(days=1)).replace(
                    hour=ZERO_TRIP_NUDGE_START_HOUR,
                    minute=ZERO_TRIP_NUDGE_START_MINUTE,
                    second=0,
                    microsecond=0,
                )

        same_day_start = next_run.replace(
            hour=ZERO_TRIP_NUDGE_START_HOUR,
            minute=ZERO_TRIP_NUDGE_START_MINUTE,
            second=0,
            microsecond=0,
        )
        if next_run < same_day_start:
            next_run = same_day_start

        wait_seconds = max((next_run - jhb_now()).total_seconds(), 60.0)
        time.sleep(wait_seconds)

def _start_zero_trip_nudge_worker():
    global _zero_trip_worker_started
    if _zero_trip_worker_started:
        return
    if not ZERO_TRIP_NUDGES_ENABLED or ZERO_TRIP_NUDGE_INTERVAL_SECONDS <= 0:
        log.info("Zero-trip nudge worker disabled by configuration.")
        _zero_trip_worker_started = True
        return
    thread = threading.Thread(target=_zero_trip_nudge_worker, name="zero-trip-nudge", daemon=True)
    thread.start()
    _zero_trip_worker_started = True
    log.info("Zero-trip nudge worker started (interval=%ss).", ZERO_TRIP_NUDGE_INTERVAL_SECONDS)

# -----------------------------------------------------------------------------
# Intraday performance updates (bi-hourly)
# -----------------------------------------------------------------------------
def _intraday_checkpoint_targets(daily_target: int) -> List[Tuple[int, int]]:
    checkpoints: List[Tuple[int, int]] = []
    last_target = 0
    last_hour = INTRADAY_CHECKPOINT_RATIOS[-1][0] if INTRADAY_CHECKPOINT_RATIOS else 18
    for hour, ratio in INTRADAY_CHECKPOINT_RATIOS:
        target = max(1, int(math.ceil(daily_target * ratio)))
        if target <= last_target:
            target = last_target + 1
        if hour == last_hour and target < daily_target:
            target = daily_target
        checkpoints.append((hour, target))
        last_target = target
    return checkpoints

def _intraday_schedule_label() -> str:
    return ", ".join([f"{hour:02d}:00" for hour, _ratio in INTRADAY_CHECKPOINT_RATIOS])

def _intraday_due_slot(now: datetime, daily_target: int) -> Optional[Tuple[int, int]]:
    grace = timedelta(minutes=INTRADAY_UPDATE_GRACE_MINUTES)
    for hour, target in _intraday_checkpoint_targets(daily_target):
        slot_time = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        if slot_time <= now <= slot_time + grace:
            return hour, target
    return None

def _intraday_next_checkpoint(now: datetime, daily_target: int) -> Tuple[int, int]:
    checkpoints = _intraday_checkpoint_targets(daily_target)
    for hour, target in checkpoints:
        if now.hour < hour or (now.hour == hour and now.minute <= 59):
            return hour, target
    return checkpoints[-1]

def _intraday_target_for_hour(requested_hour: int, daily_target: int) -> Tuple[int, int]:
    checkpoints = _intraday_checkpoint_targets(daily_target)
    for hour, target in checkpoints:
        if requested_hour <= hour:
            return hour, target
    return checkpoints[-1]

def _intraday_slot_sent(ctx: Dict[str, Any], date_str: str, slot_hour: int) -> bool:
    payload = ctx.get("_intraday_last_slot")
    if isinstance(payload, dict):
        try:
            return payload.get("date") == date_str and int(payload.get("hour") or -1) == slot_hour
        except Exception:
            return False
    if isinstance(payload, str):
        return payload == f"{date_str}:{slot_hour}"
    return False

def _intraday_daily_trip_target(ctx: Dict[str, Any], driver: Dict[str, Any]) -> int:
    targets = get_model_targets(driver.get("asset_model"))
    goal_targets = _recent_goal_targets(ctx)
    if goal_targets:
        targets = dict(targets or {})
        targets.update(goal_targets)
        _min_hours, min_trips = _min_target_thresholds()
        if targets.get("trip_count") and targets["trip_count"] < min_trips:
            targets["trip_count"] = min_trips
    weekly_trips = targets.get("trip_count") or DEFAULT_TARGETS["trip_count"]
    try:
        weekly_trips_val = float(weekly_trips)
    except Exception:
        weekly_trips_val = float(DEFAULT_TARGETS["trip_count"])
    daily_target = int(math.ceil(weekly_trips_val / 5.0))
    return max(INTRADAY_DAILY_MIN_FINISHED_ORDERS, daily_target)

def _today_acceptance_rate(metrics: Dict[str, Any]) -> Optional[float]:
    rate = metrics.get("today_acceptance_rate")
    if rate is not None:
        return rate
    sent = _coerce_float(metrics.get("today_trips_sent"))
    accepted = _coerce_float(metrics.get("today_trips_accepted"))
    if sent:
        return _normalize_acceptance((accepted or 0.0) / sent)
    return None

def _pick_intraday_variant(ctx: Dict[str, Any], key: str, options: List[str]) -> str:
    if not options:
        return ""
    cache = ctx.setdefault("_intraday_variant_cache", {})
    last_idx = cache.get(key)
    idxs = list(range(len(options)))
    if isinstance(last_idx, int) and last_idx in idxs and len(idxs) > 1:
        idxs.remove(last_idx)
    idx = random.choice(idxs)
    cache[key] = idx
    return options[idx]

def _format_intraday_update_message(
    *,
    now: datetime,
    finished_trips: int,
    acceptance_rate: Optional[float],
    target_now: int,
    daily_target: int,
    ctx: Dict[str, Any],
) -> str:
    slot_label = now.strftime("%H:%M")
    acceptance_text = _fmt_percent(acceptance_rate)
    acceptance_clause = ""
    if acceptance_text:
        acceptance_clause = _pick_intraday_variant(
            ctx,
            "intraday_acceptance",
            INTRADAY_ACCEPTANCE_VARIANTS,
        ).format(acceptance=acceptance_text)
    summary = _pick_intraday_variant(ctx, "intraday_summary", INTRADAY_SUMMARY_VARIANTS).format(
        time=slot_label,
        finished=int(finished_trips),
        acceptance_clause=acceptance_clause,
    )
    target_line = _pick_intraday_variant(ctx, "intraday_target", INTRADAY_TARGET_VARIANTS).format(
        time=slot_label,
        target_now=int(target_now),
        daily_target=int(daily_target),
    )
    diff = int(finished_trips) - int(target_now)
    if diff > 0:
        pace_line = _pick_intraday_variant(ctx, "intraday_ahead", INTRADAY_AHEAD_VARIANTS).format(diff=diff)
        if now.hour in {10, 12, 14, 18}:
            praise = _pick_intraday_variant(ctx, "intraday_praise", INTRADAY_PRAISE_PREFIXES)
            if praise:
                pace_line = f"{praise} {pace_line}"
    elif diff == 0:
        pace_line = _pick_intraday_variant(ctx, "intraday_on_target", INTRADAY_ON_TARGET_VARIANTS)
    else:
        shortfall_daily = max(int(INTRADAY_DAILY_MIN_FINISHED_ORDERS) - int(finished_trips), 0)
        pace_line = _pick_intraday_variant(ctx, "intraday_behind", INTRADAY_BEHIND_VARIANTS).format(
            behind=abs(diff),
            shortfall=shortfall_daily,
            daily_goal=int(INTRADAY_DAILY_MIN_FINISHED_ORDERS),
        )
    prompt = _pick_intraday_variant(ctx, "intraday_prompt", INTRADAY_PROMPT_QUESTIONS)
    if prompt:
        return f"{summary} {target_line} {pace_line} {prompt}".strip()
    return f"{summary} {target_line} {pace_line}".strip()

def _format_intraday_missing_message(now: datetime, reason: Optional[str], ctx: Dict[str, Any]) -> str:
    slot_label = now.strftime("%H:%M")
    pretty = _pretty_reason(reason)
    if pretty:
        template = _pick_intraday_variant(ctx, "intraday_missing_reason", INTRADAY_MISSING_WITH_REASON_VARIANTS)
        base = template.format(time=slot_label, reason=pretty)
        prompt = _pick_intraday_variant(ctx, "intraday_missing_prompt", INTRADAY_MISSING_PROMPT_QUESTIONS)
        return f"{base} {prompt}".strip() if prompt else base
    template = _pick_intraday_variant(ctx, "intraday_missing_generic", INTRADAY_MISSING_GENERIC_VARIANTS)
    base = template.format(time=slot_label)
    prompt = _pick_intraday_variant(ctx, "intraday_missing_prompt", INTRADAY_MISSING_PROMPT_QUESTIONS)
    return f"{base} {prompt}".strip() if prompt else base

def _log_intraday_update(
    *,
    wa_id: str,
    message: str,
    status: str,
    outbound_id: Optional[str],
    slot_hour: int,
    target_now: int,
    daily_target: int,
    finished_trips: Optional[int],
    acceptance_rate: Optional[float],
) -> None:
    send_dt = jhb_now()
    timestamp_unix = str(int(send_dt.timestamp()))
    s_label, s_score, s_raw = analyze_sentiment(message)
    convo_id = outbound_id or f"intraday-{wa_id}-{send_dt.date().isoformat()}-{slot_hour}"
    log_message(
        direction="OUTBOUND",
        wa_id=wa_id,
        text=message,
        intent=INTRADAY_UPDATE_INTENT,
        status=status,
        wa_message_id=outbound_id,
        message_id=outbound_id,
        business_number=None,
        phone_number_id=None,
        origin_type="whatsapp",
        raw_json={
            "intraday_update": True,
            "slot_hour": slot_hour,
            "target_now": target_now,
            "daily_target": daily_target,
            "finished_trips": finished_trips,
            "acceptance_rate": acceptance_rate,
        },
        timestamp_unix=timestamp_unix,
        sentiment=s_label,
        sentiment_score=s_score,
        intent_label=INTRADAY_UPDATE_INTENT,
        ai_raw=s_raw,
        conversation_id=convo_id,
    )

def _run_intraday_update_cycle() -> None:
    if not INTRADAY_UPDATES_ENABLED:
        return
    if not mysql_available():
        log.debug("Skipping intraday updates: MySQL unavailable.")
        return
    now = jhb_now()
    grace = timedelta(minutes=INTRADAY_UPDATE_GRACE_MINUTES)
    if not any(
        now.replace(hour=hour, minute=0, second=0, microsecond=0) <= now <= now.replace(hour=hour, minute=0, second=0, microsecond=0) + grace
        for hour, _ratio in INTRADAY_CHECKPOINT_RATIOS
    ):
        return

    today = now.date()
    date_str = today.isoformat()
    for ctx_path in CTX_DIR.glob("*.json"):
        wa_id = ctx_path.stem
        if not wa_id:
            continue
        ctx = load_context_file(wa_id)
        if ctx.get("_global_opt_out"):
            continue
        if not ctx.get("_intraday_updates_enabled"):
            continue
        if ctx.get("_intraday_updates_paused"):
            continue
        driver = lookup_driver_by_wa(wa_id)
        if not driver:
            continue
        daily_target = _intraday_daily_trip_target(ctx, driver)
        due = _intraday_due_slot(now, daily_target)
        if not due:
            continue
        slot_hour, target_now = due
        if _intraday_slot_sent(ctx, date_str, slot_hour):
            continue

        metrics, reason = get_driver_kpis(wa_id, driver, include_today=True)
        finished_val: Optional[int] = None
        acceptance_rate = None
        if metrics:
            finished_raw = metrics.get("today_finished_trips")
            if finished_raw is None:
                finished_raw = metrics.get("today_trips_sent")
            try:
                finished_val = int(round(float(finished_raw))) if finished_raw is not None else 0
            except Exception:
                finished_val = 0
            acceptance_rate = _today_acceptance_rate(metrics)

        if not _reserve_intraday_update(
            wa_id=wa_id,
            update_date=today,
            slot_hour=slot_hour,
            target_trips=int(target_now),
            finished_trips=finished_val,
            acceptance_rate=acceptance_rate,
        ):
            continue

        if metrics:
            message = _format_intraday_update_message(
                now=now,
                finished_trips=finished_val or 0,
                acceptance_rate=acceptance_rate,
                target_now=target_now,
                daily_target=daily_target,
                ctx=ctx,
            )
        else:
            message = _format_intraday_missing_message(now, reason, ctx)

        outbound_id = send_whatsapp_text(wa_id, message)
        status = "sent" if outbound_id else "send_failed"
        _update_intraday_update_status(
            wa_id=wa_id,
            update_date=today,
            slot_hour=slot_hour,
            status=status,
            whatsapp_message_id=outbound_id,
        )
        _log_intraday_update(
            wa_id=wa_id,
            message=message,
            status=status,
            outbound_id=outbound_id,
            slot_hour=slot_hour,
            target_now=target_now,
            daily_target=daily_target,
            finished_trips=finished_val,
            acceptance_rate=acceptance_rate,
        )
        ctx["_intraday_last_slot"] = {"date": date_str, "hour": slot_hour}
        ctx["_intraday_last_sent_at"] = time.time()
        ctx["_intraday_last_message"] = message
        save_context_file(wa_id, ctx)

_intraday_updates_worker_started = False

def _intraday_updates_worker() -> None:
    while True:
        try:
            _run_intraday_update_cycle()
        except Exception as exc:
            log.warning("Intraday update worker error: %s", exc)
        time.sleep(max(60, INTRADAY_UPDATE_INTERVAL_SECONDS))

def _start_intraday_updates_worker() -> None:
    global _intraday_updates_worker_started
    if _intraday_updates_worker_started:
        return
    if not INTRADAY_UPDATES_ENABLED or INTRADAY_UPDATE_INTERVAL_SECONDS <= 0:
        log.info("Intraday updates worker disabled by configuration.")
        _intraday_updates_worker_started = True
        return
    thread = threading.Thread(target=_intraday_updates_worker, name="intraday-updates", daemon=True)
    thread.start()
    _intraday_updates_worker_started = True
    log.info("Intraday updates worker started (interval=%ss).", INTRADAY_UPDATE_INTERVAL_SECONDS)

# -----------------------------------------------------------------------------
# Tone / NLG
# -----------------------------------------------------------------------------
DINEO_TONE = """
You are Dineo, a warm WhatsApp coach for My Next Car drivers in Johannesburg.
Tone rules:
- Always polite and friendly; encouraging and non-judgmental.
- Prefer suggestions over commands (“you could”, “let’s”, “if you’d like”).
- Keep replies short (≤ 2 sentences). No greeting unless intent is 'greeting'.
- Use at most one supportive emoji. Do not repeat the user’s name. First name only if needed.
- If you don’t have data, say so briefly and offer a next step or a clarifying question.
- Use contractions and casual phrasing; avoid sounding scripted.
- Vary wording so similar replies do not repeat back-to-back.

Facts:
- My Next Car is an Exclusive partner of Bolt and drivers are not allowed to use the vehicles rented out by My Next Car for to operate on Uber or any other E-Hailing platform.
-  Maponya Mall in Soweto is a dangerous zone to work.
- My Next Car weekly performance targets are based on vehicle models:
    1. Bajaj Qute = 55 Online Hours per week, 80% Average Acceptance Rate, R130+ Earnings Per Hours, R7150 Gross Earnings per Week and 165+ Trips per week
    2. Toyota Vitz = 55 Online Hours per week, 80% Average Acceptance Rate, R150+ Earnings Per Hours, R8250 Gross Earnings per Week and 105+ Trips per week
    3. Suzuki S-Presso = 55 Online Hours per week, 80% Average Acceptance Rate, R150+ Earnings Per Hours, R8250 Gross Earnings per Week and 105+ Trips per week
    4. Suzuki Dzire = 55 Online Hours per week, 80% Average Acceptance Rate, R150+ Earnings Per Hours, R8250 Gross Earnings per Week and 105+ Trips per week
    5. Nissan Micra & Nissan Almera = 55 Online Hours per week, 80% Average Acceptance Rate, R150+ Earnings Per Hours, R8250 Gross Earnings per Week and 105+ Trips per week

- Top Drivers at My Next Car earn R12000+ per week and 72+ online hours.

"""

INTRADAY_SUMMARY_VARIANTS = [
    "Update {time}: {finished} finished trips today{acceptance_clause}.",
    "Quick check-in {time}: {finished} trips so far today{acceptance_clause}.",
    "Progress {time}: {finished} trips done today{acceptance_clause}.",
    "Status {time}: {finished} trips completed today{acceptance_clause}.",
]
INTRADAY_ACCEPTANCE_VARIANTS = [
    ", {acceptance} acceptance",
    " — {acceptance} acceptance",
    "; acceptance {acceptance}",
    " ({acceptance} acceptance)",
]
INTRADAY_TARGET_VARIANTS = [
    "Target by now: {target_now} (daily goal {daily_target}).",
    "Checkpoint target: {target_now} of {daily_target}.",
    "By {time}, aim for {target_now} (daily goal {daily_target}).",
]
INTRADAY_PRAISE_PREFIXES = [
    "Well done!",
    "Nice work!",
    "Great job!",
    "Solid shift!",
]
INTRADAY_AHEAD_VARIANTS = [
    "You're {diff} ahead — keep that pace.",
    "Ahead by {diff}. Keep it up.",
    "Great pace: {diff} ahead.",
    "You're {diff} ahead — stay in those busy zones.",
]
INTRADAY_ON_TARGET_VARIANTS = [
    "Right on target — keep it steady.",
    "On pace so far — nice work.",
    "Right on pace — keep rolling.",
]
INTRADAY_BEHIND_VARIANTS = [
    "You're {behind} behind — shortfall to {daily_goal} today: {shortfall} trips.",
    "Behind by {behind}. You need {shortfall} more to reach {daily_goal} today.",
    "Off pace by {behind} — shortfall to {daily_goal} today: {shortfall}.",
    "You're {behind} short of the checkpoint — {shortfall} trips to {daily_goal} today.",
]
INTRADAY_MISSING_WITH_REASON_VARIANTS = [
    "Update {time}: I couldn't read today's stats because {reason}. I'll retry at the next check-in.",
    "Quick update {time}: stats unavailable because {reason}. I'll try again at the next check-in.",
    "Update {time}: no stats yet ({reason}). I'll check again soon.",
]
INTRADAY_MISSING_GENERIC_VARIANTS = [
    "Update {time}: I couldn't read today's stats yet. I'll retry at the next check-in.",
    "Quick check-in {time}: no stats yet. I'll try again at the next check-in.",
    "Update {time}: today's stats aren't available yet. I'll check again soon.",
]
INTRADAY_PROMPT_QUESTIONS = [
    "Want a quick target check-in?",
    "Need tips for the next block?",
    "Want me to check the next checkpoint for you?",
    "Want tips on where to drive next?",
]
INTRADAY_MISSING_PROMPT_QUESTIONS = [
    "Want tips on where to drive while I retry?",
    "Want a quick plan for the next block?",
    "Need tips on where to head next?",
]

EARNINGS_TIPS_LEADS = [
    "Quick tips",
    "Short tips",
    "A few quick tips",
    "Here are a few quick tips",
]
EARNINGS_TIPS_LINES = [
    "drive 05:00-10:00 and 16:00-19:00, plus weekends; stay near hotspots (CBDs, malls, transport hubs) and reposition after each drop to avoid dead kms.",
    "focus on 05:00-10:00 and 16:00-19:00, plus weekends; work CBDs, malls, and transport hubs and reposition after each drop to avoid dead kms.",
    "hit the 05:00-10:00 and 16:00-19:00 windows, plus weekends; stay close to hotspots and reposition after each drop to avoid dead kms.",
]
EARNINGS_TIPS_CLOSINGS = [
    "How's the shift going - want a simple plan for your usual area and what hours or trips can you commit to this week?",
    "Hope the shift's going okay - want a simple plan for your usual area and what hours or trips can you commit to this week?",
    "Want a simple plan for your usual area - what hours or trips can you commit to this week?",
]
PERFORMANCE_PLAN_VARIANTS = [
    "Plan: {hours_per_day:g} hours/day x {days_per_week} days = {total_hours:g} hours; aim for {trips_per_hour} trips/hour (about {trip_target} trips/week) by staying in busy zones.",
    "Plan: {total_hours:g} hours this week ({hours_per_day:g}h x {days_per_week} days); keep {trips_per_hour} trips/hour to land around {trip_target} trips.",
    "Plan: {hours_per_day:g} hours/day for {days_per_week} days; that is {total_hours:g} hours and about {trip_target} trips if you keep {trips_per_hour} trips/hour.",
]
COMMITMENT_QUESTIONS = [
    "What hours or trips can you commit to this week?",
    "What target feels realistic this week - hours or trips?",
    "What can you commit to this week: hours or trips?",
]

COMMITMENT_BLOCKER_KEYWORDS = {
    "time": [
        "busy", "time", "schedule", "shift", "family", "kids", "child", "school", "study",
        "class", "another job", "work", "church", "funeral", "wedding",
    ],
    "fuel": ["fuel", "petrol", "gas", "money", "cash", "top up", "refuel", "cost"],
    "demand": ["no trips", "low demand", "slow", "quiet", "not getting trips", "few trips"],
    "distance": ["far", "distance", "long trip", "long pickup", "dead km", "dead kms", "too far"],
    "safety": ["unsafe", "danger", "crime", "rob", "robbery", "hijack", "scared", "attack"],
    "app": ["app", "phone", "network", "data", "signal", "gps", "update", "login"],
}
COMMITMENT_BLOCKER_LABELS = {
    "time": "time constraints",
    "fuel": "fuel or cash limits",
    "demand": "low demand or slow trips",
    "distance": "far pickups or long trips",
    "safety": "safety concerns",
    "app": "app or phone issues",
    "other": "a few blockers",
}
COMMITMENT_BLOCKER_TIPS = {
    "time": "Use two peak blocks (05:00-10:00 and 16:00-19:00) and protect those slots.",
    "fuel": "Focus on short pickups and refuel before peak windows to avoid dead kms.",
    "demand": "Stay near hotspots and reposition after each drop to catch the next ride.",
    "distance": "Stick close to hotspots and avoid long pickups to keep trips/hour up.",
    "safety": "Avoid risky zones like Maponya Mall and stick to safer hotspots and daylight peaks.",
    "app": "Restart the app/phone, check data, and update to avoid missing requests.",
    "other": "If you share the main blocker, I can tailor the plan.",
}

INTENT_CONFIRMATION_TTL_SECONDS = int(os.getenv("INTENT_CONFIRMATION_TTL_SECONDS", "21600"))
INTENT_CONFIRMATION_SKIP_INTENTS = {
    "greeting",
    "opt_in",
    "opt_out",
    "assistant_identity",
    "oph_definition",
    "platform_exclusivity",
    "current_time",
    "current_date",
    "smalltalk",
    "acknowledgement",
    "account_inquiry",
    "provide_personal_code",
    "progress_update",
    "daily_target_status",
    "hotspot_summary",
    "earnings_projection",
    "earnings_tips",
    "acceptance_rate_status",
    "earnings_per_hour_status",
    "target_update",
    "target_benefit",
    "schedule_update",
    "bot_identity",
    "voice_unavailable",
    "clarify",
    "unknown",
    "fallback",
    "media_message",
    "media_image",
    "media_document",
    "media_video",
    "media_audio",
    "media_voice",
    "media_location",
    "media_contacts",
    "media_sticker",
    "media_reaction",
}
INTENT_CONFIRMATION_TICKET_INTENTS = {
    "low_demand",
    "account_suspension",
    "app_issue",
    "balance_dispute",
    "medical_issue",
    "no_vehicle",
    "vehicle_repossession",
    "car_problem",
    CASH_ENABLE_INTENT,
    CASH_BALANCE_UPDATE_INTENT,
    BRANDING_BONUS_INTENT,
    ACCIDENT_REPORT_INTENT,
}
INTENT_CONFIRMATION_LABELS = {
    "account_suspension": "a Bolt account block/suspension",
    "app_issue": "an app/login issue",
    "balance_dispute": "a balance dispute",
    "medical_issue": "a medical issue",
    "no_vehicle": "not having the vehicle right now",
    "vehicle_repossession": "a repossession issue",
    "car_problem": "a vehicle problem",
    CASH_ENABLE_INTENT: "cash rides enablement",
    CASH_BALANCE_UPDATE_INTENT: "a cash rides POP update",
    BRANDING_BONUS_INTENT: "a missing branding/campaign bonus",
    ACCIDENT_REPORT_INTENT: "an accident report",
}
LOW_DEMAND_CONFIRM_OPTIONS = {
    "1": "low_demand",
    "2": "account_suspension",
    "3": "app_issue",
    "4": "hotspot_summary",
}
LOW_DEMAND_CONFIRM_PROMPTS = [
    "Just to confirm: are you 1) not getting trips/low demand, 2) account blocked/suspended, "
    "3) app/login issue, or 4) just want tips on busy areas? Reply 1-4.",
    "Quick check: is this 1) low demand, 2) account blocked, 3) app issue, or 4) just tips on busy areas? Reply 1-4.",
]


def _intent_confirmed_recent(ctx: Dict[str, Any], intent: str) -> bool:
    if not ctx or not intent:
        return False
    bucket = ctx.get("_intent_confirmed")
    if not isinstance(bucket, dict):
        return False
    try:
        ts = float(bucket.get(intent) or 0)
    except Exception:
        return False
    return bool(ts and (time.time() - ts) < INTENT_CONFIRMATION_TTL_SECONDS)


def _mark_intent_confirmed(ctx: Dict[str, Any], intent: str) -> None:
    if not ctx or not intent:
        return
    bucket = ctx.get("_intent_confirmed")
    if not isinstance(bucket, dict):
        bucket = {}
        ctx["_intent_confirmed"] = bucket
    bucket[intent] = time.time()


def _intent_confirmation_blocked(ctx: Dict[str, Any]) -> bool:
    if not isinstance(ctx, dict) or not ctx:
        return False
    if ctx.get("_intent_confirm_pending"):
        return True
    if ctx.get("_pending_intent"):
        return True
    if ctx.get("_awaiting_target_update") or ctx.get("_pending_goal") or ctx.get("_awaiting_goal_confirm"):
        return True
    if _commitment_prompt_active(ctx):
        return True
    if ctx.get("_medical_pending_decision") or ctx.get("_medical_pending_location"):
        return True
    if ctx.get("_medical_pending_commitment") or ctx.get("_medical_pending_certificate"):
        return True
    if ctx.get("_no_vehicle_pending"):
        return True
    if ctx.get("_balance_dispute_pending"):
        return True
    if ctx.get("_low_demand_pending"):
        return True
    if ctx.get("_account_suspension_pending"):
        return True
    if ctx.get("_app_issue_pending"):
        return True
    if ctx.get("_pop_pending_confirmation"):
        return True
    cash_ticket = ctx.get("_cash_ticket")
    if isinstance(cash_ticket, dict) and cash_ticket.get("awaiting_pop", False):
        return True
    bonus_ctx = ctx.get("_branding_bonus")
    if isinstance(bonus_ctx, dict) and bonus_ctx.get("awaiting"):
        return True
    accident_ctx = ctx.get("_accident_case")
    if isinstance(accident_ctx, dict) and accident_ctx.get("awaiting") and not accident_ctx.get("closed"):
        return True
    return False


def _intent_confirmation_prompt(intent: str, msg: str, ctx: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
    if intent == "low_demand":
        prompt = _pick_phrase(LOW_DEMAND_CONFIRM_PROMPTS)
        meta = {
            "intent": intent,
            "type": "choice",
            "options": LOW_DEMAND_CONFIRM_OPTIONS,
            "prompt": prompt,
            "set_at": time.time(),
        }
        return prompt, meta
    label = INTENT_CONFIRMATION_LABELS.get(intent)
    if not label:
        return None
    prompt = f"Just to confirm — is this about {label}? Reply yes or no."
    meta = {
        "intent": intent,
        "type": "yes_no",
        "prompt": prompt,
        "set_at": time.time(),
    }
    return prompt, meta


def _resolve_intent_confirmation_reply(msg: str, ctx: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    pending = ctx.get("_intent_confirm_pending")
    if not isinstance(pending, dict):
        return None, None
    intent = pending.get("intent")
    kind = pending.get("type") or "yes_no"
    raw = _normalize_text(msg or "")
    lowered = raw.lower().strip()
    if not lowered:
        return None, pending.get("prompt") or "Please reply yes or no."

    if kind == "choice":
        choice_match = re.search(r"\b([1-4])\b", lowered)
        chosen = None
        if choice_match:
            chosen = (pending.get("options") or {}).get(choice_match.group(1))
        else:
            if _is_account_suspension(lowered):
                chosen = "account_suspension"
            elif _is_app_issue(lowered):
                chosen = "app_issue"
            elif _is_hotspot_query(lowered):
                chosen = "hotspot_summary"
            elif _is_low_demand_issue(lowered):
                chosen = "low_demand"
        if chosen:
            ctx.pop("_intent_confirm_pending", None)
            ctx.pop("_intent_confirm_pending_at", None)
            _mark_intent_confirmed(ctx, chosen)
            return chosen, None
        if _is_negative_confirmation(lowered):
            ctx.pop("_intent_confirm_pending", None)
            ctx.pop("_intent_confirm_pending_at", None)
            return None, "No problem — what do you want help with right now?"
        return None, pending.get("prompt") or "Please reply with 1, 2, 3, or 4."

    if _is_positive_confirmation(lowered):
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        _mark_intent_confirmed(ctx, intent)
        return intent, None
    if _is_negative_confirmation(lowered):
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return None, "No worries — what do you want help with?"

    if intent == "account_suspension" and _is_account_suspension(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == "app_issue" and _is_app_issue(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == "balance_dispute" and _is_balance_dispute(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == "medical_issue" and _is_medical_issue(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == "no_vehicle" and _is_no_vehicle(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == "vehicle_repossession" and _is_vehicle_repossession(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == "car_problem" and _is_car_problem(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == ACCIDENT_REPORT_INTENT and _is_accident_message(lowered):
        _mark_intent_confirmed(ctx, intent)
        ctx.pop("_intent_confirm_pending", None)
        ctx.pop("_intent_confirm_pending_at", None)
        return intent, None
    if intent == BRANDING_BONUS_INTENT and intent:
        if _extract_trip_count(lowered, allow_plain=True) or _extract_week_reference(lowered):
            _mark_intent_confirmed(ctx, intent)
            ctx.pop("_intent_confirm_pending", None)
            ctx.pop("_intent_confirm_pending_at", None)
            return intent, None

    return None, pending.get("prompt") or "Please reply yes or no."


def _is_short_ambiguous_message(text: str) -> bool:
    if not text:
        return True
    cleaned = _normalize_text(text).strip().lower()
    if cleaned in ACKNOWLEDGEMENT_TOKENS or cleaned in YES_TOKENS or cleaned in NO_TOKENS:
        return True
    tokens = re.findall(r"[a-z0-9]+", cleaned)
    if len(tokens) <= 2:
        return True
    if re.fullmatch(r"\d+(?:\.\d+)?", cleaned):
        return True
    return False


def _should_confirm_intent(intent: str, msg: str, ctx: Dict[str, Any], *, message_type: str = "text") -> bool:
    if not intent or intent in INTENT_CONFIRMATION_SKIP_INTENTS:
        return False
    if message_type != "text":
        return False
    if _intent_confirmation_blocked(ctx):
        return False
    if _intent_confirmed_recent(ctx, intent):
        return False
    if intent in INTENT_CONFIRMATION_TICKET_INTENTS:
        return True
    if _is_short_ambiguous_message(msg):
        return True
    return False

# -----------------------------------------------------------------------------
# Build reply
# -----------------------------------------------------------------------------
def build_reply(
    intent: str,
    msg: str,
    d: Dict[str, Any],
    ctx: Dict[str, Any],
    wa_id: str,
    *,
    message_type: str = "text",
    media: Optional[Dict[str, Any]] = None,
    location: Optional[Dict[str, Any]] = None,
) -> str:
    location = location or {}
    name = (d.get("first_name") or (d.get("display_name") or "there").split()[0]).strip()
    ctx["_last_user_message"] = msg
    _clear_stale_performance_followups(ctx)
    pending_state = ctx.get("_pending_intent")
    if intent != "earnings_projection" and pending_state in {PENDING_EARNINGS_TRIPS, PENDING_EARNINGS_AVG}:
        ctx.pop("_pending_intent", None)
    if ctx.get("_pending_intent") == PENDING_REPOSSESSION_REASON:
        if ctx.get("_awaiting_performance_tips") or ctx.get("_awaiting_performance_plan"):
            ctx.pop("_pending_intent", None)
        elif intent in KPI_INTENTS or intent in {"target_benefit", "target_update"}:
            ctx.pop("_pending_intent", None)
    template_amount = _extract_driver_update_amount(ctx)
    if ctx.get("_pop_pending_confirmation") and _pop_pending_expired(ctx):
        ctx.pop("_pop_pending_confirmation", None)
        ctx.pop("_pop_pending_media", None)
        ctx.pop("_pop_pending_at", None)

    if ctx.get("_global_opt_out") and intent not in {"opt_in", "opt_out"}:
        if ctx.get("_global_opt_out_at"):
            return ""
        ctx["_global_opt_out_at"] = time.time()
        body = "I’ve paused messages as requested. Reply 'start' if you want me to resume."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if ctx.get("_pop_pending_confirmation"):
        if message_type in CASH_POP_MEDIA_TYPES and media:
            ctx["_pop_pending_media"] = media
        confirmation = _parse_pop_confirmation(msg)
        if confirmation is True:
            media_payload = ctx.get("_pop_pending_media") or media
            ctx.pop("_pop_pending_confirmation", None)
            ctx.pop("_pop_pending_media", None)
            ctx.pop("_pop_pending_at", None)
            if not media_payload:
                body = "Thanks — please resend the POP photo or document so I can log it with Finance."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="pop_submission")
            if ticket_id:
                ctx["_pop_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": "pending_ops",
                    "opened_at": time.time(),
                }
                append_driver_issue_media(ticket_id, media_payload)
                update_driver_issue_metadata(
                    ticket_id,
                    {
                        "pop_submission": True,
                        "pop_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                        "pop_media_type": media_payload.get("type"),
                        "shared_with_finance": True,
                        "shared_with_finance_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                )
                update_driver_issue_status(ticket_id, "pending_ops")
            body = (
                "Thanks — I’ll share this POP with Finance to validate and allocate it to your account. "
                "I’ll update you once it’s allocated."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if confirmation is False:
            ctx.pop("_pop_pending_confirmation", None)
            ctx.pop("_pop_pending_media", None)
            ctx.pop("_pop_pending_at", None)
            body = "Got it — if you want me to log a payment, just send the POP and I’ll share it with Finance."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        body = "Just to confirm, is this a proof of payment (POP)? Reply yes or no."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "opt_out":
        ctx["_global_opt_out"] = True
        ctx["_global_opt_out_at"] = time.time()
        ctx["_engagement_followup_paused"] = True
        ctx["_engagement_followup_paused_at"] = time.time()
        ctx["_intraday_updates_enabled"] = False
        ctx["_intraday_updates_paused"] = True
        ctx["_intraday_updates_paused_at"] = time.time()
        ctx["_no_vehicle_checkin_pending"] = False
        body = "Understood — I’ll stop messaging. If you want me back later, just say 'start'."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "opt_in":
        ctx.pop("_global_opt_out", None)
        ctx.pop("_global_opt_out_at", None)
        ctx["_engagement_followup_paused"] = False
        ctx["_intraday_updates_paused"] = False
        body = "Thanks — I’m back. What do you want to focus on today: trips, hours, or tips?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if ctx.get("_intent_confirm_pending"):
        confirmed_intent, prompt = _resolve_intent_confirmation_reply(msg, ctx)
        if prompt:
            return soften_reply(_strip_leading_greeting_or_name(prompt, d.get("display_name") or "", name), name)
        if confirmed_intent:
            intent = confirmed_intent
            ctx["_intent_override"] = confirmed_intent

    if _should_confirm_intent(intent, msg, ctx, message_type=message_type):
        prompt_meta = _intent_confirmation_prompt(intent, msg, ctx)
        if prompt_meta:
            prompt, meta = prompt_meta
            ctx["_intent_confirm_pending"] = meta
            ctx["_intent_confirm_pending_at"] = time.time()
            return soften_reply(_strip_leading_greeting_or_name(prompt, d.get("display_name") or "", name), name)

    if intent == "assistant_identity":
        body = "I’m Dineo, your My Next Car coach. What do you want help with right now — busy areas, today’s trips, or your weekly target?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "oph_definition":
        body = "OPH means Orders Received per Hour — the number of trip requests you get each hour. Want me to show your OPH or tips to lift it?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "platform_exclusivity":
        body = (
            "My Next Car is exclusive with Bolt, so operating on Uber/InDrive or other e-hailing platforms is a contract breach. "
            "If you’re unsure about a case, I can ask ops to advise."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent in KPI_INTENTS:
        ctx.pop("_awaiting_performance_tips", None)
        ctx.pop("_awaiting_performance_plan", None)

    if _commitment_prompt_active(ctx):
        if _is_positive_confirmation(msg) and not _has_goal_update_values(msg, allow_plain=True):
            targets = ctx.get("_commitment_prompt_targets") or {}
            if not targets:
                targets = get_model_targets(d.get("asset_model"))
            hours_val = targets.get("online_hours")
            trips_val = targets.get("trip_count")
            hours_text = _coerce_goal_value(hours_val)
            trips_text = _coerce_goal_value(trips_val)
            if hours_val:
                ctx["_goal_online_hours"] = hours_val
            if trips_val:
                ctx["_goal_trip_count"] = trips_val
            ctx["_goal_set_at"] = time.time()
            ctx["_goal_acknowledged_at"] = time.time()
            ctx["_goal_acknowledged_repeat"] = False
            ctx["_awaiting_target_update"] = False
            ctx["_awaiting_goal_confirm"] = False
            ctx.pop("_pending_intent", None)
            ctx.pop("_commitment_prompt_at", None)
            ctx.pop("_commitment_prompt_targets", None)
            if hours_text and trips_text:
                body = f"Locked in — I’ll log {hours_text} hours and {trips_text} trips this week."
            elif hours_text:
                body = f"Locked in — I’ll log {hours_text} hours this week."
            elif trips_text:
                body = f"Locked in — I’ll log {trips_text} trips this week."
            else:
                body = "Locked in — I’ll log your target for this week."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if _is_commitment_adjust_request(msg):
            ctx["_awaiting_target_update"] = True
            ctx["_awaiting_target_update_at"] = time.time()
            ctx.pop("_pending_intent", None)
            ctx.pop("_commitment_prompt_at", None)
            ctx.pop("_commitment_prompt_targets", None)
            body = "No problem — what hours or trips can you commit to this week?"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if _is_intraday_pause_request(msg):
        ctx["_intraday_updates_enabled"] = False
        ctx["_intraday_updates_paused"] = True
        ctx["_intraday_updates_paused_at"] = time.time()
        body = "All good - I'll pause the bi-hourly updates. Reply 'daily update' any time to resume."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if (
        INTRADAY_UPDATES_ENABLED
        and INTRADAY_AUTO_ON_RESPONDED
        and mysql_available()
        and not ctx.get("_intraday_updates_paused")
    ):
        if not ctx.get("_intraday_updates_enabled"):
            ctx["_intraday_updates_opted_at"] = time.time()
            ctx["_intraday_updates_source"] = "auto"
        ctx["_intraday_updates_enabled"] = True

    if message_type in CASH_POP_MEDIA_TYPES and media and _pop_prompt_allowed(ctx, intent):
        pop_hint = _parse_pop_confirmation(msg)
        if pop_hint is True:
            ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="pop_submission")
            if ticket_id:
                ctx["_pop_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": "pending_ops",
                    "opened_at": time.time(),
                }
                append_driver_issue_media(ticket_id, media)
                update_driver_issue_metadata(
                    ticket_id,
                    {
                        "pop_submission": True,
                        "pop_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                        "pop_media_type": media.get("type"),
                        "shared_with_finance": True,
                        "shared_with_finance_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                )
                update_driver_issue_status(ticket_id, "pending_ops")
            body = (
                "Thanks — I’ll share this POP with Finance to validate and allocate it to your account. "
                "I’ll update you once it’s allocated."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        ctx["_pop_pending_confirmation"] = True
        ctx["_pop_pending_media"] = media
        ctx["_pop_pending_at"] = time.time()
        body = "Thanks — is this a proof of payment (POP)? Reply yes or no."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    def _pick_variant(options: List[str], key: str) -> str:
        if not options:
            return ""
        cache = ctx.setdefault("_variant_cache", {})
        last_idx = cache.get(key)
        idxs = list(range(len(options)))
        if isinstance(last_idx, int) and last_idx in idxs and len(idxs) > 1:
            idxs.remove(last_idx)
        idx = random.choice(idxs)
        cache[key] = idx
        return options[idx]

    def _build_earnings_tips_reply() -> str:
        lead = _pick_variant(EARNINGS_TIPS_LEADS, "earnings_tips_lead")
        tips = _pick_variant(EARNINGS_TIPS_LINES, "earnings_tips_lines")
        closing = _pick_variant(EARNINGS_TIPS_CLOSINGS, "earnings_tips_closing")
        return f"{lead}: {tips} {closing}".strip()

    def _build_performance_plan_reply(
        hours_per_day: float,
        days_per_week: int,
        total_hours: float,
        trips_per_hour: int,
        trip_target: int,
    ) -> str:
        plan = _pick_variant(PERFORMANCE_PLAN_VARIANTS, "performance_plan").format(
            hours_per_day=hours_per_day,
            days_per_week=days_per_week,
            total_hours=total_hours,
            trips_per_hour=trips_per_hour,
            trip_target=trip_target,
        )
        question = _pick_variant(COMMITMENT_QUESTIONS, "commitment_question")
        return f"{plan} {question}".strip()

    def _build_low_demand_oph_sentence() -> str:
        contact_ids = (d.get("xero_contact_ids") or [])[:5]
        scope = HOTSPOT_SCOPE_DEFAULT if contact_ids else "global"
        scope_contact_ids = contact_ids if scope != "global" else []
        oph_areas, _oph_reason, oph_label = get_top_oph_areas(
            contact_ids=scope_contact_ids,
            scope=scope,
            timeframe=HOTSPOT_TIMEFRAME_DEFAULT,
            limit=3,
        )
        if not oph_areas and scope != "global":
            fallback_areas, _fallback_reason, fallback_label = get_top_oph_areas(
                contact_ids=[],
                scope="global",
                timeframe=HOTSPOT_TIMEFRAME_DEFAULT,
                limit=3,
            )
            if fallback_areas:
                oph_areas = fallback_areas
                oph_label = fallback_label
                scope = "global"
        if oph_areas:
            return _format_oph_area_sentence(oph_areas, None, oph_label, scope)
        return "Try CBDs, malls, and transport hubs nearby—they usually have higher OPH."

    def _days_left_in_week(now: Optional[datetime] = None) -> int:
        now = now or jhb_now()
        try:
            return max(1, 7 - int(now.weekday()))
        except Exception:
            return 5

    def _parse_commitment_blocker(text: str) -> Optional[str]:
        if not text:
            return None
        lowered = text.strip().lower()
        for key, keywords in COMMITMENT_BLOCKER_KEYWORDS.items():
            for kw in keywords:
                if kw in lowered:
                    return key
        return "other"

    def _commitment_blocker_prompt(
        *,
        offered_hours: Optional[float] = None,
        offered_trips: Optional[float] = None,
    ) -> str:
        min_hours, _min_trips = _min_target_thresholds()
        min_trips = int(ENGAGEMENT_TARGET_TRIPS)
        ctx["_pending_commitment_blocker"] = True
        ctx["_pending_commitment_blocker_at"] = time.time()
        ctx["_pending_commitment_hours"] = offered_hours
        ctx["_pending_commitment_trips"] = offered_trips
        ctx.pop("_pending_goal", None)
        ctx.pop("_pending_goal_hours", None)
        ctx.pop("_pending_goal_trips", None)
        ctx["_awaiting_goal_confirm"] = False
        ctx["_awaiting_target_update"] = False
        return (
            f"I hear you. What's holding you back from at least {min_hours} hours and {min_trips} trips this week "
            "(time, fuel, low demand, safety, distance, or app issues)?"
        )

    def _build_commitment_recovery_reply(
        blocker_key: str,
        *,
        offered_hours: Optional[float] = None,
        offered_trips: Optional[float] = None,
    ) -> str:
        min_hours, _min_trips = _min_target_thresholds()
        min_trips = int(ENGAGEMENT_TARGET_TRIPS)
        label = COMMITMENT_BLOCKER_LABELS.get(blocker_key, COMMITMENT_BLOCKER_LABELS["other"])
        tip = COMMITMENT_BLOCKER_TIPS.get(blocker_key, COMMITMENT_BLOCKER_TIPS["other"]).rstrip(".")
        days_left = _days_left_in_week()
        drive_days = max(1, min(5, days_left))
        hours_per_day = int(math.ceil(min_hours / drive_days))
        trips_per_day = int(math.ceil(min_trips / drive_days))
        return (
            f"Got it - sounds like {label}; {tip} "
            f"With {drive_days} days left, that's about {hours_per_day}h/day and {trips_per_day} trips/day "
            f"to get back on track for {min_hours}h and {min_trips} trips. Can you commit to that?"
        )

    if ctx.get("_pending_commitment_blocker"):
        if intent in {"medical_issue", "vehicle_repossession", "car_problem", "no_vehicle"}:
            ctx.pop("_pending_commitment_blocker", None)
            ctx.pop("_pending_commitment_blocker_at", None)
            ctx.pop("_pending_commitment_hours", None)
            ctx.pop("_pending_commitment_trips", None)
        elif intent in {"fallback", "clarify", "unknown"} and _has_goal_update_values(msg, allow_plain=True):
            intent = "target_update"
            ctx.pop("_pending_commitment_blocker", None)
            ctx.pop("_pending_commitment_blocker_at", None)
            ctx.pop("_pending_commitment_hours", None)
            ctx.pop("_pending_commitment_trips", None)
        else:
            blocker = _parse_commitment_blocker(msg)
            if not blocker or (blocker == "other" and len(msg.strip().split()) < 3):
                body = (
                    "Got it. Is it mainly time, fuel, low demand, safety, distance, or app issues? "
                    "Share the main blocker and I will map a recovery plan."
                )
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            offered_hours = ctx.pop("_pending_commitment_hours", None)
            offered_trips = ctx.pop("_pending_commitment_trips", None)
            ctx.pop("_pending_commitment_blocker", None)
            ctx.pop("_pending_commitment_blocker_at", None)
            ctx["_awaiting_target_update"] = True
            ctx["_awaiting_target_update_at"] = time.time()
            body = _build_commitment_recovery_reply(
                blocker,
                offered_hours=offered_hours,
                offered_trips=offered_trips,
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    def _should_use_template_context(text: str) -> bool:
        if not text:
            return False
        lowered = text.strip().lower()
        if lowered in {"?", "??", "???", "what", "what?", "huh", "huh?"}:
            return True
        keywords = {"outstanding", "balance", "owe", "amount", "which", "what", "why", "mean", "explain", "clarify"}
        return any(k in lowered for k in keywords)

    def _is_affirmative_message(text: str) -> bool:
        if not text:
            return False
        lowered = text.strip().lower()
        tokens = set(re.findall(r"[a-z']+", lowered))
        if tokens and tokens <= YES_TOKENS:
            return True
        return bool(tokens & YES_TOKENS)

    if ctx.get("_awaiting_performance_tips"):
        if _is_affirmative_message(msg) or _is_performance_tip_request(msg):
            ctx["_awaiting_performance_tips"] = False
            ctx["_awaiting_performance_plan"] = True
            ctx["_performance_tips_sent_at"] = time.time()
            ctx["_awaiting_target_update"] = True
            ctx["_awaiting_target_update_at"] = time.time()
            body = _build_earnings_tips_reply()
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if _is_negative_confirmation(msg):
            ctx["_awaiting_performance_tips"] = False
            body = "No problem—if you want tips later, just say the word."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if ctx.get("_awaiting_performance_plan"):
        if _is_affirmative_message(msg) or _is_performance_plan_request(msg):
            ctx["_awaiting_performance_plan"] = False
            hours_per_day, days_per_week = _parse_hours_days_plan(msg)
            hours_per_day = hours_per_day or 11.0
            days_per_week = days_per_week or 5
            total_hours = hours_per_day * days_per_week
            trips_per_hour = 2
            trip_target = int(round(total_hours * trips_per_hour))
            ctx["_goal_online_hours"] = total_hours
            ctx["_goal_trip_count"] = trip_target
            ctx["_goal_set_at"] = time.time()
            ctx["_goal_acknowledged_at"] = time.time()
            ctx["_goal_acknowledged_repeat"] = False
            ctx["_awaiting_target_update"] = True
            ctx["_awaiting_target_update_at"] = time.time()
            body = _build_performance_plan_reply(
                hours_per_day,
                days_per_week,
                total_hours,
                trips_per_hour,
                trip_target,
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if _is_negative_confirmation(msg):
            ctx["_awaiting_performance_plan"] = False
            body = "No worries—if you want a plan later, just say the word."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if _is_peak_time_query(msg):
        body = (
            "Peak demand is usually 05:00-10:00 and 16:00-19:00 on weekdays, plus weekends (especially Fri-Sun). "
            "Want a suggested schedule?"
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    pending_goal = ctx.get("_pending_goal")
    awaiting_goal_confirm = bool(ctx.get("_awaiting_goal_confirm"))
    if awaiting_goal_confirm:
        if not (
            _is_affirmative_message(msg)
            or _extract_trip_count(msg, allow_plain=True)
            or _extract_hours_count(msg, allow_plain=True)
        ):
            ctx["_awaiting_goal_confirm"] = False
            awaiting_goal_confirm = False
    if pending_goal in {"trip_count", "online_hours"}:
        if _needs_target_clarification(msg):
            number = _first_number(msg) or "that"
            body = f"Did you mean {number} trips or {number} hours?"
            ctx["_awaiting_target_update"] = True
            ctx["_awaiting_target_update_at"] = time.time()
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        has_hours_word = _mentions_hours(msg)
        has_trip_word = _mentions_trips(msg)
        min_hours, min_trips = _min_target_thresholds()
        if pending_goal == "trip_count":
            if has_hours_word and not has_trip_word:
                hours_val = _extract_hours_count(msg, allow_plain=False)
                if hours_val:
                    if hours_val < min_hours:
                        body = _commitment_blocker_prompt(offered_hours=hours_val)
                        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                    hours_text = _coerce_goal_value(hours_val) or str(hours_val)
                    ctx["_goal_online_hours"] = hours_val
                    ctx["_goal_set_at"] = time.time()
                    ctx["_goal_acknowledged_at"] = time.time()
                    ctx["_goal_acknowledged_repeat"] = False
                    body = f"Got it — {hours_text} hours. How many trips should we target?"
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            trips = _extract_trip_count(msg, allow_plain=False)
            if trips and trips <= 300:
                if trips < min_trips:
                    body = _commitment_blocker_prompt(offered_trips=trips)
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                hours = ctx.get("_pending_goal_hours") or _extract_no_trips_goals(ctx)[0]
                trips_text = _coerce_goal_value(trips) or str(trips)
                ctx.pop("_pending_goal", None)
                ctx.pop("_pending_goal_hours", None)
                ctx["_goal_trip_count"] = trips
                ctx["_goal_set_at"] = time.time()
                ctx["_goal_acknowledged_at"] = time.time()
                ctx["_goal_acknowledged_repeat"] = False
                ctx["_awaiting_goal_confirm"] = False
                if hours:
                    try:
                        ctx["_goal_online_hours"] = float(str(hours))
                    except Exception:
                        ctx["_goal_online_hours"] = hours
                    body = f"Perfect — I’ll log a goal of {hours} hours and {trips_text} trips this week."
                else:
                    body = f"Perfect — I’ll log a goal of {trips_text} trips this week."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            body = "Got it. How many trips should we target? (e.g., 25)"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if has_trip_word and not has_hours_word:
            trips = _extract_trip_count(msg, allow_plain=False)
            if trips:
                if trips < min_trips:
                    body = _commitment_blocker_prompt(offered_trips=trips)
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                trips_text = _coerce_goal_value(trips) or str(trips)
                ctx["_goal_trip_count"] = trips
                ctx["_goal_set_at"] = time.time()
                ctx["_goal_acknowledged_at"] = time.time()
                ctx["_goal_acknowledged_repeat"] = False
                body = f"Got it — {trips_text} trips. How many hours should we aim for?"
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        hours_val = _extract_hours_count(msg, allow_plain=False)
        if hours_val and hours_val <= 120:
            if hours_val < min_hours:
                body = _commitment_blocker_prompt(offered_hours=hours_val)
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            trips = ctx.get("_pending_goal_trips") or _extract_no_trips_goals(ctx)[1]
            hours_text = _coerce_goal_value(hours_val) or str(hours_val)
            ctx.pop("_pending_goal", None)
            ctx.pop("_pending_goal_trips", None)
            ctx["_goal_online_hours"] = hours_val
            ctx["_goal_set_at"] = time.time()
            ctx["_goal_acknowledged_at"] = time.time()
            ctx["_goal_acknowledged_repeat"] = False
            ctx["_awaiting_goal_confirm"] = False
            if trips:
                try:
                    ctx["_goal_trip_count"] = float(str(trips))
                except Exception:
                    ctx["_goal_trip_count"] = trips
                body = f"Perfect — I’ll log a goal of {hours_text} hours and {trips} trips this week."
            else:
                body = f"Perfect — I’ll log a goal of {hours_text} hours this week."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        body = "Got it. How many hours should we target? (e.g., 40)"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if _is_affirmative_message(msg) and ctx.get("_goal_acknowledged_at") and not pending_goal:
        if not ctx.get("_goal_acknowledged_repeat"):
            ctx["_goal_acknowledged_repeat"] = True
            ctx["_awaiting_goal_confirm"] = False
            body = "All good — if you want to change the target, just send the new hours or trips."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        return ""

    if not pending_goal:
        recent_no_trips = _recent_outbound_template_for_group(ctx, "performance_no_trips_yet") if awaiting_goal_confirm else None
        if recent_no_trips:
            if _needs_target_clarification(msg):
                number = _first_number(msg) or "that"
                body = f"Did you mean {number} hours or {number} trips?"
                ctx["_awaiting_target_update"] = True
                ctx["_awaiting_target_update_at"] = time.time()
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            has_hours_word = _mentions_hours(msg)
            has_trip_word = _mentions_trips(msg)
            allow_plain = not has_hours_word and not has_trip_word
            trips = _extract_trip_count(msg, allow_plain=allow_plain)
            hours_val = _extract_hours_count(msg, allow_plain=allow_plain)
            min_hours, min_trips = _min_target_thresholds()
            if trips and not hours_val:
                if trips < min_trips:
                    body = _commitment_blocker_prompt(offered_trips=trips)
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                trips_text = _coerce_goal_value(trips) or str(trips)
                ctx["_goal_trip_count"] = trips
                ctx["_goal_set_at"] = time.time()
                ctx["_goal_acknowledged_repeat"] = False
                ctx["_awaiting_goal_confirm"] = False
                ctx["_pending_goal"] = "online_hours"
                ctx["_pending_goal_trips"] = trips_text
                body = f"Nice — {trips_text} trips it is. How many hours should we target?"
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            if hours_val and not trips:
                if hours_val < min_hours:
                    body = _commitment_blocker_prompt(offered_hours=hours_val)
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                hours_text = _coerce_goal_value(hours_val) or str(hours_val)
                ctx["_goal_online_hours"] = hours_val
                ctx["_goal_set_at"] = time.time()
                ctx["_goal_acknowledged_repeat"] = False
                ctx["_awaiting_goal_confirm"] = False
                ctx["_pending_goal"] = "trip_count"
                ctx["_pending_goal_hours"] = hours_text
                body = f"Great — {hours_text} hours it is. How many trips should we target?"
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    # store personal code if provided explicitly
    if intent == "provide_personal_code":
        pc = extract_personal_code(msg)
        if pc:
            ctx["personal_code"] = pc
            body = "Thanks — I’ll use this personal code for your account lookups."
        else:
            body = "I couldn’t read a personal code. Please send the digits only."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "bot_identity":
        body = "I’m Dineo, your coach from My Next Car. What would you like to focus on today?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    no_trips_hours, no_trips_count = _extract_no_trips_goals(ctx)
    if awaiting_goal_confirm and (no_trips_hours or no_trips_count) and _is_affirmative_message(msg):
        if no_trips_hours and no_trips_count:
            try:
                ctx["_goal_online_hours"] = float(str(no_trips_hours))
            except Exception:
                ctx["_goal_online_hours"] = no_trips_hours
            try:
                ctx["_goal_trip_count"] = float(str(no_trips_count))
            except Exception:
                ctx["_goal_trip_count"] = no_trips_count
            ctx["_goal_set_at"] = time.time()
            ctx["_goal_acknowledged_at"] = time.time()
            ctx["_goal_acknowledged_repeat"] = False
            ctx["_awaiting_goal_confirm"] = False
            body = (
                f"Great — I’ll log a goal of {no_trips_hours} hours and {no_trips_count} trips this week. "
                "If you want to tweak it, just reply with your target."
            )
        elif no_trips_hours:
            ctx["_pending_goal"] = "trip_count"
            ctx["_pending_goal_hours"] = no_trips_hours
            try:
                ctx["_goal_online_hours"] = float(str(no_trips_hours))
            except Exception:
                ctx["_goal_online_hours"] = no_trips_hours
            ctx["_goal_set_at"] = time.time()
            ctx["_goal_acknowledged_at"] = time.time()
            ctx["_goal_acknowledged_repeat"] = False
            ctx["_awaiting_goal_confirm"] = False
            body = (
                f"Great — I’ll log a goal of {no_trips_hours} hours this week. "
                "How many trips should we target?"
            )
        else:
            ctx["_pending_goal"] = "online_hours"
            ctx["_pending_goal_trips"] = no_trips_count
            try:
                ctx["_goal_trip_count"] = float(str(no_trips_count))
            except Exception:
                ctx["_goal_trip_count"] = no_trips_count
            ctx["_goal_set_at"] = time.time()
            ctx["_goal_acknowledged_at"] = time.time()
            ctx["_goal_acknowledged_repeat"] = False
            ctx["_awaiting_goal_confirm"] = False
            body = (
                f"Great — I’ll log a goal of {no_trips_count} trips this week. "
                "How many hours should we target?"
            )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "fuel_estimate":
        distance = _extract_distance_km(msg)
        if distance:
            ctx["_last_fuel_distance_km"] = distance
        else:
            distance = ctx.get("_last_fuel_distance_km")
        if not distance:
            body = "Let me know roughly how many kilometres you plan to drive and I’ll work out the litres and cost at today’s fuel price."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        efficiency = get_model_efficiency(d.get("asset_model", ""))
        litres_needed = distance / max(efficiency, 1.0)
        total_cost = litres_needed * FUEL_PRICE_RANDS
        body = (
            f"For about {distance:.0f} km you’ll need roughly {litres_needed:.1f} ℓ. "
            f"At R{FUEL_PRICE_RANDS:.2f}/ℓ that’s around {fmt_rands(total_cost)}. "
            "If your distance changes, tell me the new km and I’ll recalc."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "earnings_tips":
        ctx.pop("_pending_intent", None)
        ctx["_awaiting_performance_tips"] = False
        ctx["_awaiting_performance_plan"] = True
        ctx["_performance_tips_sent_at"] = time.time()
        ctx["_awaiting_target_update"] = True
        ctx["_awaiting_target_update_at"] = time.time()
        body = _build_earnings_tips_reply()
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "earnings_projection":
        awaiting_trips = ctx.get("_pending_intent") == PENDING_EARNINGS_TRIPS
        awaiting_avg = ctx.get("_pending_intent") == PENDING_EARNINGS_AVG

        trips = _extract_trip_count(msg, allow_plain=awaiting_trips)
        if trips:
            ctx["_last_projection_trips"] = trips
        else:
            trips = ctx.get("_last_projection_trips")

        metrics, reason = get_driver_kpis(wa_id, d)
        avg_from_metrics = None
        if metrics:
            total_trips = metrics.get("trip_count")
            total_gross = metrics.get("gross_earnings")
            if total_trips and total_trips > 0 and total_gross is not None:
                try:
                    avg_from_metrics = float(total_gross) / float(total_trips)
                except Exception:
                    avg_from_metrics = None
        else:
            if reason:
                ctx["_last_kpi_reason"] = reason

        avg_msg = _extract_earnings_per_trip(msg, allow_plain=awaiting_avg)
        if avg_msg:
            ctx["_last_projection_avg"] = avg_msg
        avg_per_trip = avg_msg or ctx.get("_last_projection_avg") or avg_from_metrics

        if not trips:
            ctx["_pending_intent"] = PENDING_EARNINGS_TRIPS
            body = "How many trips are you planning? Tell me the trip count and I’ll project the earnings."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if avg_per_trip is None:
            ctx["_pending_intent"] = PENDING_EARNINGS_AVG
            body = "I can estimate once I know the average earnings per trip—share the rand amount you usually make per ride."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx.pop("_pending_intent", None)
        total_est = trips * avg_per_trip
        source_note = ""
        if avg_msg:
            source_note = " using the per-trip amount you shared"
        elif avg_from_metrics is not None:
            source_note = " using your latest reported average"

        body = (
            f"{trips} trips at about {fmt_rands(avg_per_trip)} each comes to roughly {fmt_rands(total_est)}{source_note}. "
            "Want help planning the shifts to hit that number?"
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "raise_concern":
        ctx["_active_concern"] = {"type": "general", "opened_at": time.time(), "message": msg}
        ctx.pop("_pending_intent", None)
        body = (
            "I’m sorry something’s not working smoothly. Share the details or what changed and I’ll help you decide the next step."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "cost_concern":
        ctx["_active_concern"] = {"type": "cost", "opened_at": time.time(), "message": msg}
        ctx.pop("_pending_intent", None)
        tips = [
            "stick to high-demand zones to avoid long dead kilometres",
            "log odometer photos at handover so we can dispute surprises",
            "map your shift before leaving the depot to cut the backtracking",
        ]
        tip = random.choice(tips)
        body = (
            "Excess KM charges can sting. Let’s " + tip + "; if you’d like, I can help you draft a note for support."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "schedule_update":
        ctx.pop("_pending_intent", None)
        ctx["_intraday_updates_paused"] = True
        ctx["_intraday_updates_paused_at"] = time.time()
        ctx["_intraday_updates_enabled"] = False
        schedule_hint = _extract_schedule_hint(msg)
        body = (
            f"Thanks for the update — I’ll check back {schedule_hint}. "
            "When you’re back online, reply 'daily update' and I’ll send a live check-in."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "low_demand":
        ctx["_active_concern"] = {"type": "low_demand", "opened_at": time.time(), "message": msg}
        ctx.pop("_pending_intent", None)
        ctx["_awaiting_target_update"] = False
        ctx.pop("_pending_goal", None)
        ctx.pop("_awaiting_goal_confirm", None)
        msg_text = msg or ""
        msg_lower = _normalize_text(msg_text).lower()
        explicit_ticket = _is_ticket_request(msg_text)
        everywhere_issue = _is_everywhere_no_trips(msg_text)
        everywhere_signal = bool(re.search(r"\b(everywhere|anywhere|wherever)\b", msg_lower) or re.search(r"\ball\s+(over|around)\b", msg_lower))

        ticket_ctx = ctx.get("_low_demand_ticket") if isinstance(ctx.get("_low_demand_ticket"), dict) else None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
        ticket_open = bool(ticket_id) and ticket_ctx.get("status") in {"collecting", "pending_ops"}

        area_hint = None
        if location and (location.get("latitude") is not None or location.get("longitude") is not None):
            area_hint = location.get("name") or location.get("address")
        area_candidates = _extract_area_candidates(msg_text)
        if _is_low_demand_issue(msg_text):
            area_candidates = _extract_low_demand_area_candidates(msg_text)
        if area_candidates:
            area_candidates = list(dict.fromkeys(area_candidates))
        if not area_hint and area_candidates:
            area_hint = ", ".join(area_candidates)
        oph_sentence = _build_low_demand_oph_sentence()
        area_names: List[str] = []
        if area_candidates:
            area_names = area_candidates
        elif area_hint:
            area_names = _extract_area_candidates(area_hint)
            if not area_names:
                mapped = _lookup_suburb_from_address(area_hint)
                if mapped:
                    area_names = [mapped]
        if area_names:
            contact_ids = (d.get("xero_contact_ids") or [])[:5]
            scope = HOTSPOT_SCOPE_DEFAULT if contact_ids else "global"
            scope_contact_ids = contact_ids if scope != "global" else []
            area_oph, area_oph_reason, area_oph_label = get_oph_for_areas(
                contact_ids=scope_contact_ids,
                scope=scope,
                timeframe=HOTSPOT_TIMEFRAME_DEFAULT,
                areas=area_names,
            )
            area_sentence = _format_area_oph_sentence(area_oph, area_oph_reason, area_oph_label, area_names)
            if area_sentence:
                if any((entry.get("oph") or 0.0) < 7.0 for entry in (area_oph or [])):
                    oph_sentence = f"{area_sentence} {oph_sentence}"
                else:
                    oph_sentence = area_sentence
        logged_areas = ctx.get("_low_demand_areas")
        if not isinstance(logged_areas, list):
            logged_areas = []
        logged_areas = [area for area in logged_areas if area and not _is_low_demand_issue(area)]

        pending_kind = ctx.get("_low_demand_pending_kind")
        wants_ticket = False
        if ticket_open:
            wants_ticket = True
        elif explicit_ticket or everywhere_issue or (ctx.get("_low_demand_pending") and everywhere_signal):
            wants_ticket = True
        elif ctx.get("_low_demand_pending") and pending_kind == "choice":
            if _is_positive_confirmation(msg_text) or explicit_ticket or everywhere_signal:
                wants_ticket = True
            elif _is_negative_confirmation(msg_text) or re.search(r"\btips?\b", msg_lower) or area_hint or _is_hotspot_query(msg_text):
                ctx["_low_demand_pending"] = False
                ctx.pop("_low_demand_pending_at", None)
                ctx["_low_demand_pending_kind"] = "tips"
                ctx["_low_demand_tips_only"] = True
            else:
                body = (
                    "Reply 'ticket' if it's everywhere and you want me to log it, "
                    "or tell me your suburb/mall for tips."
                )
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if wants_ticket:
            ctx.pop("_low_demand_tips_only", None)
            if not ticket_open:
                ctx["_low_demand_pending"] = False
                ctx.pop("_low_demand_pending_at", None)
                ticket_id = create_driver_issue_ticket(wa_id, msg_text, d, issue_type="low_demand")
                ticket_ctx = {
                    "ticket_id": ticket_id,
                    "status": "collecting" if ticket_id else "collecting",
                    "opened_at": time.time(),
                }
                ctx["_low_demand_ticket"] = ticket_ctx
            ctx["_low_demand_pending_kind"] = "ticket"

            if area_hint and ticket_id:
                lat = location.get("latitude") if location else None
                lng = location.get("longitude") if location else None
                raw_location = location if location else {"text": area_hint, "source": "text"}
                update_driver_issue_location(
                    ticket_id,
                    latitude=lat,
                    longitude=lng,
                    description=area_hint,
                    raw=raw_location,
                )
                update_driver_issue_metadata(
                    ticket_id,
                    {"operating_area": area_hint, "details_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                )
                update_driver_issue_status(ticket_id, "pending_ops")
                ctx["_low_demand_pending"] = False
                ctx.pop("_low_demand_pending_at", None)
                ctx["_low_demand_pending_kind"] = None
                for area in area_candidates or [area_hint]:
                    if area and area not in logged_areas:
                        logged_areas.append(area)
                ctx["_low_demand_areas"] = logged_areas
                if logged_areas and ticket_id:
                    update_driver_issue_metadata(ticket_id, {"operating_areas": logged_areas})
                area_label = " and ".join(logged_areas[-2:]) if logged_areas else area_hint
                body = f"Thanks — I've logged {area_label}. {oph_sentence} Want me to check back later or add another area?"
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

            if ctx.get("_low_demand_pending"):
                detail_text = (msg_text or "").strip()
                if _is_low_demand_issue(detail_text) and not area_candidates:
                    body = "Thanks. Which suburb or mall/landmark are you operating in right now (or planning to work)?"
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                if (
                    detail_text
                    and detail_text.lower() not in ACKNOWLEDGEMENT_TOKENS
                    and not _is_affirmative_message(detail_text)
                    and not _is_low_demand_issue(detail_text)
                    and not _is_ticket_request(detail_text)
                    and not everywhere_signal
                ):
                    if ticket_id:
                        update_driver_issue_metadata(
                            ticket_id,
                            {"operating_area": detail_text, "details_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                        )
                        update_driver_issue_status(ticket_id, "pending_ops")
                    ctx["_low_demand_pending"] = False
                    ctx.pop("_low_demand_pending_at", None)
                    ctx["_low_demand_pending_kind"] = None
                    if detail_text not in logged_areas:
                        logged_areas.append(detail_text)
                    ctx["_low_demand_areas"] = logged_areas
                    if logged_areas and ticket_id:
                        update_driver_issue_metadata(ticket_id, {"operating_areas": logged_areas})
                    body = f"Thanks — I've logged that. {oph_sentence} Want me to check back later or add another area?"
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                body = "Thanks. Which suburb or mall/landmark are you operating in right now (or planning to work)?"
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

            ctx["_low_demand_pending"] = True
            ctx["_low_demand_pending_kind"] = "ticket"
            ctx["_low_demand_pending_at"] = time.time()
            body = (
                "Sorry you're not getting trips. I've opened a ticket to flag low demand. "
                "Which suburb or mall/landmark are you operating in right now (or planning to work)? You can share more than one. "
                f"{oph_sentence}"
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_low_demand_tips_only"] = True
        if area_hint:
            for area in area_candidates or [area_hint]:
                if area and area not in logged_areas and not _is_low_demand_issue(area):
                    logged_areas.append(area)
            ctx["_low_demand_areas"] = logged_areas
            ctx["_low_demand_pending"] = False
            ctx.pop("_low_demand_pending_at", None)
            ctx["_low_demand_pending_kind"] = None
            area_label = " and ".join(logged_areas[-2:]) if logged_areas else area_hint
            body = (
                f"Thanks — got {area_label}. {oph_sentence} Want me to check back later or add another area? "
                "If this is happening everywhere and you want me to log a ticket, just say 'ticket'."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        tips_mode = bool(ctx.get("_low_demand_tips_only") or pending_kind == "tips")
        if ctx.get("_low_demand_pending") and ctx.get("_low_demand_pending_kind") == "tips":
            body = "Which suburb or mall/landmark are you in right now (or planning to work)? If it's everywhere, reply 'ticket'."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_low_demand_pending"] = True
        ctx["_low_demand_pending_kind"] = "tips" if tips_mode else "choice"
        ctx["_low_demand_pending_at"] = time.time()
        body = (
            "Sorry you're not getting trips. "
            + (
                "Which suburb or mall/landmark are you in right now (or planning to work)? "
                "If it's everywhere and you want me to log a ticket for ops, reply 'ticket'."
                if tips_mode
                else
                "Is this happening everywhere, or just in one area? "
                "If it's everywhere and you want me to log a ticket for ops, reply 'ticket'. "
                "If it's one area, tell me the suburb or mall and I'll suggest busy spots."
            )
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "account_suspension":
        ctx["_active_concern"] = {"type": "account_suspension", "opened_at": time.time(), "message": msg}
        ctx.pop("_pending_intent", None)
        ctx["_awaiting_target_update"] = False
        ctx.pop("_pending_goal", None)
        ctx.pop("_awaiting_goal_confirm", None)
        ticket_ctx = ctx.get("_account_suspension_ticket") if isinstance(ctx.get("_account_suspension_ticket"), dict) else None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
        if not ticket_id or ticket_ctx.get("status") not in {"collecting", "pending_ops"}:
            ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="account_suspension")
            ctx["_account_suspension_ticket"] = {
                "ticket_id": ticket_id,
                "status": "collecting" if ticket_id else "collecting",
                "opened_at": time.time(),
            }
        if media and message_type in {"image", "document"} and ticket_id:
            if append_driver_issue_media(ticket_id, media):
                update_driver_issue_metadata(
                    ticket_id,
                    {"suspension_media_received": True, "suspension_media_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                )
        if ctx.get("_account_suspension_pending"):
            detail_text = (msg or "").strip()
            if detail_text and detail_text.lower() not in ACKNOWLEDGEMENT_TOKENS and not _is_positive_confirmation(msg):
                if ticket_id:
                    update_driver_issue_metadata(
                        ticket_id,
                        {"suspension_details": detail_text, "details_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                    )
                    update_driver_issue_status(ticket_id, "pending_ops")
                ctx["_account_suspension_pending"] = False
                body = (
                    "Thanks — I’ve logged that and asked Ops to check the suspension reason. "
                    "I’ll update you as soon as I hear back."
                )
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            body = "Thanks — can you share the exact message you saw in the app (or a screenshot) so we can find the cause quickly?"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_account_suspension_pending"] = True
        body = (
            "Sorry to hear your Bolt account is suspended. I’ve opened a ticket to check the cause. "
            "What message did you see in the app (or do you have a screenshot), and when did it start?"
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "app_issue":
        ctx["_active_concern"] = {"type": "app_issue", "opened_at": time.time(), "message": msg}
        ctx.pop("_pending_intent", None)
        ctx["_awaiting_target_update"] = False
        ctx.pop("_pending_goal", None)
        ctx.pop("_awaiting_goal_confirm", None)
        ticket_ctx = ctx.get("_app_issue_ticket") if isinstance(ctx.get("_app_issue_ticket"), dict) else None
        if ticket_ctx and _ticket_ctx_is_closed(ticket_ctx, ctx):
            ctx.pop("_app_issue_ticket", None)
            ticket_ctx = None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
        if not ticket_id:
            existing = fetch_open_driver_issue_ticket(wa_id, ["app_issue"])
            if existing:
                ticket_id = existing.get("id")
            if ticket_id:
                ctx["_app_issue_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": (ticket_ctx or {}).get("status") or "collecting",
                    "opened_at": time.time(),
                }
            else:
                ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="app_issue")
                ctx["_app_issue_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": "collecting" if ticket_id else "collecting",
                    "opened_at": time.time(),
                }

        if media and message_type in {"image", "document"} and ticket_id:
            if append_driver_issue_media(ticket_id, media):
                update_driver_issue_metadata(
                    ticket_id,
                    {
                        "app_issue_media_received": True,
                        "app_issue_media_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                )

        if ctx.get("_app_issue_pending"):
            detail_text = (msg or "").strip()
            if detail_text and detail_text.lower() not in ACKNOWLEDGEMENT_TOKENS and not _is_positive_confirmation(msg):
                if ticket_id:
                    update_driver_issue_metadata(
                        ticket_id,
                        {"app_issue_details": detail_text, "details_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                    )
                    update_driver_issue_status(ticket_id, "pending_ops")
                ctx["_app_issue_pending"] = False
                body = "Thanks — I’ve logged that for support and asked Ops to check. I’ll update you as soon as I hear back."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            body = "Thanks — can you share the exact error message or a screenshot so we can diagnose quickly?"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_app_issue_pending"] = True
        body = (
            "Sorry you're having app trouble. Are you seeing an error or login issue, or does it say your account is blocked/suspended? "
            "Please share the exact message (or a screenshot) and I’ll log it for support."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "medical_issue":
        ctx["_active_concern"] = {"type": "medical", "opened_at": time.time(), "message": msg}
        ctx.pop("_pending_intent", None)
        ctx["_awaiting_target_update"] = False
        ctx.pop("_pending_goal", None)
        ctx.pop("_awaiting_goal_confirm", None)
        ticket_ctx = ctx.get("_medical_ticket") if isinstance(ctx.get("_medical_ticket"), dict) else None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
        if not ticket_id:
            ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="medical_pause")
            ctx["_medical_ticket"] = {
                "ticket_id": ticket_id,
                "status": "collecting" if ticket_id else "collecting",
                "opened_at": time.time(),
            }

        cert_received = False
        if media and message_type in MEDICAL_CERT_MEDIA_TYPES and ticket_id:
            if append_driver_issue_media(ticket_id, media):
                cert_received = True
                ctx["_medical_certificate_received"] = True
                ctx["_medical_pending_certificate"] = False
                update_driver_issue_metadata(
                    ticket_id,
                    {
                        "medical_certificate_received": True,
                        "medical_certificate_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                        "medical_certificate_media_type": message_type,
                    },
                )

        location_captured = False
        if ctx.get("_medical_pending_location"):
            lowered = _normalize_text(msg or "").lower()
            loc_text = None
            if location and (location.get("latitude") is not None or location.get("longitude") is not None):
                loc_text = location.get("name") or location.get("address")
            elif _looks_like_address(msg) or re.search(r"\b(home|house|my place)\b", lowered):
                loc_text = msg.strip()
            if ticket_id and loc_text:
                update_driver_issue_location(
                    ticket_id,
                    latitude=location.get("latitude") if location else None,
                    longitude=location.get("longitude") if location else None,
                    description=loc_text,
                    raw=location or {"text": loc_text, "source": "text"},
                )
                ctx["_medical_pending_location"] = False
                location_captured = True

        if ctx.get("_medical_pending_commitment"):
            lowered = _normalize_text(msg or "").lower()
            trips_val = _extract_trip_count(msg, allow_plain=True)
            hours_val = _extract_hours_count(msg, allow_plain=True)
            has_hours_word = _mentions_hours(lowered)
            has_trip_word = _mentions_trips(lowered)
            if trips_val and not has_trip_word and has_hours_word:
                trips_val = None
            if hours_val and not has_hours_word and has_trip_word:
                hours_val = None
            if trips_val and not has_trip_word and not has_hours_word:
                ctx["_medical_commitment_pending_unit"] = trips_val
                body = f"Did you mean {int(trips_val)} trips or {int(trips_val)} hours?"
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            pending_unit = ctx.pop("_medical_commitment_pending_unit", None)
            if pending_unit and (has_trip_word or has_hours_word):
                if has_trip_word:
                    trips_val = pending_unit
                elif has_hours_word:
                    hours_val = pending_unit
            online_flag = None
            schedule_hint = _extract_schedule_hint(msg) if _is_schedule_update(msg) else None
            if _is_negative_confirmation(msg):
                online_flag = False
            elif _is_positive_confirmation(msg) or schedule_hint or "online" in lowered or "back" in lowered:
                online_flag = True
            metadata_patch = {}
            if online_flag is not None:
                metadata_patch["online_this_week"] = online_flag
            if schedule_hint:
                metadata_patch["online_schedule_hint"] = schedule_hint
            if trips_val:
                metadata_patch["trip_commitment"] = float(trips_val)
                ctx["_goal_trip_count"] = float(trips_val)
                ctx["_goal_set_at"] = time.time()
            if hours_val:
                metadata_patch["hours_commitment"] = float(hours_val)
                ctx["_goal_online_hours"] = float(hours_val)
                ctx["_goal_set_at"] = time.time()
            if ticket_id and metadata_patch:
                update_driver_issue_metadata(ticket_id, metadata_patch)
            if online_flag is False:
                ctx["_medical_pending_commitment"] = False
                parts = ["Thanks for the update. Focus on recovery — just tell me when you're ready to go online and I’ll help you set a plan."]
                if ctx.get("_medical_pending_location"):
                    parts.append("What suburb or address should I share with ops so they can support you?")
                if ctx.get("_medical_pending_certificate"):
                    parts.append("Please send a clear photo of your medical certificate so we can verify it and support your request.")
                body = " ".join(parts)
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            if trips_val or hours_val:
                ctx["_medical_pending_commitment"] = False
                if trips_val and hours_val:
                    body = f"Got it — I’ll note {int(trips_val)} trips and {int(round(hours_val))} hours for this week."
                elif trips_val:
                    body = f"Got it — I’ll note {int(trips_val)} trips for this week."
                else:
                    body = f"Got it — I’ll note {int(round(hours_val))} hours for this week."
                followups = []
                if ctx.get("_medical_pending_location"):
                    followups.append("What suburb or address should I share with ops so they can support you?")
                if ctx.get("_medical_pending_certificate"):
                    followups.append("Please send a clear photo of your medical certificate so we can verify it and support your request.")
                if followups:
                    body = f"{body} {' '.join(followups)}"
                else:
                    body = f"{body} If plans change, just tell me."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            if online_flag:
                prefix = "Thanks —" if not location_captured else "Thanks, I’ve logged your location —"
                body = f"{prefix} about how many trips can you commit to this week?"
                if ctx.get("_medical_pending_certificate"):
                    body = f"{body} Please send a clear photo of your medical certificate so we can verify it and support your request."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            prefix = "Thanks, I’ve logged your location. " if location_captured else ""
            body = f"{prefix}Will you be online this week? If yes, about how many trips can you commit to?"
            if ctx.get("_medical_pending_certificate"):
                body = f"{body} Please send a clear photo of your medical certificate so we can verify the pause."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if ctx.get("_medical_pending_decision"):
            lowered = _normalize_text(msg or "").lower()
            decision = None
            if any(kw in lowered for kw in ["continue", "keep", "stay", "yes", "carry on", "carryon"]):
                decision = "continue"
            if any(kw in lowered for kw in ["hand back", "handover", "return", "give back", "quit", "stop"]):
                decision = "handover"
            if decision:
                if ticket_id:
                    update_driver_issue_metadata(
                        ticket_id,
                        {"return_intent": decision, "decision_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                    )
                ctx["_medical_pending_decision"] = False
                ctx["_medical_decision"] = decision
                ctx["_medical_pending_location"] = True
                ctx["_medical_pending_certificate"] = not ctx.get("_medical_certificate_received")
                if decision == "handover":
                    body = (
                        "Thanks for letting me know. When you're ready, please return the vehicle to the office so ops can assist. "
                        "What suburb or address should I share with ops? "
                    )
                    if ctx.get("_medical_pending_certificate"):
                        body = f"{body}Please send a clear photo of your medical certificate so we can verify it and support your request."
                else:
                    ctx["_medical_pending_commitment"] = True
                    body = (
                        "Thanks for confirming. Will you be online this week? If yes, about how many trips can you commit to? "
                        "Also, what suburb or address should I share with ops so they can support you?"
                    )
                    if ctx.get("_medical_pending_certificate"):
                        body = f"{body} Please send a clear photo of your medical certificate too."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if ctx.get("_medical_pending_location") and not ctx.get("_medical_pending_commitment"):
            lowered = _normalize_text(msg or "").lower()
            loc_text = None
            if location and (location.get("latitude") is not None or location.get("longitude") is not None):
                loc_text = location.get("name") or location.get("address")
            elif _looks_like_address(msg) or re.search(r"\b(home|house|my place)\b", lowered):
                loc_text = msg.strip()
            if ticket_id and loc_text:
                update_driver_issue_location(
                    ticket_id,
                    latitude=location.get("latitude") if location else None,
                    longitude=location.get("longitude") if location else None,
                    description=loc_text,
                    raw=location or {"text": loc_text, "source": "text"},
                )
                ctx["_medical_pending_location"] = False
                if not ctx.get("_medical_certificate_received"):
                    ctx["_medical_pending_certificate"] = True
                    body = (
                        "Thanks, I've logged your location. Please send a clear photo of your medical certificate so we can verify it and support your request."
                    )
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                if ctx.get("_medical_decision") == "continue":
                    ctx["_medical_pending_commitment"] = True
                    body = "Thanks, I've logged your location. Will you be online this week? If yes, about how many trips can you commit to?"
                    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
                body = "Thanks, I've logged your location and alerted ops. Please head to the office when it's safe."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            body = "Thanks. What suburb or address should I share with ops so they can support you?"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if ctx.get("_medical_pending_certificate") and not ctx.get("_medical_pending_commitment"):
            if cert_received:
                if ctx.get("_medical_decision") == "continue":
                    ctx["_medical_pending_commitment"] = True
                    body = "Thanks for the certificate. Will you be online this week? If yes, about how many trips can you commit to?"
                else:
                    body = "Thanks for the certificate — I’ve attached it to your ticket and will update ops."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            body = "Please send a clear photo of your medical certificate so we can verify it and support your request."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_medical_pending_decision"] = True
        body = (
            "Sorry you're not feeling well. When you can, please return the vehicle to the office so ops can assist. "
            "Are you planning to continue operating with My Next Car once you're better, or are you planning on handing the car back?"
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "no_vehicle":
        ctx["_active_concern"] = {"type": "no_vehicle", "opened_at": time.time(), "message": msg}
        ctx["_awaiting_target_update"] = False
        ctx.pop("_pending_goal", None)
        ctx.pop("_awaiting_goal_confirm", None)
        _schedule_no_vehicle_checkin(ctx, reason="no_vehicle")
        pending_followup = ctx.get("_no_vehicle_pending")
        alt_reason = _parse_no_vehicle_reason(msg) if pending_followup else None
        if pending_followup == "finance_contact" and alt_reason and alt_reason != "balance":
            ctx["_no_vehicle_pending"] = None
            pending_followup = None
        if pending_followup == "workshop_details" and alt_reason and alt_reason != "workshop":
            ctx["_no_vehicle_pending"] = None
            pending_followup = None
        if pending_followup == "finance_details":
            ticket_ctx = ctx.get("_no_vehicle_finance_ticket") if isinstance(ctx.get("_no_vehicle_finance_ticket"), dict) else None
            ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
            detail_text = (msg or "").strip()
            if ticket_id and detail_text:
                update_driver_issue_metadata(
                    ticket_id,
                    {"finance_contact_preference": detail_text, "details_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                )
            ctx["_no_vehicle_pending"] = None
            body = "Thanks - I'll pass that to Finance and follow up if they need anything else."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if pending_followup == "workshop_details":
            ticket_ctx = ctx.get("_no_vehicle_ticket") if isinstance(ctx.get("_no_vehicle_ticket"), dict) else None
            ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
            if ticket_id:
                detail_text = msg.strip() if msg else ""
                if detail_text:
                    update_driver_issue_metadata(
                        ticket_id,
                        {"workshop_details": detail_text, "details_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S")},
                    )
                if location and (location.get("latitude") is not None or location.get("longitude") is not None):
                    update_driver_issue_location(
                        ticket_id,
                        latitude=location.get("latitude"),
                        longitude=location.get("longitude"),
                        description=location.get("name") or location.get("address"),
                        raw=location,
                    )
            ctx["_no_vehicle_pending"] = None
            body = "Thanks - I've added those details for ops and will follow up with you."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        reason = alt_reason or _parse_no_vehicle_reason(msg)
        if reason:
            _schedule_no_vehicle_checkin(ctx, reason=reason)
        if not reason:
            ctx["_pending_intent"] = PENDING_NO_VEHICLE_REASON
            ctx["_no_vehicle_prompt_at"] = time.time()
            body = (
                "Sorry you don't have a car right now. Is it in the workshop/maintenance, waiting for a replacement, "
                "held due to an outstanding balance, or something else? Share a short reason and I'll guide you."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx.pop("_pending_intent", None)
        if reason == "medical":
            body = (
                "Sorry you're not feeling well. Please return the vehicle to the office as soon as you can so ops can assist—"
                "if you need help arranging it, tell me where you are."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if reason == "balance":
            ticket_ctx = ctx.get("_no_vehicle_finance_ticket") if isinstance(ctx.get("_no_vehicle_finance_ticket"), dict) else None
            ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
            if not ticket_id:
                ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="finance_followup")
                ctx["_no_vehicle_finance_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": "collecting" if ticket_id else "collecting",
                    "opened_at": time.time(),
                }
            body = (
                "Thanks for letting me know. If the car is held due to an outstanding balance, the quickest way back is to arrange with "
                "Finance to reduce it. I've asked Finance to contact you within 24 hours to set up a payment plan. "
                "What's the best time or number to reach you?"
            )
            ctx["_no_vehicle_pending"] = "finance_details"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if reason == "workshop":
            ticket_ctx = ctx.get("_no_vehicle_ticket") if isinstance(ctx.get("_no_vehicle_ticket"), dict) else None
            ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
            if not ticket_id or ticket_ctx.get("status") not in {"collecting", "pending_ops"}:
                ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="workshop_followup")
                ticket_ctx = {
                    "ticket_id": ticket_id,
                    "status": "pending_ops" if ticket_id else "collecting",
                    "opened_at": time.time(),
                }
                ctx["_no_vehicle_ticket"] = ticket_ctx
            body = (
                "Thanks for letting me know. I've logged a workshop follow-up for ops. "
                "If you have the ticket number, car location, or expected completion date, send it and I'll add it."
            )
            ctx["_no_vehicle_pending"] = "workshop_details"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if reason == "assignment":
            body = (
                "Got it. I can flag ops to arrange a replacement or assignment—what branch are you closest to and when can you collect?"
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if reason == "blocked":
            body = (
                "Thanks for explaining. If it's an account hold or block, share any details and I'll raise it with management; "
                "I can't guarantee the outcome, but I'll push for an update."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if reason == "other":
            ticket_ctx = ctx.get("_no_vehicle_other_ticket") if isinstance(ctx.get("_no_vehicle_other_ticket"), dict) else None
            ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
            if not ticket_id:
                ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="no_vehicle_other")
                ctx["_no_vehicle_other_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": "collecting" if ticket_id else "collecting",
                    "opened_at": time.time(),
                }
            body = (
                "Thanks for clarifying. What exactly happened and where is the car now? "
                "Share a short note and I'll update ops."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        body = "Thanks for the update. What happened, and do you need a replacement vehicle or just time off?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "vehicle_back":
        ctx.pop("_pending_intent", None)
        ctx.pop("_no_vehicle_pending", None)
        ctx.pop("_no_vehicle_prompt_at", None)
        ctx.pop("_no_vehicle_checkin_due_at", None)
        ctx.pop("_no_vehicle_checkin_reason", None)
        ctx.pop("_no_vehicle_checkin_status", None)
        ctx.pop("_no_vehicle_checkin_message_id", None)
        ctx.pop("_no_vehicle_checkin_sent_at", None)
        ctx["_no_vehicle_checkin_pending"] = False
        ctx["_engagement_followup_paused"] = False
        ctx.pop("_engagement_followup_paused_at", None)
        ctx.pop("_engagement_followup_pause_reason", None)
        active_concern = ctx.get("_active_concern")
        if isinstance(active_concern, dict) and active_concern.get("type") == "no_vehicle":
            ctx.pop("_active_concern", None)
        body = (
            "Glad you're back with a car. Are you going online this week? "
            "If yes, about how many trips can you commit to?"
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "target_benefit":
        if ctx.get("_pending_intent") == PENDING_REPOSSESSION_REASON:
            ctx.pop("_pending_intent", None)
        ctx["_awaiting_target_update"] = True
        no_trips_hours, no_trips_count = _extract_no_trips_goals(ctx)
        targets = _recent_goal_targets(ctx)
        hours_target = _coerce_goal_value(targets.get("online_hours")) if targets else None
        trips_target = _coerce_goal_value(targets.get("trip_count")) if targets else None
        hours = hours_target or no_trips_hours or "55"
        trips = trips_target or no_trips_count or "110"
        body = (
            f"{hours} hours + {trips} trips is the benchmark that usually lifts weekly earnings and acceptance. "
            "It keeps you online long enough to catch peak demand and build steady trip volume. "
            "If that feels too high, tell me your realistic target and I will adjust it. "
            "What can you commit to this week? Reply with hours or trips."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "target_update":
        if ctx.get("_pending_intent") == PENDING_REPOSSESSION_REASON:
            ctx.pop("_pending_intent", None)
        lowered = (msg or "").lower()
        has_hours_word = _mentions_hours(lowered)
        has_trip_word = _mentions_trips(lowered)
        allow_plain = not has_hours_word and not has_trip_word
        if _needs_target_clarification(msg):
            number = _first_number(msg) or "that"
            body = f"Did you mean {number} hours or {number} trips?"
            ctx["_awaiting_target_update"] = True
            ctx["_awaiting_target_update_at"] = time.time()
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        trips = _extract_trip_count(msg, allow_plain=allow_plain)
        hours_val = _extract_hours_count(msg, allow_plain=allow_plain)
        if has_hours_word and not has_trip_word:
            trips = None
        if has_trip_word and not has_hours_word:
            hours_val = None
        if not trips and not hours_val:
            body = "Share the hours or trips you want and I’ll update the target."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        min_hours, min_trips = _min_target_thresholds()
        if hours_val and hours_val < min_hours:
            body = _commitment_blocker_prompt(offered_hours=hours_val)
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if trips and trips < min_trips:
            body = _commitment_blocker_prompt(offered_trips=trips)
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_awaiting_target_update"] = False
        ctx["_goal_set_at"] = time.time()
        ctx["_goal_acknowledged_at"] = time.time()
        ctx["_goal_acknowledged_repeat"] = False
        ctx["_awaiting_goal_confirm"] = False

        if trips:
            ctx["_goal_trip_count"] = trips
        if hours_val:
            ctx["_goal_online_hours"] = hours_val

        trips_text = _coerce_goal_value(trips) if trips else None
        hours_text = _coerce_goal_value(hours_val) if hours_val else None

        if trips and hours_val:
            body = f"Locked in — I’ll log {hours_text} hours and {trips_text} trips this week."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if trips:
            ctx["_pending_goal"] = "online_hours"
            ctx["_pending_goal_trips"] = trips_text
            body = f"Got it — {trips_text} trips. How many hours should we aim for?"
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        ctx["_pending_goal"] = "trip_count"
        ctx["_pending_goal_hours"] = hours_text
        body = f"Got it — {hours_text} hours. How many trips should we aim for?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "vehicle_repossession":
        ctx["_active_concern"] = {"type": "repossession", "opened_at": time.time(), "message": msg}
        reason = _parse_repossession_reason(msg)
        _schedule_no_vehicle_checkin(ctx, reason="repossession")
        if not reason:
            ctx["_pending_intent"] = PENDING_REPOSSESSION_REASON
            ctx["_repossession_prompted_at"] = time.time()
            body = (
                "I am sorry to hear the vehicle was repossessed. I will raise this with management to see if it can be resolved. "
                "Do you know if it was due to a high outstanding balance or a behaviour issue that was flagged? "
                "Reply with 'balance' or 'behaviour' so I can guide you."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx.pop("_pending_intent", None)
        ctx["_last_repossession_reason"] = reason
        parts: List[str] = ["Thanks for clarifying."]
        if reason in {"outstanding", "both"}:
            parts.append(
                "If the repossession is linked to an outstanding balance, the quickest path is to reduce the balance as much as possible."
            )
        if reason in {"behavior", "both"}:
            parts.append(
                "If it was flagged for behaviour or a block, please share any details or context you want me to pass on."
            )
            parts.append(
                "I cannot guarantee the issue will be resolved, but I will inform management to see what can be done to help."
            )
        else:
            parts.append("I will discuss this with management to see if it can be resolved.")
        body = " ".join(parts)
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == BRANDING_BONUS_INTENT:
        ctx.pop("_pending_intent", None)
        bonus_ctx = ctx.get("_branding_bonus") if isinstance(ctx.get("_branding_bonus"), dict) else None
        if not bonus_ctx:
            bonus_ctx = {"opened_at": time.time(), "messages": []}
        bonus_ctx.setdefault("messages", [])
        if msg:
            bonus_ctx["messages"].append(msg)

        first_message = bonus_ctx["messages"][0] if bonus_ctx["messages"] else msg
        ctx["_active_concern"] = {
            "type": "branding_bonus",
            "opened_at": bonus_ctx.get("opened_at", time.time()),
            "message": first_message,
        }

        awaiting = bonus_ctx.get("awaiting")
        week_info = bonus_ctx.get("week_info")
        trip_count = bonus_ctx.get("trip_count")

        if awaiting == "week":
            week_candidate = _extract_week_reference(msg)
            if week_candidate:
                bonus_ctx["week_info"] = week_candidate
                bonus_ctx["awaiting"] = None
                week_info = week_candidate
            else:
                ctx["_branding_bonus"] = bonus_ctx
                body = (
                    "Thanks for the nudge. Which week’s branding/campaign bonus is missing? "
                    "Something like “week of 1 July” or the payout week number works perfectly."
                )
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if bonus_ctx.get("awaiting") == "trips":
            trip_candidate = _extract_trip_count(msg, allow_plain=True)
            if trip_candidate:
                bonus_ctx["trip_count"] = trip_candidate
                bonus_ctx["awaiting"] = None
                trip_count = trip_candidate
            else:
                ctx["_branding_bonus"] = bonus_ctx
                body = (
                    "Got it. How many completed trips did you log for that week? "
                    "Share the total so I can send it through to ops."
                )
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if not week_info:
            week_candidate = _extract_week_reference(msg)
            if week_candidate:
                bonus_ctx["week_info"] = week_candidate
                week_info = week_candidate

        if not trip_count:
            trip_candidate = _extract_trip_count(msg, allow_plain=True)
            if trip_candidate:
                bonus_ctx["trip_count"] = trip_candidate
                trip_count = trip_candidate

        if not bonus_ctx.get("week_info"):
            bonus_ctx["awaiting"] = "week"
            ctx["_branding_bonus"] = bonus_ctx
            body = (
                "Let me log this for the ops team. Which week did you miss the branding/campaign bonus for?"
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if not bonus_ctx.get("trip_count"):
            bonus_ctx["awaiting"] = "trips"
            ctx["_branding_bonus"] = bonus_ctx
            body = (
                "Thanks. How many completed trips did you record for that week? "
                "The ops team needs the count to double-check the payout."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ticket_id = bonus_ctx.get("ticket_id")
        week_info = bonus_ctx.get("week_info")
        trip_count = bonus_ctx.get("trip_count")

        if ticket_id:
            metadata_patch = {
                "branding_bonus_week": week_info,
                "branding_bonus_trip_count": trip_count,
                "branding_bonus_messages": bonus_ctx.get("messages")[-5:],
            }
            update_driver_issue_metadata(ticket_id, metadata_patch)
            ctx["_branding_bonus"] = bonus_ctx
            body = (
                f"I’ve already logged a ticket about the {week_info} branding bonus with your {int(trip_count)} trips. "
                "Ops will review and get back to you—shout if anything changes in the meantime."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        summary = (
            f"Branding bonus missing for {week_info}; driver reports {int(trip_count)} completed trips."
        )
        initial_note = summary + f" First note: {first_message}" if first_message else summary
        ticket_id = create_driver_issue_ticket(wa_id, initial_note, d, issue_type="branding_bonus")

        if ticket_id:
            bonus_ctx["ticket_id"] = ticket_id
            bonus_ctx["status"] = "pending_ops"
            bonus_ctx["awaiting"] = None
            bonus_ctx["logged_at"] = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
            metadata_patch = {
                "branding_bonus_week": week_info,
                "branding_bonus_trip_count": trip_count,
                "branding_bonus_messages": bonus_ctx.get("messages")[-5:],
            }
            update_driver_issue_metadata(ticket_id, metadata_patch)
            update_driver_issue_status(ticket_id, "pending_ops")
            ctx["_last_branding_bonus_ticket"] = {
                "ticket_id": ticket_id,
                "week_info": week_info,
                "trip_count": trip_count,
                "logged_at": bonus_ctx["logged_at"],
            }
            ctx["_branding_bonus"] = bonus_ctx
            ctx.pop("_active_concern", None)
            body = (
                f"Thanks for confirming—I've logged a ticket for ops to review the {week_info} branding bonus "
                f"with your {int(trip_count)} trips. They'll investigate and follow up directly."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        bonus_ctx["awaiting"] = None
        ctx["_branding_bonus"] = bonus_ctx
        body = (
            "I couldn’t log the branding bonus ticket just now, but I’ve saved your details and will retry shortly. "
            "If you notice anything else about that payout week, let me know."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == CASH_ENABLE_INTENT:
        ticket_ctx = ctx.get("_cash_ticket") if isinstance(ctx.get("_cash_ticket"), dict) else None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None

        if not ticket_ctx or ticket_ctx.get("closed") or not ticket_id:
            summary = "Cash rides enablement requested—awaiting POP."
            ticket_id = create_driver_issue_ticket(wa_id, summary + f" First note: {msg}", d, issue_type="cash_ride")
            ticket_ctx = {
                "ticket_id": ticket_id,
                "status": "collecting" if ticket_id else "collecting",
                "awaiting_pop": True,
                "opened_at": time.time(),
            }
        else:
            ticket_ctx["awaiting_pop"] = True

        ctx["_cash_ticket"] = ticket_ctx
        ctx["_active_concern"] = {"type": "cash_ride", "opened_at": ticket_ctx.get("opened_at", time.time()), "message": msg}

        metadata_patch = {
            "cash_enable_requested": True,
            "cash_enable_requested_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if ticket_id:
            update_driver_issue_metadata(ticket_id, metadata_patch)

        body = (
            "I've noted your request to enable cash rides. Cash was turned off due to a high outstanding balance. "
            "If you’ve already paid, please send your Proof of Payment (POP) and I’ll escalate to our Accounts team immediately and get back to you. "
            "If not, kindly make a payment toward your MNC account and share the POP—once received, we’ll request cash reactivation without delay."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == CASH_BALANCE_UPDATE_INTENT:
        ctx.pop("_pending_intent", None)
        ticket_ctx = ctx.get("_cash_ticket") if isinstance(ctx.get("_cash_ticket"), dict) else None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None

        if not ticket_ctx or ticket_ctx.get("closed") or not ticket_id:
            ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="cash_ride")
            ticket_ctx = {
                "ticket_id": ticket_id,
                "status": "collecting" if ticket_id else "collecting",
                "awaiting_pop": True,
                "opened_at": time.time(),
            }

        ticket_ctx["awaiting_pop"] = ticket_ctx.get("awaiting_pop", True)
        ctx["_cash_ticket"] = ticket_ctx
        ctx["_active_concern"] = {"type": "cash_ride", "opened_at": ticket_ctx.get("opened_at", time.time()), "message": msg}

        metadata_patch: Dict[str, Any] = {"cash_pop_requested": True}
        if ticket_id:
            update_driver_issue_metadata(ticket_id, metadata_patch)

        if media and message_type in CASH_POP_MEDIA_TYPES:
            if ticket_id:
                append_driver_issue_media(ticket_id, media)
                update_driver_issue_metadata(
                    ticket_id,
                    {
                        "cash_pop_received": True,
                        "cash_pop_received_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                        "cash_pop_media_type": message_type,
                    },
                )
                if ticket_ctx.get("status") != "pending_ops":
                    update_driver_issue_status(ticket_id, "pending_ops")
            ticket_ctx["awaiting_pop"] = False
            ticket_ctx["status"] = "pending_ops"
            ticket_ctx["closed"] = True
            ticket_ctx["closed_at"] = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
            ctx["_last_cash_ticket"] = dict(ticket_ctx)
            ctx.pop("_cash_ticket", None)
            ctx.pop("_active_concern", None)
            body = (
                "Thanks for sending the POP—I’ve logged it with Accounts right away and will follow up to get cash rides reactivated."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ticket_ctx["awaiting_pop"] = True
        ctx["_cash_ticket"] = ticket_ctx
        body = (
            "Thank you for settling the balance. Please share a clear photo of the Proof of Payment (POP) so I can log it and request cash reactivation."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == ACCIDENT_REPORT_INTENT:
        ctx.pop("_pending_intent", None)
        raw_case = ctx.get("_accident_case")
        case = dict(raw_case) if isinstance(raw_case, dict) else {}
        ticket_id = case.get("ticket_id")
        status = case.get("status")
        if not ticket_id or status not in {"collecting", "pending_ops"}:
            ticket_id = create_driver_issue_ticket(wa_id, msg, d, issue_type="accident")
            case = {
                "ticket_id": ticket_id,
                "status": "collecting",
                "driver_ok": None,
                "medical_needed": None,
                "location_received": False,
                "location_text": None,
                "photos_received": False,
                "other_vehicle_involved": None,
                "vehicle_details": None,
                "other_driver_details": None,
                "created_at": time.time(),
                "awaiting": "safety",
            }
        else:
            case.setdefault("status", status or "collecting")
            case.setdefault("driver_ok", None)
            case.setdefault("medical_needed", None)
            case.setdefault("location_received", False)
            case.setdefault("location_text", None)
            case.setdefault("photos_received", False)
            case.setdefault("other_vehicle_involved", None)
            case.setdefault("vehicle_details", None)
            case.setdefault("other_driver_details", None)
            case.setdefault("created_at", time.time())
            case.setdefault("awaiting", "safety")
        case["last_message_at"] = time.time()

        ctx["_active_concern"] = {"type": "accident", "opened_at": case.get("created_at", time.time()), "message": msg}
        received_location = False
        received_media = False
        vehicle_details_captured = False
        driver_details_captured = False
        address_captured = False
        metadata_patch: Dict[str, Any] = {}

        if location and (location.get("latitude") is not None or location.get("longitude") is not None):
            if not case.get("location_received"):
                received_location = True
            case["location_received"] = True
            case["last_location"] = location
            label = location.get("name") or location.get("address")
            if label:
                case["location_text"] = label
                metadata_patch["location_label"] = label
            metadata_patch["location_received"] = True
            if ticket_id:
                update_driver_issue_location(
                    ticket_id,
                    latitude=location.get("latitude"),
                    longitude=location.get("longitude"),
                    description=label,
                    raw=location,
                )

        if not case.get("location_received") and _looks_like_address(msg):
            case["location_received"] = True
            case["location_text"] = msg.strip()
            address_captured = True
            metadata_patch["location_text"] = case["location_text"]

        if media and media.get("url") and message_type in ACCIDENT_MEDIA_TYPES:
            if not case.get("photos_received"):
                received_media = True
            case["photos_received"] = True
            case["_last_media_url"] = media.get("url")
            if ticket_id:
                append_driver_issue_media(ticket_id, media)
            metadata_patch["photos_received"] = True

        prev_driver_ok = case.get("driver_ok")
        prev_medical_needed = case.get("medical_needed")
        driver_ok_resp, medical_needed_resp = _interpret_medical_response(msg)
        driver_status_updated = False
        medical_status_updated = False
        if driver_ok_resp is not None and driver_ok_resp != prev_driver_ok:
            case["driver_ok"] = driver_ok_resp
            metadata_patch["driver_ok"] = driver_ok_resp
            driver_status_updated = True
        if medical_needed_resp is not None and medical_needed_resp != prev_medical_needed:
            case["medical_needed"] = medical_needed_resp
            metadata_patch["medical_needed"] = medical_needed_resp
            medical_status_updated = True

        prev_other_vehicle = case.get("other_vehicle_involved")
        other_vehicle_resp = _interpret_other_vehicle_response(msg)
        if other_vehicle_resp is not None and other_vehicle_resp != prev_other_vehicle:
            case["other_vehicle_involved"] = other_vehicle_resp
            metadata_patch["other_vehicle_involved"] = other_vehicle_resp

        if case.get("other_vehicle_involved"):
            if case.get("vehicle_details") is None and _contains_vehicle_details(msg):
                case["vehicle_details"] = msg.strip()
                vehicle_details_captured = True
                metadata_patch["other_vehicle_details_text"] = case["vehicle_details"]
            if case.get("other_driver_details") is None and _contains_contact_details(msg):
                case["other_driver_details"] = msg.strip()
                driver_details_captured = True
                metadata_patch["other_driver_details_text"] = case["other_driver_details"]

        stage = _next_accident_stage(case)
        case["awaiting"] = stage

        last_case_summary: Optional[Dict[str, Any]] = None

        if stage == "wrapup":
            now_ts = time.time()
            case["closed"] = True
            case["closed_at"] = now_ts
            case["awaiting"] = None
            metadata_patch["closed"] = True
            metadata_patch["closed_at"] = jhb_now().strftime("%Y-%m-%d %H:%M:%S")

        if ticket_id and metadata_patch:
            metadata_patch["stage"] = stage
            update_driver_issue_metadata(ticket_id, metadata_patch)
        elif ticket_id:
            update_driver_issue_metadata(ticket_id, {"stage": stage})

        if stage == "wrapup":
            if ticket_id and case.get("status") != "pending_ops":
                update_driver_issue_status(ticket_id, "pending_ops")
            case["status"] = "pending_ops"
            last_case_summary = {
                "ticket_id": ticket_id,
                "closed_at": case.get("closed_at"),
                "status": case.get("status"),
                "metadata": {"driver_ok": case.get("driver_ok"), "medical_needed": case.get("medical_needed")},
            }
        else:
            case["status"] = "collecting"

        if stage == "wrapup":
            ctx.pop("_accident_case", None)
            if last_case_summary:
                ctx["_last_accident_case"] = last_case_summary
            car_ticket_ctx = ctx.get("_car_ticket")
            if isinstance(car_ticket_ctx, dict):
                if not car_ticket_ctx.get("closed"):
                    car_ticket_ctx["closed"] = True
                    car_ticket_ctx["closed_at"] = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
                ctx["_last_car_ticket"] = dict(car_ticket_ctx)
                ctx.pop("_car_ticket", None)
        else:
            ctx["_accident_case"] = case

        ack_sentence: Optional[str] = None
        if medical_status_updated and case.get("medical_needed"):
            ack_sentence = "I’m alerting ops now—please stay where it’s safe."
        elif received_location:
            ack_sentence = "Thanks for the location pin—I’ve logged it for ops."
        elif address_captured:
            ack_sentence = "Thanks for sharing the location—I’ve logged it for ops."
        elif received_media:
            ack_sentence = "Thanks for the photo—I’ve added it to the report."
        elif vehicle_details_captured:
            ack_sentence = "I’ve logged the other vehicle details."
        elif driver_details_captured:
            ack_sentence = "I’ve saved the other driver’s details."

        prompt: str
        if stage == "safety":
            if case.get("_safety_prompted"):
                prompt = "Just checking—do you or anyone else need medical attention so I can get help to you?"
            else:
                prompt = "That sounds scary—are you okay right now, and if anyone needs medical help please call 112 and tell me so I can alert ops. 🚑"
                case["_safety_prompted"] = True
        elif stage == "location":
            if case.get("medical_needed"):
                prompt = "Ops is on alert—could you drop a live location pin or share the address so help can reach you quickly?"
            else:
                prompt = "Could you drop a live location pin or share the address so ops can reach you quickly, and send one clear photo if you’re able?"
        elif stage == "other_vehicle":
            prompt = "Was another vehicle involved so I can log their details?"
        elif stage == "vehicle_details":
            prompt = "Please send the other vehicle’s registration plate and a quick description."
        elif stage == "other_driver_details":
            prompt = "Please share the other driver’s name and phone number if you have them."
        else:
            ack_sentence = ack_sentence or "Thanks for all the details—I’ve logged them for ops."
            extra = " They’ll contact you shortly; if anything changes before they arrive, tell me right away. 🙏"
            prompt = extra.strip()
            ctx.pop("_active_concern", None)

        parts: List[str] = []
        if ack_sentence:
            parts.append(ack_sentence)
        parts.append(prompt)
        body = " ".join(parts)
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "car_problem":
        ctx.pop("_pending_intent", None)
        ticket_ctx = ctx.get("_car_ticket") if isinstance(ctx.get("_car_ticket"), dict) else None
        ticket_status = ticket_ctx.get("status") if ticket_ctx else None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None

        if not ticket_ctx or ticket_status not in {"collecting", "pending_ops"} or not ticket_id:
            ticket_id = create_driver_issue_ticket(wa_id, msg, d)
            ticket_ctx = {
                "ticket_id": ticket_id,
                "status": "collecting" if ticket_id else "collecting",
                "awaiting_media": True,
                "awaiting_location": True,
                "opened_at": time.time(),
            }
            ctx["_car_ticket"] = ticket_ctx
            ctx["_active_concern"] = {"type": "car", "opened_at": time.time(), "message": msg}
            body = (
                "I’ve logged this for the workshop. Please park safely and send one clear photo of the issue with a short note about any warning lights, "
                "then drop a pin or address so the ops team knows where to find the car."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_active_concern"] = {"type": "car", "opened_at": ticket_ctx.get("opened_at", time.time()), "message": msg}

        confirm_ctx = ctx.get("_car_confirm_close") if isinstance(ctx.get("_car_confirm_close"), dict) else None
        confirm_prefix = ""

        if confirm_ctx and confirm_ctx.get("awaiting"):
            if (media and media.get("url")) or (location and (location.get("latitude") is not None or location.get("longitude") is not None)) or message_type != "text":
                ctx.pop("_car_confirm_close", None)
                confirm_prefix = "No problem—I’ll keep the workshop ticket open. "
            elif _is_positive_confirmation(msg):
                if ticket_id:
                    update_driver_issue_status(ticket_id, "driver_confirmed_resolved")
                    update_driver_issue_metadata(
                        ticket_id,
                        {
                            "driver_confirmed_resolved": True,
                            "driver_confirmed_resolved_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                        },
                    )
                ticket_ctx["status"] = "driver_confirmed_resolved"
                ticket_ctx["closed"] = True
                ticket_ctx["closed_at"] = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
                ticket_ctx["awaiting_media"] = False
                ticket_ctx["awaiting_location"] = False
                ctx["_last_car_ticket"] = dict(ticket_ctx)
                ctx.pop("_car_ticket", None)
                ctx.pop("_active_concern", None)
                ctx.pop("_car_confirm_close", None)
                body = "Great—I’ve closed the workshop ticket as resolved. If anything flares up again, just shout."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            elif _is_negative_confirmation(msg):
                ctx.pop("_car_confirm_close", None)
                confirm_prefix = "No problem—I’ll keep the workshop ticket open. "
            else:
                body = "Just give me a quick yes if it’s resolved so I can close the workshop ticket, otherwise share what’s still happening."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        awaiting_media = bool(ticket_ctx.get("awaiting_media", True))
        awaiting_location = bool(ticket_ctx.get("awaiting_location", True))
        received_media = False
        received_location = False
        manual_location_captured = False

        if ticket_id and _is_car_resolution_message(msg):
            confirm_payload = {
                "awaiting": True,
                "ticket_id": ticket_id,
                "asked_at": time.time(),
            }
            ctx["_car_confirm_close"] = confirm_payload
            body = (
                "Glad it’s feeling sorted. Should I close the workshop ticket? Reply yes if it’s resolved, or let me know what still needs attention."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        if media and media.get("url") and message_type in CAR_PROBLEM_MEDIA_TYPES:
            if ticket_id and append_driver_issue_media(ticket_id, media):
                awaiting_media = False
                received_media = True
            ticket_ctx["_last_media_url"] = media.get("url")

        if location and (location.get("latitude") is not None or location.get("longitude") is not None):
            lat = location.get("latitude")
            lng = location.get("longitude")
            desc = location.get("name") or location.get("address")
            if ticket_id:
                update_driver_issue_location(
                    ticket_id,
                    latitude=lat,
                    longitude=lng,
                    description=desc,
                    raw=location,
                )
            awaiting_location = False
            received_location = True
            ticket_ctx["_last_location"] = location
            if desc:
                ticket_ctx["_last_location_text"] = desc

        if awaiting_location and _looks_like_address(msg):
            manual_address = msg.strip()
            if manual_address:
                manual_payload = {"text": manual_address, "source": "text"}
                if ticket_id:
                    update_driver_issue_location(
                        ticket_id,
                        latitude=None,
                        longitude=None,
                        description=manual_address,
                        raw=manual_payload,
                    )
                awaiting_location = False
                received_location = True
                manual_location_captured = True
                ticket_ctx["_last_location"] = manual_payload
                ticket_ctx["_last_location_text"] = manual_address

        ticket_ctx["awaiting_media"] = awaiting_media
        ticket_ctx["awaiting_location"] = awaiting_location
        ctx["_car_ticket"] = ticket_ctx

        if not awaiting_media and not awaiting_location:
            if ticket_ctx.get("status") != "pending_ops" and ticket_id:
                update_driver_issue_status(ticket_id, "pending_ops")
            ticket_ctx["status"] = "pending_ops"
            ticket_ctx["closed"] = True
            ticket_ctx["closed_at"] = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
            ctx["_last_car_ticket"] = dict(ticket_ctx)
            ctx.pop("_car_ticket", None)
            ctx.pop("_active_concern", None)
            body = (
                "Perfect—I've added those details and flagged the ops team. They'll reach out once the workshop is lined up."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ack_bits: List[str] = []
        if received_media:
            ack_bits.append("photos")
        if received_location and not manual_location_captured:
            ack_bits.append("location details")

        if ack_bits:
            ack = f"Thanks, I’ve added the {' and '.join(ack_bits)} to your ticket. "
        else:
            if ticket_ctx.get("status") == "pending_ops":
                ack = "Ops is already on it—I’ll keep adding anything you send. "
            else:
                ack = "Thanks, I’m keeping this open for the workshop. "

        if received_location:
            loc_text = ticket_ctx.get("_last_location_text")
            if manual_location_captured and loc_text:
                ack = f"I’ve logged the address as {loc_text}. Give me a quick yes if that’s right or resend it if we need to tweak it."
            elif loc_text:
                ack += f" Logged the location as {loc_text}. "

        if confirm_prefix:
            ack = confirm_prefix + ack

        if awaiting_media and awaiting_location:
            ask = (
                "Could you send a clear photo of the issue—like the leak, damage, or warning light—and drop a pin or address for where the car is parked?"
            )
        elif awaiting_media:
            ask = "Could you send a clear photo showing the problem (leak, damage, warning light) so the workshop can prep parts?"
        else:
            ask = "Could you drop a pin or share the address where the car is parked so ops can reach you?"

        body = ack + ask
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "acknowledgement":
        body = "Thanks for the update—just shout when you need another hand."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "voice_unavailable":
        ctx["_audio_transcript_status"] = "failed"
        reason_note = (ctx.get("_last_transcript_error") or "").lower()
        if "unsupported" in reason_note or "unsupported_value" in reason_note:
            body = "The voice note format isn’t supported yet. If you can drop a quick text summary, I’ll jump on it."
        elif "download_failed" in reason_note:
            body = "I couldn’t fetch the voice note from WhatsApp just now. Could you try resending or share a short text?"
        else:
            body = "I couldn’t pick up the voice note yet. If you send a quick text summary, I’ll dive in right away."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    # smalltalk / date / time / greeting
    if intent == "smalltalk":
        reason_note = ctx.get("_last_kpi_reason")
        options = SMALLTALK_RESPONSES
        body = random.choice(options)
        if reason_note:
            pretty = _pretty_reason(reason_note) or reason_note
            body = f"Still no update on the data because {pretty}. How’s your shift going today?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
    if intent == "current_date":
        body = f"Today is {fmt_jhb_date()}."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
    if intent == "current_time":
        body = f"The current time in JHB is {fmt_jhb_time()}."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
    if intent == "greeting":
        tmpl_ctx = _recent_outbound_template(ctx, "driver_update")
        if tmpl_ctx:
            params_named = tmpl_ctx.get("params_named") or {}
            amount = params_named.get("amount") or params_named.get("outstanding")
            if amount:
                body = f"Thanks for replying. Your outstanding balance is {amount}. Would you like a payment plan?"
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        reason_note = ctx.get("_last_kpi_reason")
        if reason_note:
            pretty = _pretty_reason(reason_note) or reason_note
            body = f"I’m still waiting on the data because {pretty}. Want to look at something else for now?"
        else:
            body = random.choice(GREETING_RESPONSES)
        body = soften_reply(body, name)
        return with_greet(ctx, name, body, intent, force=True)

    if intent in KPI_INTENTS:
        intraday_opted = False
        commitment_prompt = False
        ctx["_last_kpi_intent"] = intent
        ctx["_last_kpi_intent_at"] = time.time()
        msg_lower = (msg or "").lower()
        if intent == "progress_update":
            if "daily update" not in msg_lower and "daily updates" not in msg_lower:
                commitment_prompt = _should_prompt_commitment(ctx)
            ctx["_awaiting_target_update"] = True
            ctx["_awaiting_target_update_at"] = time.time()
            if INTRADAY_UPDATES_ENABLED and mysql_available() and _wants_intraday_updates(msg):
                ctx["_intraday_updates_enabled"] = True
                ctx["_intraday_updates_paused"] = False
                ctx["_intraday_updates_opted_at"] = time.time()
                intraday_opted = True
        elif intent not in {"hotspot_summary", "daily_target_status"}:
            commitment_prompt = _should_prompt_commitment(ctx)
        metrics, reason = get_driver_kpis(wa_id, d)
        if intent == "daily_target_status":
            needs_today = not metrics or (
                metrics.get("today_finished_trips") is None
                and metrics.get("today_trips_sent") is None
            )
            if needs_today:
                fresh_metrics, fresh_reason = get_driver_kpis(wa_id, d, use_cache=False, include_today=True)
                if fresh_metrics is not None:
                    metrics, reason = fresh_metrics, fresh_reason
        if metrics:
            ctx["_last_kpis"] = metrics
            if metrics.get("period_label"):
                ctx["_last_kpi_period"] = metrics.get("period_label")
        else:
            if intent != "daily_target_status":
                metrics = ctx.get("_last_kpis")
        if reason:
            ctx["_last_kpi_reason"] = reason
        elif metrics:
            ctx.pop("_last_kpi_reason", None)
        else:
            reason = reason or ctx.get("_last_kpi_reason")

        requested_hour = _extract_time_hour(msg or "")
        include_hours = _mentions_hours(msg or "")
        now = jhb_now()
        contact_ids = (d.get("xero_contact_ids") or [])[:5]
        hotspot_scope = _detect_hotspot_scope(msg_lower)
        hotspot_timeframe_key = _detect_hotspot_timeframe(msg_lower)

        scope_contact_ids = contact_ids if hotspot_scope != "global" else []
        hotspots_list, hotspots_reason, hotspots_label = get_hotspots(
            contact_ids=scope_contact_ids,
            scope=hotspot_scope,
            timeframe=hotspot_timeframe_key,
        )

        ctx["_hotspots_scope"] = hotspot_scope
        ctx["_hotspots_label"] = hotspots_label

        if hotspots_list:
            ctx["_hotspots_cache"] = hotspots_list
            ctx.pop("_hotspots_reason", None)
        else:
            ctx["_hotspots_cache"] = []
            if hotspots_reason:
                ctx["_hotspots_reason"] = hotspots_reason
            else:
                hotspots_reason = ctx.get("_hotspots_reason")

        hotspots_reason = hotspots_reason or ctx.get("_hotspots_reason")

        driver_id = (metrics or {}).get("driver_id") or d.get("driver_id")
        if driver_id is not None:
            try:
                driver_id = int(driver_id)
            except Exception:
                driver_id = None

        hotspot_times, hotspot_times_reason, hotspot_times_label = get_hotspot_times(
            contact_ids=scope_contact_ids,
            scope=hotspot_scope,
            timeframe=hotspot_timeframe_key,
            driver_id=driver_id,
        )

        ctx["_hotspot_times_label"] = hotspot_times_label
        if hotspot_times:
            ctx["_hotspot_times_cache"] = hotspot_times
            ctx.pop("_hotspot_times_reason", None)
        else:
            ctx["_hotspot_times_cache"] = []
            if hotspot_times_reason:
                ctx["_hotspot_times_reason"] = hotspot_times_reason
            else:
                hotspot_times_reason = ctx.get("_hotspot_times_reason")

        hotspot_times_reason = hotspot_times_reason or ctx.get("_hotspot_times_reason")

        oph_areas = None
        oph_reason = None
        oph_label = None
        oph_scope = hotspot_scope
        area_oph = None
        area_oph_reason = None
        area_oph_label = None
        area_oph_requested = None
        if intent == "hotspot_summary":
            area_candidates = _extract_area_candidates(msg or "")
            if area_candidates:
                area_oph_requested = area_candidates
                area_oph, area_oph_reason, area_oph_label = get_oph_for_areas(
                    contact_ids=scope_contact_ids,
                    scope=hotspot_scope,
                    timeframe=hotspot_timeframe_key,
                    areas=area_candidates,
                )
                if area_oph:
                    ctx["_area_oph_cache"] = area_oph
                    ctx.pop("_area_oph_reason", None)
                else:
                    ctx["_area_oph_cache"] = []
                    if area_oph_reason:
                        ctx["_area_oph_reason"] = area_oph_reason
                ctx["_area_oph_label"] = area_oph_label
                ctx["_area_oph_requested"] = area_oph_requested
            else:
                ctx.pop("_area_oph_cache", None)
                ctx.pop("_area_oph_reason", None)
                ctx.pop("_area_oph_label", None)
                ctx.pop("_area_oph_requested", None)
            oph_areas, oph_reason, oph_label = get_top_oph_areas(
                contact_ids=scope_contact_ids,
                scope=hotspot_scope,
                timeframe=hotspot_timeframe_key,
                limit=3,
            )
            if not oph_areas and hotspot_scope != "global":
                fallback_areas, fallback_reason, fallback_label = get_top_oph_areas(
                    contact_ids=[],
                    scope="global",
                    timeframe=hotspot_timeframe_key,
                    limit=3,
                )
                if fallback_areas:
                    oph_areas, oph_reason, oph_label = fallback_areas, fallback_reason, fallback_label
                    oph_scope = "global"
                elif not oph_reason:
                    oph_reason = fallback_reason
                    oph_label = oph_label or fallback_label

            ctx["_oph_areas_scope"] = oph_scope
            if oph_label:
                ctx["_oph_areas_label"] = oph_label
            if oph_areas:
                ctx["_oph_areas_cache"] = oph_areas
                ctx.pop("_oph_areas_reason", None)
            else:
                ctx["_oph_areas_cache"] = []
                if oph_reason:
                    ctx["_oph_areas_reason"] = oph_reason
                else:
                    oph_reason = ctx.get("_oph_areas_reason")
            oph_reason = oph_reason or ctx.get("_oph_areas_reason")

        if LOG_DB_INSERTS:
            log.info(
                "[KPI] intent=%s metrics=%s reason=%s hotspot_scope=%s hotspot_timeframe=%s hotspot_reason=%s",
                intent,
                metrics,
                reason,
                hotspot_scope,
                hotspots_label,
                hotspots_reason,
            )
        targets = get_model_targets(d.get("asset_model"))
        goal_targets = _recent_goal_targets(ctx)
        if goal_targets:
            targets = dict(targets or {})
            targets.update(goal_targets)
            min_hours, min_trips = _min_target_thresholds()
            if targets.get("online_hours") and targets["online_hours"] < min_hours:
                targets["online_hours"] = min_hours
            if targets.get("trip_count") and targets["trip_count"] < min_trips:
                targets["trip_count"] = min_trips
        if intent not in {"hotspot_summary", "daily_target_status"}:
            if _is_no_trips_metrics(metrics) or (not metrics and _is_missing_kpi_reason(reason)):
                body = _kpi_no_trips_commitment_message(targets)
                if intraday_opted:
                    body = f"{body} Bi-hourly updates are on; reply 'stop updates' to pause."
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        if metrics:
            ai_body = _compose_kpi_ai_reply(
                intent=intent,
                user_message=msg,
                name=name,
                metrics=metrics,
                targets=targets,
                ctx=ctx,
                hotspots=ctx.get("_hotspots_cache"),
                hotspots_reason=hotspots_reason,
                hotspots_timeframe=ctx.get("_hotspots_label"),
                hotspots_scope=ctx.get("_hotspots_scope", HOTSPOT_SCOPE_DEFAULT),
                oph_areas=ctx.get("_oph_areas_cache"),
                oph_reason=ctx.get("_oph_areas_reason"),
                oph_timeframe=ctx.get("_oph_areas_label"),
                oph_scope=ctx.get("_oph_areas_scope", HOTSPOT_SCOPE_DEFAULT),
                hotspot_times=ctx.get("_hotspot_times_cache"),
                hotspot_times_timeframe=ctx.get("_hotspot_times_label"),
                hotspot_times_scope=ctx.get("_hotspots_scope", HOTSPOT_SCOPE_DEFAULT),
            )
            body = ai_body or render_kpi_reply(
                intent,
                metrics,
                targets,
                reason,
                ctx.get("_hotspots_cache"),
                hotspots_reason,
                ctx.get("_hotspots_label"),
                ctx.get("_hotspots_scope", HOTSPOT_SCOPE_DEFAULT),
                oph_areas=ctx.get("_oph_areas_cache"),
                oph_reason=ctx.get("_oph_areas_reason"),
                oph_timeframe=ctx.get("_oph_areas_label"),
                oph_scope=ctx.get("_oph_areas_scope", HOTSPOT_SCOPE_DEFAULT),
                area_oph=ctx.get("_area_oph_cache"),
                area_oph_reason=ctx.get("_area_oph_reason"),
                area_oph_timeframe=ctx.get("_area_oph_label"),
                area_oph_requested=ctx.get("_area_oph_requested"),
                hotspot_times=ctx.get("_hotspot_times_cache"),
                hotspot_times_reason=hotspot_times_reason,
                hotspot_times_timeframe=ctx.get("_hotspot_times_label"),
                requested_hour=requested_hour,
                include_hours=include_hours,
                now=now,
                commitment_prompt=commitment_prompt,
            )
        else:
            body = render_kpi_reply(
                intent,
                metrics,
                targets,
                reason,
                ctx.get("_hotspots_cache"),
                hotspots_reason,
                ctx.get("_hotspots_label"),
                ctx.get("_hotspots_scope", HOTSPOT_SCOPE_DEFAULT),
                oph_areas=ctx.get("_oph_areas_cache"),
                oph_reason=ctx.get("_oph_areas_reason"),
                oph_timeframe=ctx.get("_oph_areas_label"),
                oph_scope=ctx.get("_oph_areas_scope", HOTSPOT_SCOPE_DEFAULT),
                area_oph=ctx.get("_area_oph_cache"),
                area_oph_reason=ctx.get("_area_oph_reason"),
                area_oph_timeframe=ctx.get("_area_oph_label"),
                area_oph_requested=ctx.get("_area_oph_requested"),
                hotspot_times=ctx.get("_hotspot_times_cache"),
                hotspot_times_reason=hotspot_times_reason,
                hotspot_times_timeframe=ctx.get("_hotspot_times_label"),
                requested_hour=requested_hour,
                include_hours=include_hours,
                now=now,
                commitment_prompt=commitment_prompt,
            )
        if commitment_prompt:
            ctx["_commitment_prompt_at"] = time.time()
            ctx["_commitment_prompt_targets"] = {
                "online_hours": targets.get("online_hours"),
                "trip_count": targets.get("trip_count"),
            }
        if commitment_prompt and intent not in {"progress_update", "hotspot_summary", "daily_target_status"}:
            prompt_line = _commitment_prompt_line(targets, include_update_hint=True)
            body = f"{body} {prompt_line}".strip()
        if intraday_opted:
            schedule = _intraday_schedule_label()
            body = f"{body} Bi-hourly updates are on ({schedule}); reply 'stop updates' to pause."
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent == "balance_dispute":
        login_url = "https://60b9b868ac0b.ngrok-free.app/driver/login"
        ctx["_active_concern"] = {"type": "balance_dispute", "opened_at": time.time(), "message": msg}
        ticket_ctx = ctx.get("_balance_dispute_ticket") if isinstance(ctx.get("_balance_dispute_ticket"), dict) else None
        if ticket_ctx and _ticket_ctx_is_closed(ticket_ctx, ctx):
            ctx.pop("_balance_dispute_ticket", None)
            ticket_ctx = None
        ticket_id = ticket_ctx.get("ticket_id") if ticket_ctx else None
        issue_type = (ticket_ctx or {}).get("issue_type")
        if not ticket_id:
            finance_ctx = ctx.get("_no_vehicle_finance_ticket") if isinstance(ctx.get("_no_vehicle_finance_ticket"), dict) else None
            if finance_ctx and not _ticket_ctx_is_closed(finance_ctx, ctx):
                ticket_id = finance_ctx.get("ticket_id")
                issue_type = "finance_followup"
            if not ticket_id:
                existing = fetch_open_driver_issue_ticket(wa_id, ["balance_dispute", "finance_followup"])
                if existing:
                    ticket_id = existing.get("id")
                    issue_type = (existing.get("issue_type") or "").strip().lower() or issue_type
            if ticket_id:
                ctx["_balance_dispute_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": (ticket_ctx or {}).get("status") or "collecting",
                    "opened_at": time.time(),
                    "issue_type": issue_type,
                }
                if issue_type == "finance_followup" and not isinstance(ctx.get("_no_vehicle_finance_ticket"), dict):
                    ctx["_no_vehicle_finance_ticket"] = {
                        "ticket_id": ticket_id,
                        "status": (ticket_ctx or {}).get("status") or "collecting",
                        "opened_at": time.time(),
                    }
            else:
                ticket_id = create_driver_issue_ticket(wa_id, msg or "Balance dispute", d, issue_type="balance_dispute")
                ctx["_balance_dispute_ticket"] = {
                    "ticket_id": ticket_id,
                    "status": "collecting" if ticket_id else "collecting",
                    "opened_at": time.time(),
                    "issue_type": "balance_dispute",
                }
        detail_text = (msg or "").strip()
        pc = extract_personal_code(detail_text)
        if pc and "personal_code" not in ctx:
            ctx["personal_code"] = pc
        if ticket_id and detail_text:
            patch = {
                "last_driver_note": detail_text,
                "last_driver_note_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
                "balance_dispute": True,
            }
            if pc:
                patch["personal_code"] = pc
            update_driver_issue_metadata(ticket_id, patch)

        if ctx.get("_balance_dispute_pending"):
            if _is_positive_confirmation(msg) or _is_negative_confirmation(msg) or (msg or "").strip().lower() in ACKNOWLEDGEMENT_TOKENS:
                body = (
                    "No problem. Please send your personal code and a short note of what looks wrong (amount/date). "
                    f"You can also check your statement here: {login_url}."
                )
                return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
            ctx["_balance_dispute_pending"] = False
            body = "Thanks, I’ve added that to your Finance ticket. We’ll review and get back to you."
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

        ctx["_balance_dispute_pending"] = True
        if pc:
            body = (
                "Thanks for flagging that — I’ve opened a Finance ticket to review your balance. "
                "Please share a short note of what looks wrong (amount/date). "
                f"You can also check your statement here: {login_url}."
            )
        else:
            body = (
                "Thanks for flagging that — I’ve opened a Finance ticket to review your balance. "
                "Please send your personal code and a short note of what looks wrong (amount/date). "
                f"You can also check your statement here: {login_url}."
            )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    # account inquiry (send login link + personal code guidance)
    if intent == "account_inquiry":
        login_url = "https://60b9b868ac0b.ngrok-free.app/driver/login"
        if template_amount:
            body = (
                f"The outstanding balance I mentioned is {template_amount}. "
                f"For the latest balance, please log in here: {login_url}. "
                "Use your personal code (South African ID number, or Traffic Register Number (TRN) for foreign nationals from your PrDP)."
            )
            return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)
        body = (
            f"You can check your account balance here: {login_url}. "
            "Please use your personal code (South African ID number, or Traffic Register Number (TRN) for foreign nationals from your PrDP)."
        )
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent in {"fallback", "clarify", "unknown"} and template_amount and _should_use_template_context(msg):
        body = f"The outstanding balance I mentioned is {template_amount}. Would you like a payment plan?"
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    if intent in {"fallback", "clarify", "unknown"}:
        llm_body = _compose_llm_fallback_reply(
            user_message=msg,
            name=name,
            ctx=ctx,
            driver=d,
        )
        if llm_body:
            return soften_reply(_strip_leading_greeting_or_name(llm_body, d.get("display_name") or "", name), name)
        body = _build_clarify_reply(msg, ctx, d)
        return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

    body = _build_clarify_reply(msg, ctx, d)
    return soften_reply(_strip_leading_greeting_or_name(body, d.get("display_name") or "", name), name)

# -----------------------------------------------------------------------------
# Status webhook logging
# -----------------------------------------------------------------------------
def log_status_event(*, status_event: dict, business_number: str, phone_number_id: str, origin_type: str):
    wa_msg_id = status_event.get("id")
    ts_unix   = status_event.get("timestamp")
    recv_id   = status_event.get("recipient_id")
    log_message(
        direction="STATUS",
        wa_id=str(recv_id or ""),
        text=None,
        intent=None,
        status=status_event.get("status"),
        wa_message_id=wa_msg_id,
        message_id=wa_msg_id,
        business_number=business_number,
        phone_number_id=phone_number_id,
        origin_type=origin_type,
        raw_json=status_event,
        timestamp_unix=ts_unix,
        intent_label=None,
        sentiment=None,
        sentiment_score=None,
    )
    status_detail = None
    status_code = None
    errors = status_event.get("errors") or []
    if errors:
        first_error = errors[0] or {}
        status_code = str(first_error.get("code")) if first_error.get("code") is not None else None
        status_detail = first_error.get("message") or first_error.get("title")
    _update_nudge_event_status(
        whatsapp_message_id=wa_msg_id,
        status=status_event.get("status"),
        status_code=status_code,
        status_detail=status_detail,
        raw_status=status_event,
    )

# -----------------------------------------------------------------------------
# FastAPI
# -----------------------------------------------------------------------------
app = FastAPI(title="Dineo WA Bot (JHB + schema-aware logging)")

@app.on_event("startup")
async def _startup_zero_trip_worker():
    asyncio.create_task(_bootstrap_schema_startup())
    asyncio.create_task(_keep_driver_roster_cache_warm())
    try:
        _start_zero_trip_nudge_worker()
    except Exception as exc:
        log.warning("Zero-trip nudge worker not started: %s", exc)
    try:
        _start_engagement_followup_worker()
    except Exception as exc:
        log.warning("Engagement follow-up worker not started: %s", exc)
    try:
        _start_intraday_updates_worker()
    except Exception as exc:
        log.warning("Intraday updates worker not started: %s", exc)


async def _bootstrap_schema_startup():
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, ensure_schema)
        await loop.run_in_executor(None, ensure_admin_bootstrap_user)
    except Exception as exc:
        log.warning("Schema bootstrap task failed: %s", exc)


async def _warm_driver_roster_cache_async():
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(None, warm_driver_roster_cache)
    except Exception as exc:
        log.warning("Driver roster cache warm-up failed: %s", exc)


async def _keep_driver_roster_cache_warm():
    loop = asyncio.get_running_loop()
    interval = max(10, DRIVER_ROSTER_WARM_INTERVAL_SECONDS)
    while True:
        try:
            await loop.run_in_executor(None, warm_driver_roster_cache)
        except Exception as exc:
            log.warning("Periodic driver roster warm failed: %s", exc)
        await asyncio.sleep(interval)

@app.get("/health/db", response_class=PlainTextResponse)
def health_db():
    try:
        require_mysql()
        conn = get_mysql()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            _ = cur.fetchone()
        return "DB OK"
    except Exception as e:
        return f"DB ERROR: {e}"


def _redirect_to_login() -> RedirectResponse:
    return RedirectResponse(url="/admin/login", status_code=303)


@app.get("/", response_class=RedirectResponse)
def landing(request: Request):
    # Send authenticated admins to the summary; unauthenticated users to login.
    if get_authenticated_admin(request):
        return RedirectResponse(url="/admin/summary", status_code=303)
    return _redirect_to_login()


@app.get("/admin/summary", response_class=HTMLResponse)
def admin_summary(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    # Load full roster to compute aggregates
    drivers, error, total, collections_options, status_options, driver_type_options, payer_type_options = fetch_active_driver_profiles(
        limit=0, offset=0, paginate=False
    )
    if error:
        drivers = []
    all_drivers_for_options = list(drivers)
    # Filters for summary page
    def _split_multi(key: str) -> List[str]:
        values: List[str] = []
        for raw in request.query_params.getlist(key):
            for part in str(raw).split(","):
                val = part.strip()
                if val:
                    values.append(val)
        return values

    def _normalize_status(value: Optional[str]) -> str:
        return str(value or "").strip()

    status_options = sorted(
        {
            _normalize_status(d.get("status") or d.get("sf_status") or d.get("portal_status"))
            for d in all_drivers_for_options
            if _normalize_status(d.get("status") or d.get("sf_status") or d.get("portal_status"))
        }
    )

    selected_collections = _split_multi("collections_agent")
    selected_models = _split_multi("model")
    selected_status = _split_multi("status")
    if selected_collections:
        lc = {c.lower() for c in selected_collections}
        drivers = [d for d in drivers if (d.get("collections_agent") or "").lower() in lc]
    if selected_models:
        lm = {m.lower() for m in selected_models}
        drivers = [d for d in drivers if (d.get("model") or "").lower() in lm]
    if selected_status:
        desired = {s.lower() for s in selected_status}
        drivers = [
            d
            for d in drivers
            if (str(d.get("status") or d.get("sf_status") or "").strip().lower() in desired)
        ]

    driver_summary = _build_badge_summary(drivers, "efficiency_badge_label")
    payer_summary = _build_badge_summary(drivers, "payer_badge_label")
    return templates.TemplateResponse(
        "admin_summary.html",
        {
            "request": request,
            "admin": admin_user,
            "driver_summary": driver_summary,
            "payer_summary": payer_summary,
            "nav_active": "drivers",
            "total_drivers": total,
            "format_rands": fmt_rands,
            "collections_options": collections_options,
            "status_options": status_options,
            "model_options": sorted({(d.get("model") or "").strip() for d in all_drivers_for_options if d.get("model")}),
            "selected_collections": selected_collections,
            "selected_models": selected_models,
            "selected_status": selected_status,
        },
    )


def _build_engagement_preview_rows(
    rows: List[Dict[str, Any]],
    template_map: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    preview_rows: List[Dict[str, Any]] = []
    counts = {
        "total": 0,
        "ready": 0,
        "missing_wa": 0,
        "duplicate": 0,
        "template_missing": 0,
        "missing_vars": 0,
        "skipped": 0,
    }
    seen: set[str] = set()
    templates = {t.get("id"): t for t in get_whatsapp_templates()}
    wa_ids = [row.get("wa_id") for row in rows if row.get("wa_id")]
    last_by_wa = _fetch_last_template_by_wa_ids(wa_ids)
    for row in rows:
        counts["total"] += 1
        driver_type = row.get("driver_type") or "Unknown"
        template_group = (
            template_map.get(driver_type)
            or template_map.get("__default__")
            or _default_template_for_driver_type(driver_type)
        )
        template_id = _resolve_template_variant_id(
            templates,
            template_group,
            row.get("wa_id"),
            last_by_wa.get(row.get("wa_id") or ""),
        )
        status = "ready"
        reason = ""
        if not row.get("wa_id"):
            status = "missing_wa"
            reason = "Missing WhatsApp number"
            counts["missing_wa"] += 1
        elif row.get("wa_id") in seen:
            status = "duplicate"
            reason = "Duplicate WhatsApp number"
            counts["duplicate"] += 1
        elif not template_group or template_group == "skip":
            status = "skipped"
            reason = "Skipped"
            counts["skipped"] += 1
        elif template_group not in templates:
            status = "template_missing"
            reason = "Template not found"
            counts["template_missing"] += 1
        else:
            params, missing = _resolve_template_params(templates[template_group], row)
            if missing:
                status = "missing_vars"
                reason = f"Missing variables: {', '.join(missing)}"
                counts["missing_vars"] += 1
            else:
                counts["ready"] += 1
            row = dict(row)
            row["template_params"] = params
        if row.get("wa_id"):
            seen.add(row.get("wa_id"))
        preview_rows.append(
            {
                **row,
                "driver_type": driver_type,
                "template_id": template_id,
                "template_group": template_group,
                "preview_status": status,
                "preview_reason": reason,
            }
        )
    return preview_rows, counts


def _send_engagement_messages(
    *,
    campaign_id: str,
    rows: List[Dict[str, Any]],
    template_map: Dict[str, str],
    admin_email: str,
    update_progress: bool = False,
    campaign_type: str = ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE,
) -> Dict[str, int]:
    templates = {t.get("id"): t for t in get_whatsapp_templates()}
    _preview_rows, counts = _build_engagement_preview_rows(rows, template_map)
    total_ready = counts.get("ready", 0)
    sent_rows: List[Dict[str, Any]] = []
    sent_count = 0
    failed_count = 0
    skipped_count = 0
    processed = 0

    for row in _preview_rows:
        send_status = "skipped"
        send_error = None
        message_id = None
        sent_at = None
        params_used: List[str] = []
        if campaign_type == ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS:
            baseline = _baseline_collections_metrics_from_row(row)
        else:
            baseline = _baseline_metrics_from_row(row)
        if row.get("preview_status") == "ready":
            template_id = row.get("template_id")
            template_group = row.get("template_group") or template_id
            tmpl = templates.get(template_group) or templates.get(template_id) or {}
            params, missing = _resolve_template_params(tmpl, row)
            params_used = params or []
            if missing:
                send_status = "skipped"
                send_error = "missing_vars"
                skipped_count += 1
                processed += 1
            else:
                outbound_id = send_whatsapp_template(
                    row.get("wa_id"),
                    template_id,
                    tmpl.get("language") or "en",
                    params,
                    None,
                    param_names=tmpl.get("variables"),
                    parameter_format=tmpl.get("parameter_format"),
                )
                processed += 1
                if outbound_id:
                    _record_outbound_template_context(
                        row.get("wa_id") or "",
                        template_id,
                        params,
                        param_names=tmpl.get("variables"),
                        parameter_format=tmpl.get("parameter_format"),
                    )
                    send_status = "sent"
                    message_id = outbound_id
                    sent_at = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
                    sent_count += 1
                    log_interaction(
                        row.get("wa_id"),
                        channel="whatsapp",
                        template_id=template_id,
                        variables_json=json.dumps(params or []),
                        admin_email=admin_email,
                        status="sent",
                    )
                    log_message(
                        direction="OUTBOUND",
                        wa_id=row.get("wa_id") or "",
                        text=f"template:{template_id}",
                        intent=None,
                        status="sent",
                        wa_message_id=outbound_id,
                        message_id=outbound_id,
                        business_number=None,
                        phone_number_id=None,
                        origin_type="admin_engagements",
                        raw_json={
                            "campaign_id": campaign_id,
                            "template_id": template_id,
                            "template_group": template_group,
                            "variables": params or [],
                        },
                        timestamp_unix=str(int(time.time())),
                    )
                else:
                    send_status = "failed"
                    send_error = "send_failed"
                    failed_count += 1
                    log_message(
                        direction="OUTBOUND",
                        wa_id=row.get("wa_id") or "",
                        text=f"template:{template_id}",
                        intent=None,
                        status="send_failed",
                        wa_message_id=None,
                        message_id=None,
                        business_number=None,
                        phone_number_id=None,
                        origin_type="admin_engagements",
                        raw_json={
                            "campaign_id": campaign_id,
                            "template_id": template_id,
                            "template_group": template_group,
                            "variables": params or [],
                            "error": "send_failed",
                        },
                        timestamp_unix=str(int(time.time())),
                    )
        else:
            skipped_count += 1
            send_error = row.get("preview_reason") or "skipped"

        if update_progress and total_ready > 0:
            _update_engagement_send_progress(
                campaign_id,
                processed=processed,
                sent=sent_count,
                failed=failed_count,
                skipped=skipped_count,
                total=total_ready,
                status="running",
            )

        sent_rows.append(
            {
                "wa_id": row.get("wa_id") or "",
                "display_name": row.get("display_name"),
                "driver_type": row.get("driver_type"),
                "template_id": row.get("template_id"),
                "template_group": row.get("template_group"),
                "variables_json": json.dumps(params_used or row.get("template_params") or []),
                "baseline_json": json.dumps(baseline, ensure_ascii=False),
                "send_status": send_status,
                "send_error": send_error,
                "message_id": message_id,
                "sent_at": sent_at,
                "row_json": json.dumps(_engagement_row_payload(row), ensure_ascii=False),
            }
        )

    _insert_engagement_rows(campaign_id, sent_rows)
    _update_engagement_campaign_stats(
        campaign_id,
        sent_count=sent_count,
        failed_count=failed_count,
        skipped_count=skipped_count,
    )
    return {
        "sent": sent_count,
        "failed": failed_count,
        "skipped": skipped_count,
        "processed": processed,
        "total": total_ready,
    }


def _run_engagement_send_job(
    *,
    campaign_id: str,
    rows: List[Dict[str, Any]],
    template_map: Dict[str, str],
    admin_email: str,
    campaign_type: str = ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE,
) -> None:
    try:
        _update_engagement_send_progress(
            campaign_id,
            status="running",
            error=None,
        )
        stats = _send_engagement_messages(
            campaign_id=campaign_id,
            rows=rows,
            template_map=template_map,
            admin_email=admin_email,
            update_progress=True,
            campaign_type=campaign_type,
        )
        _update_engagement_send_progress(
            campaign_id,
            status="done",
            processed=stats.get("processed", 0),
            sent=stats.get("sent", 0),
            failed=stats.get("failed", 0),
            skipped=stats.get("skipped", 0),
            total=stats.get("total", 0),
        )
    except Exception as exc:
        _update_engagement_send_progress(
            campaign_id,
            status="error",
            error=str(exc),
        )


_engagement_followup_worker_started = False


def _update_engagement_followup_row(
    row_id: int,
    *,
    status: str,
    template_id: Optional[str] = None,
    error: Optional[str] = None,
    message_id: Optional[str] = None,
    sent_at: Optional[str] = None,
) -> None:
    if not mysql_available():
        return
    try:
        conn = get_mysql()
    except Exception:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ENGAGEMENT_ROW_TABLE}
                SET followup_status=%s,
                    followup_template_id=%s,
                    followup_error=%s,
                    followup_message_id=%s,
                    followup_sent_at=%s
                WHERE id=%s
                """,
                (status, template_id, error, message_id, sent_at, row_id),
            )
    except Exception as exc:
        log.debug("update engagement followup failed: %s", exc)

def _claim_engagement_followup_row(row_id: int) -> bool:
    if not mysql_available() or not row_id:
        return False
    try:
        conn = get_mysql()
    except Exception:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {ENGAGEMENT_ROW_TABLE}
                SET followup_status='sending'
                WHERE id=%s
                  AND (followup_status IS NULL OR followup_status = '')
                """,
                (row_id,),
            )
            return cur.rowcount > 0
    except Exception as exc:
        log.debug("claim engagement followup failed: %s", exc)
        return False


def _run_engagement_followups() -> None:
    if not ENGAGEMENT_FOLLOWUP_ENABLED:
        return
    if not mysql_available():
        return
    _ensure_engagement_tables()
    try:
        conn = get_mysql()
    except Exception:
        return

    cutoff = jhb_now() - timedelta(hours=ENGAGEMENT_FOLLOWUP_DELAY_HOURS)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT r.id, r.campaign_id, r.wa_id, r.display_name, r.driver_type,
                       r.template_id, r.template_group, r.variables_json, r.row_json, r.sent_at,
                       c.campaign_type
                FROM {ENGAGEMENT_ROW_TABLE} r
                JOIN {ENGAGEMENT_CAMPAIGN_TABLE} c ON r.campaign_id = c.id
                WHERE r.send_status='sent'
                  AND r.sent_at IS NOT NULL
                  AND (r.followup_status IS NULL OR r.followup_status = '')
                  AND r.sent_at <= %s
                ORDER BY r.sent_at ASC
                LIMIT %s
                """,
                (cutoff_str, ENGAGEMENT_FOLLOWUP_MAX_BATCH),
            )
            rows = cur.fetchall() or []
    except Exception as exc:
        log.debug("fetch engagement followup candidates failed: %s", exc)
        return

    if not rows:
        return

    now = jhb_now()
    wa_ids = [row.get("wa_id") for row in rows if row.get("wa_id")]
    sent_dates = [
        _parse_log_timestamp(row.get("sent_at"))
        for row in rows
        if row.get("sent_at") is not None
    ]
    start_dt = min([d for d in sent_dates if d], default=None)
    inbound_by_wa: Dict[str, List[datetime]] = {}
    if wa_ids and start_dt:
        inbound = _fetch_inbound_logs(wa_ids, start_dt, now)
        for entry in inbound:
            wa_id = entry.get("wa_id")
            ts = entry.get("ts")
            if wa_id and ts:
                inbound_by_wa.setdefault(wa_id, []).append(ts)

    templates = {t.get("id"): t for t in get_whatsapp_templates()}
    last_by_wa = _fetch_last_template_by_wa_ids(wa_ids)
    for row in rows:
        row_id = row.get("id")
        wa_id = row.get("wa_id")
        sent_at = _parse_log_timestamp(row.get("sent_at"))
        if not (row_id and wa_id and sent_at):
            continue
        if not _claim_engagement_followup_row(row_id):
            continue
        paused_ctx = load_context_file(wa_id)
        if paused_ctx.get("_global_opt_out") or paused_ctx.get("_engagement_followup_paused"):
            _update_engagement_followup_row(row_id, status="skipped_paused")
            continue

        responses = inbound_by_wa.get(wa_id, [])
        if any(ts >= sent_at for ts in responses):
            _update_engagement_followup_row(row_id, status="skipped_responded")
            continue

        config = _get_engagement_page_config(row.get("campaign_type") or ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE)
        followup_group = config.get("followup_template_id")
        if not followup_group or followup_group not in templates:
            _update_engagement_followup_row(row_id, status="skipped_no_template")
            continue

        followup_template_id = _resolve_template_variant_id(
            templates,
            followup_group,
            wa_id,
            last_by_wa.get(wa_id),
        )
        template = templates.get(followup_group) or {}
        row_data = _safe_json_load(row.get("row_json"), default={})
        if row.get("display_name") and not row_data.get("display_name"):
            row_data["display_name"] = row.get("display_name")
        if wa_id and not row_data.get("wa_id"):
            row_data["wa_id"] = wa_id
        params, missing = _resolve_template_params(template, row_data)
        if missing:
            _update_engagement_followup_row(
                row_id,
                status="skipped_missing_vars",
                template_id=followup_template_id,
                error=f"missing_vars: {', '.join(missing)}",
            )
            continue

        outbound_id = send_whatsapp_template(
            wa_id,
            followup_template_id,
            template.get("language") or "en",
            params,
            None,
            param_names=template.get("variables"),
            parameter_format=template.get("parameter_format"),
        )
        if outbound_id:
            _record_outbound_template_context(
                wa_id,
                followup_template_id,
                params,
                param_names=template.get("variables"),
                parameter_format=template.get("parameter_format"),
            )
            sent_at_str = now.strftime("%Y-%m-%d %H:%M:%S")
            _update_engagement_followup_row(
                row_id,
                status="sent",
                template_id=followup_template_id,
                message_id=outbound_id,
                sent_at=sent_at_str,
            )
            log_message(
                direction="OUTBOUND",
                wa_id=wa_id,
                text=f"template:{followup_template_id}",
                intent=None,
                status="sent",
                wa_message_id=outbound_id,
                message_id=outbound_id,
                business_number=None,
                phone_number_id=None,
                origin_type="admin_engagements_followup",
                raw_json={
                    "campaign_id": row.get("campaign_id"),
                    "template_id": followup_template_id,
                    "template_group": followup_group,
                    "variables": params or [],
                },
                timestamp_unix=str(int(time.time())),
            )
        else:
            _update_engagement_followup_row(
                row_id,
                status="failed",
                template_id=followup_template_id,
                error="send_failed",
            )
            log_message(
                direction="OUTBOUND",
                wa_id=wa_id,
                text=f"template:{followup_template_id}",
                intent=None,
                status="send_failed",
                wa_message_id=None,
                message_id=None,
                business_number=None,
                phone_number_id=None,
                origin_type="admin_engagements_followup",
                raw_json={
                    "campaign_id": row.get("campaign_id"),
                    "template_id": followup_template_id,
                    "template_group": followup_group,
                    "variables": params or [],
                    "error": "send_failed",
                },
                timestamp_unix=str(int(time.time())),
            )


def _run_no_vehicle_checkins() -> None:
    if not NO_VEHICLE_CHECKIN_ENABLED:
        return
    now_ts = time.time()
    for ctx_path in CTX_DIR.glob("*.json"):
        wa_id = ctx_path.stem
        if not wa_id:
            continue
        ctx = load_context_file(wa_id)
        if ctx.get("_global_opt_out"):
            continue
        if not ctx.get("_no_vehicle_checkin_pending"):
            continue
        due_at = ctx.get("_no_vehicle_checkin_due_at")
        if not due_at:
            continue
        try:
            due_ts = float(due_at)
        except Exception:
            continue
        if now_ts < due_ts:
            continue
        message = "Quick check — have you got the vehicle back yet?"
        outbound_id = send_whatsapp_text(wa_id, message)
        status = "sent" if outbound_id else "send_failed"
        ctx["_no_vehicle_checkin_pending"] = False
        ctx["_no_vehicle_checkin_sent_at"] = time.time()
        ctx["_no_vehicle_checkin_status"] = status
        ctx["_no_vehicle_checkin_message_id"] = outbound_id
        save_context_file(wa_id, ctx)
        log_message(
            direction="OUTBOUND",
            wa_id=wa_id,
            text=message,
            intent="no_vehicle_checkin",
            status=status,
            wa_message_id=outbound_id,
            message_id=outbound_id,
            business_number=None,
            phone_number_id=None,
            origin_type="no_vehicle_checkin",
            raw_json={
                "no_vehicle_checkin": True,
                "status": status,
                "message_id": outbound_id,
            },
            timestamp_unix=str(int(time.time())),
        )


def _engagement_followup_worker() -> None:
    while True:
        try:
            _run_engagement_followups()
            _run_no_vehicle_checkins()
        except Exception as exc:
            log.warning("Engagement follow-up worker error: %s", exc)
        time.sleep(max(60, ENGAGEMENT_FOLLOWUP_INTERVAL_SECONDS))


def _start_engagement_followup_worker() -> None:
    global _engagement_followup_worker_started
    if _engagement_followup_worker_started:
        return
    if not ENGAGEMENT_FOLLOWUP_ENABLED or ENGAGEMENT_FOLLOWUP_INTERVAL_SECONDS <= 0:
        log.info("Engagement follow-up worker disabled by configuration.")
        _engagement_followup_worker_started = True
        return
    thread = threading.Thread(
        target=_engagement_followup_worker,
        name="engagement-followup",
        daemon=True,
    )
    thread.start()
    _engagement_followup_worker_started = True
    log.info(
        "Engagement follow-up worker started (interval=%ss, delay=%sh).",
        ENGAGEMENT_FOLLOWUP_INTERVAL_SECONDS,
        ENGAGEMENT_FOLLOWUP_DELAY_HOURS,
    )

def _engagement_base_context(
    request: Request,
    admin_user: Dict[str, Any],
    *,
    config: Dict[str, Any],
    view: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = {
        "request": request,
        "admin": admin_user,
        "nav_active": config["nav_active"],
        "view": view,
        "base_path": config["base_path"],
        "page_title": config["page_title"],
        "page_upload_title": config["page_upload_title"],
        "page_description": config["page_description"],
    }
    if extra:
        payload.update(extra)
    return payload


def _engagement_campaign_base_path(campaign_type: Optional[str]) -> str:
    if not campaign_type:
        return ENGAGEMENT_PAGE_CONFIGS[ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE]["base_path"]
    return _get_engagement_page_config(campaign_type)["base_path"]


def _render_engagement_upload(
    request: Request,
    admin_user: Dict[str, Any],
    *,
    config: Dict[str, Any],
    error: Optional[str] = None,
    status_code: int = 200,
):
    campaigns = _fetch_engagement_campaigns(limit=15, campaign_type=config["campaign_type"])
    return templates.TemplateResponse(
        "admin_engagements.html",
        _engagement_base_context(
            request,
            admin_user,
            config=config,
            view="upload",
            extra={
                "campaigns": campaigns,
                "templates": get_whatsapp_templates(),
                "error": error,
            },
        ),
        status_code=status_code,
    )


async def _render_engagement_preview(
    request: Request,
    admin_user: Dict[str, Any],
    csv_file: UploadFile,
    *,
    config: Dict[str, Any],
):
    _prune_engagement_preview_cache()
    rows, error = _parse_engagement_csv(csv_file)
    if error:
        return _render_engagement_upload(
            request,
            admin_user,
            config=config,
            error=error,
            status_code=400,
        )
    prepared: List[Dict[str, Any]] = []
    for row in rows:
        wa_id = _normalize_wa_id(row.get("wa_raw")) if row.get("wa_raw") else None
        prepared.append(
            {
                **row,
                "wa_id": wa_id,
                "display_name": row.get("display_name") or "",
                "driver_type": row.get("driver_type") or "Unknown",
            }
        )
    driver_types = sorted({r.get("driver_type") or "Unknown" for r in prepared})
    driver_type_slugs = {
        dt: re.sub(r"[^a-z0-9]+", "_", dt.strip().lower()).strip("_") for dt in driver_types
    }
    template_map = {dt: _default_template_for_driver_type(dt) for dt in driver_types}
    template_map["__default__"] = ENGAGEMENT_DEFAULT_TEMPLATE
    preview_rows, counts = _build_engagement_preview_rows(prepared, template_map)
    preview_id = secrets.token_urlsafe(12)
    _set_engagement_preview_cache(
        preview_id,
        {
            "rows": prepared,
            "driver_types": driver_types,
            "template_map": template_map,
            "source_filename": csv_file.filename or "upload.csv",
            "admin_email": admin_user.get("email"),
            "campaign_type": config["campaign_type"],
        },
    )
    return templates.TemplateResponse(
        "admin_engagements.html",
        _engagement_base_context(
            request,
            admin_user,
            config=config,
            view="preview",
            extra={
                "preview_id": preview_id,
                "preview_rows": preview_rows[:150],
                "preview_counts": counts,
                "driver_types": driver_types,
                "driver_type_slugs": driver_type_slugs,
                "template_map": template_map,
                "templates": get_whatsapp_templates(),
                "source_filename": csv_file.filename or "upload.csv",
                "total_rows": len(prepared),
            },
        ),
    )


@app.get("/admin/engagements", response_class=HTMLResponse)
def admin_engagements(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    error = request.query_params.get("error")
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE)
    return _render_engagement_upload(request, admin_user, config=config, error=error)


@app.get("/admin/collections-ai", response_class=HTMLResponse)
def admin_collections_ai(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    error = request.query_params.get("error")
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS)
    return _render_engagement_upload(request, admin_user, config=config, error=error)


@app.post("/admin/engagements/preview", response_class=HTMLResponse)
async def admin_engagements_preview(
    request: Request,
    csv_file: UploadFile = File(...),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE)
    return await _render_engagement_preview(request, admin_user, csv_file, config=config)


@app.post("/admin/collections-ai/preview", response_class=HTMLResponse)
async def admin_collections_preview(
    request: Request,
    csv_file: UploadFile = File(...),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS)
    return await _render_engagement_preview(request, admin_user, csv_file, config=config)


@app.post("/admin/engagements/send", response_class=HTMLResponse)
async def admin_engagements_send(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    form = await request.form()
    preview_id = str(form.get("preview_id") or "").strip()
    preview = _get_engagement_preview_cache(preview_id)
    if not preview:
        return RedirectResponse(url="/admin/engagements?error=preview_expired", status_code=303)
    if preview.get("campaign_type") not in (None, ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE):
        return RedirectResponse(url="/admin/engagements?error=preview_expired", status_code=303)
    rows = preview.get("rows") or []
    driver_types = preview.get("driver_types") or []
    template_map = preview.get("template_map") or {}
    override_map = _parse_template_map(form, driver_types)
    if override_map:
        template_map.update({k: v for k, v in override_map.items() if v})
    campaign_id = f"eng_{int(time.time())}_{secrets.token_hex(4)}"
    _create_engagement_campaign(
        campaign_id=campaign_id,
        admin_email=admin_user.get("email") or "",
        source_filename=str(preview.get("source_filename") or "upload.csv"),
        template_map=template_map,
        total_rows=len(rows),
        campaign_type=ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE,
    )
    _send_engagement_messages(
        campaign_id=campaign_id,
        rows=rows,
        template_map=template_map,
        admin_email=admin_user.get("email") or "",
        update_progress=False,
        campaign_type=ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS,
    )
    return RedirectResponse(url=f"/admin/engagements/{campaign_id}", status_code=303)


@app.post("/admin/collections-ai/send", response_class=HTMLResponse)
async def admin_collections_send(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    form = await request.form()
    preview_id = str(form.get("preview_id") or "").strip()
    preview = _get_engagement_preview_cache(preview_id)
    if not preview:
        return RedirectResponse(url="/admin/collections-ai?error=preview_expired", status_code=303)
    if preview.get("campaign_type") != ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS:
        return RedirectResponse(url="/admin/collections-ai?error=preview_expired", status_code=303)
    rows = preview.get("rows") or []
    driver_types = preview.get("driver_types") or []
    template_map = preview.get("template_map") or {}
    override_map = _parse_template_map(form, driver_types)
    if override_map:
        template_map.update({k: v for k, v in override_map.items() if v})
    campaign_id = f"eng_{int(time.time())}_{secrets.token_hex(4)}"
    _create_engagement_campaign(
        campaign_id=campaign_id,
        admin_email=admin_user.get("email") or "",
        source_filename=str(preview.get("source_filename") or "upload.csv"),
        template_map=template_map,
        total_rows=len(rows),
        campaign_type=ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS,
    )
    _send_engagement_messages(
        campaign_id=campaign_id,
        rows=rows,
        template_map=template_map,
        admin_email=admin_user.get("email") or "",
        update_progress=False,
    )
    return RedirectResponse(url=f"/admin/collections-ai/{campaign_id}", status_code=303)


@app.post("/admin/engagements/send_async")
async def admin_engagements_send_async(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    form = await request.form()
    preview_id = str(form.get("preview_id") or "").strip()
    preview = _get_engagement_preview_cache(preview_id)
    if not preview:
        return JSONResponse({"error": "preview_expired"}, status_code=400)
    if preview.get("campaign_type") not in (None, ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE):
        return JSONResponse({"error": "preview_expired"}, status_code=400)
    rows = preview.get("rows") or []
    driver_types = preview.get("driver_types") or []
    template_map = preview.get("template_map") or {}
    override_map = _parse_template_map(form, driver_types)
    if override_map:
        template_map.update({k: v for k, v in override_map.items() if v})

    _prune_engagement_send_progress()
    _preview_rows, counts = _build_engagement_preview_rows(rows, template_map)
    total_ready = counts.get("ready", 0)
    campaign_id = f"eng_{int(time.time())}_{secrets.token_hex(4)}"
    _create_engagement_campaign(
        campaign_id=campaign_id,
        admin_email=admin_user.get("email") or "",
        source_filename=str(preview.get("source_filename") or "upload.csv"),
        template_map=template_map,
        total_rows=len(rows),
        campaign_type=ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE,
    )
    _set_engagement_send_progress(
        campaign_id,
        {
            "campaign_id": campaign_id,
            "status": "running",
            "total": total_ready,
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "skipped": counts.get("skipped", 0),
            "error": None,
            "started_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
            "started_at_unix": time.time(),
            "updated_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    thread = threading.Thread(
        target=_run_engagement_send_job,
        kwargs={
            "campaign_id": campaign_id,
            "rows": rows,
            "template_map": template_map,
            "admin_email": admin_user.get("email") or "",
            "campaign_type": ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS,
        },
        daemon=True,
    )
    thread.start()
    return JSONResponse(
        {
            "campaign_id": campaign_id,
            "total": total_ready,
            "ready": counts.get("ready", 0),
            "skipped": counts.get("skipped", 0),
        }
    )


@app.post("/admin/collections-ai/send_async")
async def admin_collections_send_async(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    form = await request.form()
    preview_id = str(form.get("preview_id") or "").strip()
    preview = _get_engagement_preview_cache(preview_id)
    if not preview:
        return JSONResponse({"error": "preview_expired"}, status_code=400)
    if preview.get("campaign_type") != ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS:
        return JSONResponse({"error": "preview_expired"}, status_code=400)
    rows = preview.get("rows") or []
    driver_types = preview.get("driver_types") or []
    template_map = preview.get("template_map") or {}
    override_map = _parse_template_map(form, driver_types)
    if override_map:
        template_map.update({k: v for k, v in override_map.items() if v})

    _prune_engagement_send_progress()
    _preview_rows, counts = _build_engagement_preview_rows(rows, template_map)
    total_ready = counts.get("ready", 0)
    campaign_id = f"eng_{int(time.time())}_{secrets.token_hex(4)}"
    _create_engagement_campaign(
        campaign_id=campaign_id,
        admin_email=admin_user.get("email") or "",
        source_filename=str(preview.get("source_filename") or "upload.csv"),
        template_map=template_map,
        total_rows=len(rows),
        campaign_type=ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS,
    )
    _set_engagement_send_progress(
        campaign_id,
        {
            "campaign_id": campaign_id,
            "status": "running",
            "total": total_ready,
            "processed": 0,
            "sent": 0,
            "failed": 0,
            "skipped": counts.get("skipped", 0),
            "error": None,
            "started_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
            "started_at_unix": time.time(),
            "updated_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    thread = threading.Thread(
        target=_run_engagement_send_job,
        kwargs={
            "campaign_id": campaign_id,
            "rows": rows,
            "template_map": template_map,
            "admin_email": admin_user.get("email") or "",
        },
        daemon=True,
    )
    thread.start()
    return JSONResponse(
        {
            "campaign_id": campaign_id,
            "total": total_ready,
            "ready": counts.get("ready", 0),
            "skipped": counts.get("skipped", 0),
        }
    )


@app.get("/admin/engagements/{campaign_id}/progress")
def admin_engagements_progress(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    _prune_engagement_send_progress()
    entry = _get_engagement_send_progress(campaign_id)
    if not entry:
        return JSONResponse({"status": "unknown"}, status_code=404)
    return JSONResponse(entry)


@app.get("/admin/collections-ai/{campaign_id}/progress")
def admin_collections_progress(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    _prune_engagement_send_progress()
    entry = _get_engagement_send_progress(campaign_id)
    if not entry:
        return JSONResponse({"status": "unknown"}, status_code=404)
    return JSONResponse(entry)


@app.get("/admin/engagements/{campaign_id}", response_class=HTMLResponse)
def admin_engagement_report(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE)
    campaign = _fetch_engagement_campaign(campaign_id)
    if not campaign:
        return PlainTextResponse("Campaign not found", status_code=404)
    if campaign.get("campaign_type") != config["campaign_type"]:
        base_path = _engagement_campaign_base_path(campaign.get("campaign_type"))
        return RedirectResponse(url=f"{base_path}/{campaign_id}", status_code=303)
    rows = _fetch_engagement_rows(campaign_id)
    sent_rows = [r for r in rows if r.get("send_status") == "sent"]
    sent_count = len(sent_rows)

    rows_out, responded, committed, ptp_count, engagement_window_days = _build_engagement_response_rows(
        rows,
        commitment_mode=config["commitment_mode"],
    )
    rows_out, kpi_summary, kpi_snapshot_dt = _build_engagement_kpi_report(rows_out)
    kpi_snapshot_label = kpi_snapshot_dt.strftime("%Y-%m-%d") if kpi_snapshot_dt else None
    status_map = _fetch_status_map_for_message_ids([r.get("message_id") for r in sent_rows if r.get("message_id")])
    read_count = sum(1 for entry in status_map.values() if entry.get("latest") == "read")
    for row in rows_out:
        if row.get("send_status") != "sent":
            continue
        message_id = row.get("message_id")
        if not message_id:
            continue
        status_entry = status_map.get(str(message_id))
        status_latest = status_entry.get("latest") if status_entry else None
        if status_latest:
            row["send_status"] = status_latest

    failed_count = sum(
        1
        for r in rows_out
        if (r.get("send_status") or "") in {"failed", "send_failed"}
        or (r.get("send_error") == "send_failed")
    )

    engagement_rate = (responded / sent_count * 100.0) if sent_count else 0.0
    commitment_rate = (committed / sent_count * 100.0) if sent_count else 0.0
    return templates.TemplateResponse(
        "admin_engagements_report.html",
        {
            "request": request,
            "admin": admin_user,
            "nav_active": config["nav_active"],
            "base_path": config["base_path"],
            "page_title": f"{config['page_title']} Report",
            "commitment_label": config["commitment_label"],
            "commitment_rate_label": config["commitment_rate_label"],
            "commitment_column_label": config["commitment_column_label"],
            "kpi_mode": config["kpi_mode"],
            "show_ptp": config["show_ptp"],
            "show_uplift": config["show_uplift"],
            "campaign": campaign,
            "rows": rows_out,
            "sent_count": sent_count,
            "failed_count": failed_count,
            "read_count": read_count,
            "responded_count": responded,
            "committed_count": committed,
            "engagement_rate": engagement_rate,
            "commitment_rate": commitment_rate,
            "engagement_window_days": engagement_window_days,
            "kpi_summary": kpi_summary,
            "kpi_snapshot": kpi_snapshot_label,
        },
    )


@app.get("/admin/collections-ai/{campaign_id}", response_class=HTMLResponse)
def admin_collections_report(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS)
    campaign = _fetch_engagement_campaign(campaign_id)
    if not campaign:
        return PlainTextResponse("Campaign not found", status_code=404)
    if campaign.get("campaign_type") != config["campaign_type"]:
        base_path = _engagement_campaign_base_path(campaign.get("campaign_type"))
        return RedirectResponse(url=f"{base_path}/{campaign_id}", status_code=303)
    rows = _fetch_engagement_rows(campaign_id)
    sent_rows = [r for r in rows if r.get("send_status") == "sent"]
    sent_count = len(sent_rows)

    rows_out, responded, committed, ptp_count, engagement_window_days = _build_engagement_response_rows(
        rows,
        commitment_mode=config["commitment_mode"],
    )
    rows_out, kpi_summary, kpi_snapshot_dt = _build_collections_kpi_report(rows_out)
    kpi_snapshot_label = kpi_snapshot_dt.strftime("%Y-%m-%d") if kpi_snapshot_dt else None
    status_map = _fetch_status_map_for_message_ids([r.get("message_id") for r in sent_rows if r.get("message_id")])
    read_count = sum(1 for entry in status_map.values() if entry.get("latest") == "read")
    for row in rows_out:
        if row.get("send_status") != "sent":
            continue
        message_id = row.get("message_id")
        if not message_id:
            continue
        status_entry = status_map.get(str(message_id))
        status_latest = status_entry.get("latest") if status_entry else None
        if status_latest:
            row["send_status"] = status_latest

    failed_count = sum(
        1
        for r in rows_out
        if (r.get("send_status") or "") in {"failed", "send_failed"}
        or (r.get("send_error") == "send_failed")
    )

    engagement_rate = (responded / sent_count * 100.0) if sent_count else 0.0
    commitment_rate = (committed / sent_count * 100.0) if sent_count else 0.0
    ptp_rate = (ptp_count / sent_count * 100.0) if sent_count else 0.0
    return templates.TemplateResponse(
        "admin_engagements_report.html",
        {
            "request": request,
            "admin": admin_user,
            "nav_active": config["nav_active"],
            "base_path": config["base_path"],
            "page_title": f"{config['page_title']} Report",
            "commitment_label": config["commitment_label"],
            "commitment_rate_label": config["commitment_rate_label"],
            "commitment_column_label": config["commitment_column_label"],
            "kpi_mode": config["kpi_mode"],
            "show_ptp": config["show_ptp"],
            "show_uplift": config["show_uplift"],
            "campaign": campaign,
            "rows": rows_out,
            "sent_count": sent_count,
            "failed_count": failed_count,
            "read_count": read_count,
            "responded_count": responded,
            "committed_count": committed,
            "ptp_count": ptp_count,
            "engagement_rate": engagement_rate,
            "commitment_rate": commitment_rate,
            "ptp_rate": ptp_rate,
            "engagement_window_days": engagement_window_days,
            "kpi_summary": kpi_summary,
            "kpi_snapshot": kpi_snapshot_label,
        },
    )


@app.get("/admin/engagements/{campaign_id}/uplift", response_class=HTMLResponse)
def admin_engagement_uplift(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE)
    campaign = _fetch_engagement_campaign(campaign_id)
    if not campaign:
        return PlainTextResponse("Campaign not found", status_code=404)
    if campaign.get("campaign_type") != config["campaign_type"]:
        base_path = _engagement_campaign_base_path(campaign.get("campaign_type"))
        return RedirectResponse(url=f"{base_path}/{campaign_id}", status_code=303)
    rows = _fetch_engagement_rows(campaign_id)
    sent_rows = [r for r in rows if r.get("send_status") == "sent"]
    rows_with_responses, _responded, _committed, _ptp_count, _window_days = _build_engagement_response_rows(
        sent_rows,
        commitment_mode=config["commitment_mode"],
    )
    status_map = _fetch_status_map_for_message_ids(
        [r.get("message_id") for r in rows_with_responses if r.get("message_id")]
    )
    for row in rows_with_responses:
        message_id = row.get("message_id")
        status_entry = status_map.get(str(message_id)) if message_id else None
        badges = status_entry.get("badges") if status_entry else []
        latest = status_entry.get("latest") if status_entry else None
        row["read"] = bool(latest == "read" or ("read" in (badges or [])))
    rows_out, uplift_summary = _build_engagement_uplift(rows_with_responses)
    lookback_days = int(os.getenv("ENGAGEMENT_UPLIFT_LOOKBACK_DAYS", "14"))
    lookahead_days = int(os.getenv("ENGAGEMENT_UPLIFT_LOOKAHEAD_DAYS", "14"))
    pre_count = sum(1 for r in rows_out if r.get("uplift_pre_date"))
    post_count = sum(1 for r in rows_out if r.get("uplift_post_date"))
    both_count = sum(
        1 for r in rows_out if r.get("uplift_pre_date") and r.get("uplift_post_date")
    )
    return templates.TemplateResponse(
        "admin_engagements_uplift.html",
        {
            "request": request,
            "admin": admin_user,
            "nav_active": config["nav_active"],
            "base_path": config["base_path"],
            "page_title": f"{config['page_title']} Uplift",
            "campaign": campaign,
            "rows": rows_out,
            "sent_count": len(sent_rows),
            "pre_count": pre_count,
            "post_count": post_count,
            "both_count": both_count,
            "uplift_summary": uplift_summary,
            "lookback_days": lookback_days,
            "lookahead_days": lookahead_days,
        },
    )


@app.get("/admin/engagements/{campaign_id}/export.csv")
def admin_engagement_report_csv(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_PERFORMANCE)
    campaign = _fetch_engagement_campaign(campaign_id)
    if not campaign:
        return PlainTextResponse("Campaign not found", status_code=404)
    if campaign.get("campaign_type") != config["campaign_type"]:
        base_path = _engagement_campaign_base_path(campaign.get("campaign_type"))
        return RedirectResponse(url=f"{base_path}/{campaign_id}/export.csv", status_code=303)
    rows = _fetch_engagement_rows(campaign_id)
    rows_out, _responded, _committed, _ptp_count, _window_days = _build_engagement_response_rows(
        rows,
        commitment_mode=config["commitment_mode"],
    )
    rows_out, _kpi_summary, _kpi_snapshot_dt = _build_engagement_kpi_report(rows_out)
    sent_rows = [r for r in rows_out if r.get("send_status") == "sent"]
    status_map = _fetch_status_map_for_message_ids(
        [r.get("message_id") for r in sent_rows if r.get("message_id")]
    )
    for row in sent_rows:
        message_id = row.get("message_id")
        if not message_id:
            continue
        status_entry = status_map.get(str(message_id))
        status_latest = status_entry.get("latest") if status_entry else None
        if status_latest:
            row["send_status"] = status_latest
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    def _cell(value: Any) -> Any:
        return "" if value is None else value
    header = [
        "WhatsApp",
        "Display Name",
        "Driver Type",
        "Template",
        "Send Status",
        "Send Error",
        "Message ID",
        "Sent At",
        "Follow-up Template",
        "Follow-up Status",
        "Follow-up Error",
        "Follow-up Sent At",
        "Responded",
        config["commitment_column_label"],
    ]
    if config["show_ptp"]:
        header.append("PTP")
    header += [
        "Response At",
        "Response Preview",
        "Baseline Online Hours",
        "Current Online Hours",
        "Delta Online Hours",
        "Online Hours Target Met",
        "Baseline Acceptance Rate",
        "Current Acceptance Rate",
        "Delta Acceptance Rate",
        "Acceptance Target Met",
        "Baseline Earnings/Hour",
        "Current Earnings/Hour",
        "Delta Earnings/Hour",
        "Earnings/Hour Target Met",
        "Baseline Trip Count",
        "Current Trip Count",
        "Delta Trip Count",
        "Trip Target Met",
        "Current KPI Date",
    ]
    writer.writerow(header)
    for row in rows_out:
        kpi = row.get("kpi_compare") or {}
        hours = kpi.get("online_hours") or {}
        acceptance = kpi.get("acceptance_rate") or {}
        eph = kpi.get("earnings_per_hour") or {}
        trips = kpi.get("trip_count") or {}
        kpi_current = row.get("kpi_current") or {}
        row_data = [
            row.get("wa_id"),
            row.get("display_name"),
            row.get("driver_type"),
            row.get("template_id"),
            row.get("send_status"),
            row.get("send_error"),
            row.get("message_id"),
            row.get("sent_at"),
            row.get("followup_template_id"),
            row.get("followup_status"),
            row.get("followup_error"),
            row.get("followup_sent_at"),
            "Yes" if row.get("responded") else "No",
            "Yes" if row.get("committed") else "No",
        ]
        if config["show_ptp"]:
            row_data.append("Yes" if row.get("ptp") else "No")
        row_data += [
            row.get("response_at") or "",
            row.get("response_preview") or "",
            _cell(hours.get("baseline")),
            _cell(hours.get("current")),
            _cell(hours.get("delta")),
            "Yes" if hours.get("target_met") else "No" if hours.get("target_met") is not None else "",
            _cell(acceptance.get("baseline")),
            _cell(acceptance.get("current")),
            _cell(acceptance.get("delta")),
            "Yes" if acceptance.get("target_met") else "No" if acceptance.get("target_met") is not None else "",
            _cell(eph.get("baseline")),
            _cell(eph.get("current")),
            _cell(eph.get("delta")),
            "Yes" if eph.get("target_met") else "No" if eph.get("target_met") is not None else "",
            _cell(trips.get("baseline")),
            _cell(trips.get("current")),
            _cell(trips.get("delta")),
            "Yes" if trips.get("target_met") else "No" if trips.get("target_met") is not None else "",
            kpi_current.get("report_date") or "",
        ]
        writer.writerow(row_data)
    csv_bytes = output.getvalue().encode("utf-8")
    filename = f"engagement_report_{campaign_id}.csv"
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )


@app.get("/admin/collections-ai/{campaign_id}/uplift", response_class=HTMLResponse)
def admin_collections_uplift(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS)
    campaign = _fetch_engagement_campaign(campaign_id)
    if not campaign:
        return PlainTextResponse("Campaign not found", status_code=404)
    if campaign.get("campaign_type") != config["campaign_type"]:
        base_path = _engagement_campaign_base_path(campaign.get("campaign_type"))
        return RedirectResponse(url=f"{base_path}/{campaign_id}", status_code=303)
    rows = _fetch_engagement_rows(campaign_id)
    sent_rows = [r for r in rows if r.get("send_status") == "sent"]
    rows_with_responses, _responded, _committed, _ptp_count, _window_days = _build_engagement_response_rows(
        sent_rows,
        commitment_mode=config["commitment_mode"],
    )
    status_map = _fetch_status_map_for_message_ids(
        [r.get("message_id") for r in rows_with_responses if r.get("message_id")]
    )
    for row in rows_with_responses:
        message_id = row.get("message_id")
        status_entry = status_map.get(str(message_id)) if message_id else None
        badges = status_entry.get("badges") if status_entry else []
        latest = status_entry.get("latest") if status_entry else None
        row["read"] = bool(latest == "read" or ("read" in (badges or [])))
    rows_out, uplift_summary = _build_engagement_uplift(rows_with_responses)
    lookback_days = int(os.getenv("ENGAGEMENT_UPLIFT_LOOKBACK_DAYS", "14"))
    lookahead_days = int(os.getenv("ENGAGEMENT_UPLIFT_LOOKAHEAD_DAYS", "14"))
    pre_count = sum(1 for r in rows_out if r.get("uplift_pre_date"))
    post_count = sum(1 for r in rows_out if r.get("uplift_post_date"))
    both_count = sum(
        1 for r in rows_out if r.get("uplift_pre_date") and r.get("uplift_post_date")
    )
    return templates.TemplateResponse(
        "admin_engagements_uplift.html",
        {
            "request": request,
            "admin": admin_user,
            "nav_active": config["nav_active"],
            "base_path": config["base_path"],
            "page_title": f"{config['page_title']} Uplift",
            "campaign": campaign,
            "rows": rows_out,
            "sent_count": len(sent_rows),
            "pre_count": pre_count,
            "post_count": post_count,
            "both_count": both_count,
            "uplift_summary": uplift_summary,
            "lookback_days": lookback_days,
            "lookahead_days": lookahead_days,
        },
    )


@app.get("/admin/collections-ai/{campaign_id}/export.csv")
def admin_collections_report_csv(request: Request, campaign_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    config = _get_engagement_page_config(ENGAGEMENT_CAMPAIGN_TYPE_COLLECTIONS)
    campaign = _fetch_engagement_campaign(campaign_id)
    if not campaign:
        return PlainTextResponse("Campaign not found", status_code=404)
    if campaign.get("campaign_type") != config["campaign_type"]:
        base_path = _engagement_campaign_base_path(campaign.get("campaign_type"))
        return RedirectResponse(url=f"{base_path}/{campaign_id}/export.csv", status_code=303)
    rows = _fetch_engagement_rows(campaign_id)
    rows_out, _responded, _committed, _ptp_count, _window_days = _build_engagement_response_rows(
        rows,
        commitment_mode=config["commitment_mode"],
    )
    rows_out, _kpi_summary, _kpi_snapshot_dt = _build_collections_kpi_report(rows_out)
    sent_rows = [r for r in rows_out if r.get("send_status") == "sent"]
    status_map = _fetch_status_map_for_message_ids(
        [r.get("message_id") for r in sent_rows if r.get("message_id")]
    )
    for row in sent_rows:
        message_id = row.get("message_id")
        if not message_id:
            continue
        status_entry = status_map.get(str(message_id))
        status_latest = status_entry.get("latest") if status_entry else None
        if status_latest:
            row["send_status"] = status_latest
    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    def _cell(value: Any) -> Any:
        return "" if value is None else value
    header = [
        "WhatsApp",
        "Display Name",
        "Driver Type",
        "Template",
        "Send Status",
        "Send Error",
        "Message ID",
        "Sent At",
        "Follow-up Template",
        "Follow-up Status",
        "Follow-up Error",
        "Follow-up Sent At",
        "Responded",
        config["commitment_column_label"],
    ]
    if config["show_ptp"]:
        header.append("PTP")
    header += [
        "Response At",
        "Response Preview",
        "Baseline Balance",
        "Current Balance",
        "Delta Balance",
        "Balance Cleared",
        "Baseline Payments (7d)",
        "Current Payments (7d)",
        "Delta Payments (7d)",
        "Payments Target Met",
        "Current Snapshot Date",
    ]
    writer.writerow(header)
    for row in rows_out:
        kpi = row.get("kpi_compare") or {}
        balance = kpi.get("balance") or {}
        payments_7d = kpi.get("payments_7d") or {}
        kpi_current = row.get("kpi_current") or {}
        row_data = [
            row.get("wa_id"),
            row.get("display_name"),
            row.get("driver_type"),
            row.get("template_id"),
            row.get("send_status"),
            row.get("send_error"),
            row.get("message_id"),
            row.get("sent_at"),
            row.get("followup_template_id"),
            row.get("followup_status"),
            row.get("followup_error"),
            row.get("followup_sent_at"),
            "Yes" if row.get("responded") else "No",
            "Yes" if row.get("committed") else "No",
        ]
        if config["show_ptp"]:
            row_data.append("Yes" if row.get("ptp") else "No")
        row_data += [
            row.get("response_at") or "",
            row.get("response_preview") or "",
            _cell(balance.get("baseline")),
            _cell(balance.get("current")),
            _cell(balance.get("delta")),
            "Yes" if balance.get("target_met") else "No" if balance.get("target_met") is not None else "",
            _cell(payments_7d.get("baseline")),
            _cell(payments_7d.get("current")),
            _cell(payments_7d.get("delta")),
            "Yes" if payments_7d.get("target_met") else "No" if payments_7d.get("target_met") is not None else "",
            kpi_current.get("report_date") or "",
        ]
        writer.writerow(row_data)
    csv_bytes = output.getvalue().encode("utf-8")
    filename = f"engagement_report_{campaign_id}.csv"
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename=\"{filename}\"'},
    )


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request):
    if get_authenticated_admin(request):
        return RedirectResponse(url="/admin/summary", status_code=303)
    error = request.query_params.get("error")
    return templates.TemplateResponse(
        "admin_login.html",
        {
            "request": request,
            "error": error,
        },
    )


@app.post("/admin/login")
async def admin_login_submit(request: Request, email: str = Form(...), password: str = Form(...)):
    user = get_admin_user_by_email(email)
    error = None
    if not user or not _verify_password(password, user.get("password_hash")):
        error = "Incorrect email or password."
    elif not user.get("is_active"):
        error = "Your account is disabled."
    if error:
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "error": error,
            },
            status_code=401,
        )
    response = RedirectResponse(url="/admin/summary", status_code=303)
    _set_admin_session_cookie(response, user["id"])
    return response


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)
    if not admin_can_manage_users(admin_user):
        return RedirectResponse(url="/admin/summary", status_code=303)
    error = request.query_params.get("error")
    success = request.query_params.get("success")
    users = fetch_admin_users()
    return templates.TemplateResponse(
        "admin_users.html",
        {
            "request": request,
            "admin": admin_user,
            "users": users,
            "error": error,
            "success": success,
            "nav_active": "users",
        },
    )


@app.post("/admin/users", response_class=HTMLResponse)
async def admin_users_create(request: Request, email: str = Form(...), password: str = Form(...)):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)
    if not admin_can_manage_users(admin_user):
        return RedirectResponse(url="/admin/summary", status_code=303)
    ok, msg = create_admin_user(email, password)
    if not ok:
        users = fetch_admin_users()
        return templates.TemplateResponse(
            "admin_users.html",
            {
                "request": request,
                "admin": admin_user,
                "users": users,
                "error": msg,
                "success": None,
                "nav_active": "users",
            },
            status_code=400,
        )
    return RedirectResponse(url="/admin/users?success=1", status_code=303)


@app.post("/admin/users/password", response_class=HTMLResponse)
async def admin_users_change_password(request: Request, email: str = Form(...), password: str = Form(...)):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)
    if not admin_can_manage_users(admin_user):
        return RedirectResponse(url="/admin/summary", status_code=303)
    ok, msg = change_admin_password(email, password)
    if not ok:
        users = fetch_admin_users()
        return templates.TemplateResponse(
            "admin_users.html",
            {
                "request": request,
                "admin": admin_user,
                "users": users,
                "error": msg,
                "success": None,
                "nav_active": "users",
            },
            status_code=400,
        )
    return RedirectResponse(url="/admin/users?success=1", status_code=303)


@app.post("/admin/users/manage", response_class=HTMLResponse)
async def admin_users_manage_flag(request: Request, email: str = Form(...), can_manage: str = Form("1")):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)
    if not admin_can_manage_users(admin_user):
        return RedirectResponse(url="/admin/summary", status_code=303)
    desired = can_manage == "1"
    ok, msg = update_admin_manage_flag(email, desired)
    if not ok:
        users = fetch_admin_users()
        return templates.TemplateResponse(
            "admin_users.html",
            {
                "request": request,
                "admin": admin_user,
                "users": users,
                "error": msg,
                "success": None,
                "nav_active": "users",
            },
            status_code=400,
        )
    return RedirectResponse(url="/admin/users?success=1", status_code=303)

# -----------------------------------------------------------------------------
# Driver portal (self-service statement/profile)
# -----------------------------------------------------------------------------
@app.get("/driver/login", response_class=HTMLResponse)
async def driver_login_page(request: Request):
    if get_authenticated_driver(request):
        return RedirectResponse(url="/driver/profile", status_code=303)
    error = request.query_params.get("error")
    logo_url = _logo_data_url()
    return templates.TemplateResponse("driver_login.html", {"request": request, "error": error, "mnc_logo_url": logo_url})


@app.post("/driver/login")
async def driver_login_submit(
    request: Request,
    personal_code: str = Form(...),
):
    error = None
    code = re.sub(r"\D", "", personal_code or "")
    if not (8 <= len(code) <= 16):
        error = "Personal code should be 8-16 digits."

    contact_ids: List[str] = []
    if not error:
        contact_ids = lookup_xero_contact_ids_by_personal_code(code)
        if not contact_ids:
            error = "Could not find an account for that personal code. Please check and try again."

    if error:
        return templates.TemplateResponse(
            "driver_login.html",
            {"request": request, "error": error, "personal_code": personal_code},
            status_code=400,
        )

    profile, _ = await asyncio.get_running_loop().run_in_executor(None, fetch_driver_profile_by_personal_code, code)
    profile = profile or {}
    wa_id = profile.get("wa_id")
    profile_name = profile.get("display_name") or "Driver"
    session_payload = {
        "wa_id": wa_id,
        "personal_code": code,
        "contact_ids": contact_ids,
        "account_id": contact_ids[0] if contact_ids else None,
        "profile_name": profile_name,
    }
    response = RedirectResponse(url="/driver/profile", status_code=303)
    _set_driver_session_cookie(response, session_payload)
    return response


@app.get("/driver/logout")
async def driver_logout():
    response = RedirectResponse(url="/driver/login", status_code=303)
    _clear_driver_session_cookie(response)
    return response


async def _load_driver_portal_data(
    session_data: Dict[str, Any],
    *,
    statement_limit: int,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Optional[str], str, Optional[str], Optional[str], Optional[Dict[str, str]]]:
    """Load profile + statement for a driver session."""
    wa_id = session_data.get("wa_id")
    personal_code = session_data.get("personal_code")
    loop = asyncio.get_running_loop()
    if wa_id:
        profile, _ = await loop.run_in_executor(None, fetch_driver_profile, wa_id)
    elif personal_code:
        profile, _ = await loop.run_in_executor(None, fetch_driver_profile_by_personal_code, personal_code)
    else:
        profile = None
    profile = profile or {}
    if not wa_id:
        wa_id = profile.get("wa_id") or _normalize_wa_id(profile.get("whatsapp_number") or profile.get("whatsapp") or "")
    if wa_id and not profile.get("wa_id"):
        profile["wa_id"] = wa_id
    if wa_id:
        driver_lookup = await loop.run_in_executor(None, lookup_driver_by_wa, wa_id)
    elif personal_code:
        driver_lookup = await loop.run_in_executor(None, lookup_driver_by_personal_code, personal_code)
    else:
        driver_lookup = None
    driver_lookup = driver_lookup or {}
    contact_ids: List[str] = []
    contact_ids.extend(session_data.get("contact_ids") or [])
    contact_ids.extend(profile.get("contact_ids") or [])
    contact_ids.extend(driver_lookup.get("xero_contact_ids") or [])
    contact_ids = [c for c in dict.fromkeys([str(c).strip() for c in contact_ids if c])]
    account_id = session_data.get("account_id")
    candidates: List[str] = []
    if account_id:
        candidates.append(account_id)
    candidates.extend(contact_ids)
    picked_id, account_statement, stmt_error = await loop.run_in_executor(None, _pick_statement_account_sync, candidates, statement_limit)
    account_statement_display = _calculate_running_balance(account_statement)
    display_name = profile.get("display_name") or session_data.get("profile_name") or driver_lookup.get("display_name") or "Driver"
    vehicle_model = (
        profile.get("asset_model")
        or profile.get("model")
        or driver_lookup.get("asset_model")
    )
    vehicle_reg = (
        profile.get("car_reg_number")
        or profile.get("vehicle")
        or profile.get("linked_vehicles")
        or profile.get("registration_number")
        or profile.get("reg_number")
        or driver_lookup.get("car_reg_number")
    )
    bank_details = bank_details_for_model(vehicle_model)
    personal_code = profile.get("personal_code") or driver_lookup.get("personal_code") or session_data.get("personal_code")
    return profile, account_statement_display, stmt_error, display_name, vehicle_model, vehicle_reg, bank_details, personal_code


@app.get("/driver/profile", response_class=HTMLResponse)
async def driver_profile(request: Request):
    session_data = get_authenticated_driver(request)
    if not session_data:
        return RedirectResponse(url="/driver/login", status_code=303)
    statement_limit = _sanitize_statement_limit(
        request.query_params.get("stmt_limit"),
        default=DRIVER_PORTAL_STATEMENT_LIMIT_DEFAULT,
        max_limit=DRIVER_PORTAL_STATEMENT_LIMIT_MAX,
    )
    statement_limit_next = min(
        DRIVER_PORTAL_STATEMENT_LIMIT_MAX,
        statement_limit + max(10, DRIVER_PORTAL_STATEMENT_LIMIT_STEP),
    )
    (
        profile,
        account_statement_display,
        stmt_error,
        display_name,
        vehicle_model,
        vehicle_reg,
        bank_details,
        personal_code,
    ) = await _load_driver_portal_data(session_data, statement_limit=statement_limit)
    wa_id = session_data.get("wa_id") or profile.get("wa_id")
    return templates.TemplateResponse(
        "driver_portal.html",
        {
            "request": request,
            "profile": profile,
            "display_name": display_name,
            "wa_id": wa_id,
            "account_statement_display": account_statement_display,
            "account_statement_error": stmt_error,
            "statement_limit": statement_limit,
            "statement_limit_next": statement_limit_next,
            "statement_limit_max": DRIVER_PORTAL_STATEMENT_LIMIT_MAX,
            "statement_limit_default": DRIVER_PORTAL_STATEMENT_LIMIT_DEFAULT,
            "format_rands": fmt_rands,
            "vehicle_model": vehicle_model,
            "vehicle_reg": vehicle_reg,
            "bank_details": bank_details,
            "personal_code": personal_code,
            "mnc_logo_url": MNC_LOGO_URL,
        },
    )


@app.get("/admin/orders", response_class=HTMLResponse)
def admin_orders_page(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    timeframe = (request.query_params.get("timeframe") or HOTSPOT_TIMEFRAME_DEFAULT).strip().lower()
    driver_name = (request.query_params.get("driver_name") or "").strip()
    asset_model = (request.query_params.get("asset_model") or "").strip()
    timeframe_options = [
        ("today", "Today"),
        ("this_week", "This week"),
        ("last_week", "Last week"),
        ("this_month", "This month"),
        ("last_month", "Last month"),
    ]
    allowed_timeframes = {val for val, _ in timeframe_options}
    if timeframe not in allowed_timeframes:
        timeframe = HOTSPOT_TIMEFRAME_DEFAULT

    asset_model_options = get_asset_model_options()
    if asset_model and asset_model not in asset_model_options:
        asset_model_options = [asset_model] + asset_model_options

    filter_ids: List[Any] = []
    filter_error: Optional[str] = None
    filter_count = 0
    if driver_name or asset_model:
        filter_ids, filter_error, filter_count = _resolve_orders_filter_ids(driver_name, asset_model)

    scope = "global"
    if filter_ids:
        scope = "driver"

    analytics = None
    error = None
    if not filter_error:
        analytics, error = get_orders_analytics(timeframe, filter_ids=filter_ids)
    hotspots_list, hotspots_reason, hotspots_label = get_hotspots(
        contact_ids=filter_ids, scope=scope, timeframe=timeframe, limit=5
    )
    times_list, times_reason, times_label = get_hotspot_times(
        contact_ids=filter_ids, scope=scope, timeframe=timeframe
    )
    top_oph_drivers: List[Dict[str, Any]] = []
    top_oph_reason: Optional[str] = None
    if not filter_error:
        top_oph_drivers, top_oph_reason = get_top_oph_drivers(timeframe, filter_ids=filter_ids, limit=10)
    else:
        top_oph_reason = filter_error

    cards: List[Dict[str, Any]] = []
    range_text = ""
    daily_rows: List[Dict[str, Any]] = []
    if analytics:
        def fmt_int(val: Optional[float]) -> str:
            if val is None:
                return "—"
            try:
                return f"{int(val):,}"
            except Exception:
                return "—"

        def fmt_pct(val: Optional[float]) -> str:
            if val is None:
                return "—"
            return f"{val:.0f}%"

        start_dt = analytics.get("range_start")
        end_dt = analytics.get("range_end")
        if start_dt and end_dt:
            try:
                display_end = end_dt - timedelta(seconds=1)
                range_text = f"{start_dt.strftime('%Y-%m-%d')} to {display_end.strftime('%Y-%m-%d')}"
            except Exception:
                range_text = ""

        cards.append({"label": "Orders", "value": fmt_int(analytics.get("trips_total"))})
        if analytics.get("finished_total") is not None:
            cards.append({"label": "Completed", "value": fmt_int(analytics.get("finished_total"))})
        if analytics.get("acceptance_rate") is not None:
            cards.append({"label": "Acceptance rate", "value": fmt_pct(analytics.get("acceptance_rate"))})
        cards.append({"label": "Total GMV", "value": fmt_rands(analytics.get("gmv_total") or 0.0)})
        if analytics.get("avg_gmv") is not None:
            cards.append({"label": "Avg GMV / order", "value": fmt_rands(analytics.get("avg_gmv"))})
        if analytics.get("cash_pct") is not None:
            cards.append({
                "label": "Cash vs Card",
                "value": f"{fmt_pct(analytics.get('cash_pct'))} cash",
                "sub": f"{fmt_pct(analytics.get('card_pct'))} card",
            })
        if analytics.get("avg_distance_km") is not None:
            cards.append({
                "label": "Avg distance",
                "value": f"{analytics.get('avg_distance_km'):.1f} km",
            })
        if analytics.get("avg_duration_min") is not None:
            cards.append({
                "label": "Avg duration",
                "value": f"{analytics.get('avg_duration_min'):.0f} min",
            })
        if analytics.get("avg_pickup_min") is not None:
            cards.append({
                "label": "Avg pickup",
                "value": f"{analytics.get('avg_pickup_min'):.0f} min",
            })

        daily_rows = analytics.get("daily") or []

    return templates.TemplateResponse(
        "admin_orders.html",
        {
            "request": request,
            "admin": admin_user,
            "nav_active": "orders",
            "timeframe": timeframe,
            "timeframe_options": timeframe_options,
            "driver_name": driver_name,
            "asset_model": asset_model,
            "asset_model_options": asset_model_options,
            "filter_count": filter_count,
            "range_text": range_text,
            "analytics": analytics,
            "analytics_error": filter_error or error,
            "cards": cards,
            "daily_rows": daily_rows,
            "hotspots": hotspots_list,
            "hotspots_reason": hotspots_reason,
            "hotspots_label": hotspots_label,
            "timeslots": times_list,
            "times_reason": times_reason,
            "times_label": times_label,
            "top_oph_drivers": top_oph_drivers,
            "top_oph_reason": top_oph_reason,
            "format_rands": fmt_rands,
        },
    )


@app.get("/driver/statement.pdf")
async def driver_statement_pdf(request: Request):
    session_data = get_authenticated_driver(request)
    if not session_data:
        return RedirectResponse(url="/driver/login", status_code=303)
    statement_limit = _sanitize_statement_limit(
        request.query_params.get("stmt_limit"),
        default=DRIVER_PORTAL_STATEMENT_EXPORT_LIMIT,
        max_limit=DRIVER_PORTAL_STATEMENT_LIMIT_MAX,
    )
    profile, account_statement_display, stmt_error, display_name, vehicle_model, vehicle_reg, bank_details, personal_code = await _load_driver_portal_data(
        session_data, statement_limit=statement_limit
    )
    if stmt_error:
        raise HTTPException(status_code=500, detail=f"Error fetching statement: {stmt_error}")
    meta = {
        "model": vehicle_model,
        "vehicle": vehicle_reg,
        "bank": bank_details,
        "reference": personal_code,
    }
    wa_id = session_data.get("wa_id") or profile.get("wa_id") or ""
    pdf_bytes = generate_statement_pdf(wa_id or "", display_name, account_statement_display or [], meta=meta)
    filename_id = wa_id or personal_code or "driver"
    filename = f"Statement_{display_name.replace(' ', '_')}_{filename_id}.pdf"
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )

@app.get("/driver/statement.csv")
async def driver_statement_csv(request: Request):
    session_data = get_authenticated_driver(request)
    if not session_data:
        return RedirectResponse(url="/driver/login", status_code=303)
    statement_limit = _sanitize_statement_limit(
        request.query_params.get("stmt_limit"),
        default=DRIVER_PORTAL_STATEMENT_EXPORT_LIMIT,
        max_limit=DRIVER_PORTAL_STATEMENT_LIMIT_MAX,
    )
    profile, account_statement_display, stmt_error, display_name, _, _, _, personal_code = await _load_driver_portal_data(
        session_data, statement_limit=statement_limit
    )
    if stmt_error:
        raise HTTPException(status_code=500, detail=f"Error fetching statement: {stmt_error}")
    import io as _io, csv
    output = _io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Reference", "Type", "Debit", "Credit", "Outstanding"])
    for row in account_statement_display or []:
        writer.writerow([
            str((row.get("date") or "")).replace(" 00:00:00", ""),
            row.get("reference") or "",
            row.get("source") or "",
            row.get("debit") or "",
            row.get("credit") or "",
            row.get("outstanding") or "",
        ])
    csv_bytes = output.getvalue().encode("utf-8")
    wa_id = session_data.get("wa_id") or profile.get("wa_id") or ""
    filename_id = wa_id or personal_code or "driver"
    filename = f"Statement_{display_name.replace(' ', '_')}_{filename_id}.csv"
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/admin/reset-password", response_class=HTMLResponse)
def admin_reset_password_page(request: Request):
    token = request.query_params.get("token")
    context = {
        "request": request,
        "error": None,
        "success": request.query_params.get("success"),
        "token_mode": False,
        "token": None,
        "reset_email": None,
        "info": None,
    }
    if token:
        user = resolve_password_reset_token(token)
        if user:
            context["token_mode"] = True
            context["token"] = token
            context["reset_email"] = user.get("email")
        else:
            context["error"] = "That reset link is invalid or has expired. Please request a new one."
    return templates.TemplateResponse("admin_reset_password.html", context)


@app.post("/admin/reset-password", response_class=HTMLResponse)
async def admin_reset_password_submit(
    request: Request,
    mode: str = Form("request"),
    email: Optional[str] = Form(None),
    token: Optional[str] = Form(None),
    new_password: Optional[str] = Form(None),
    confirm_password: Optional[str] = Form(None),
):
    context = {
        "request": request,
        "error": None,
        "success": None,
        "token_mode": False,
        "token": None,
        "reset_email": None,
        "info": None,
    }

    if mode == "reset":
        if not token:
            context["error"] = "Reset token missing. Please use the link from your email."
            return templates.TemplateResponse("admin_reset_password.html", context, status_code=400)
        user = resolve_password_reset_token(token)
        if not user:
            context["error"] = "That reset link is invalid or has expired. Request a new one below."
            return templates.TemplateResponse("admin_reset_password.html", context, status_code=400)
        context["token_mode"] = True
        context["token"] = token
        context["reset_email"] = user.get("email")
        if not new_password or not confirm_password:
            context["error"] = "Please provide and confirm a new password."
            return templates.TemplateResponse("admin_reset_password.html", context, status_code=400)
        if new_password != confirm_password:
            context["error"] = "Passwords do not match."
            return templates.TemplateResponse("admin_reset_password.html", context, status_code=400)
        if len(new_password) < 8:
            context["error"] = "Password must be at least 8 characters long."
            return templates.TemplateResponse("admin_reset_password.html", context, status_code=400)
        if not update_admin_password(user["id"], new_password):
            context["error"] = "Could not update the password right now. Please try again."
            return templates.TemplateResponse("admin_reset_password.html", context, status_code=500)
        context["token_mode"] = False
        context["token"] = None
        context["reset_email"] = None
        context["success"] = "Password updated. You can now sign in with the new credentials."
        return templates.TemplateResponse("admin_reset_password.html", context)

    # default: request mode (send email)
    normalized_email = _normalize_email(email)
    if not normalized_email:
        context["error"] = "Please provide a valid email."
        return templates.TemplateResponse("admin_reset_password.html", context, status_code=400)
    user = get_admin_user_by_email(normalized_email)
    generic_success = "If that email is on file, a password reset link is on its way."
    if not user or not user.get("is_active"):
        context["success"] = generic_success
        return templates.TemplateResponse("admin_reset_password.html", context)

    token = create_password_reset_token(user["id"], normalized_email)
    base_url = str(request.url_for("admin_reset_password_page"))
    separator = "&" if "?" in base_url else "?"
    reset_link = f"{base_url}{separator}token={token}"
    sent = send_password_reset_email(normalized_email, reset_link)
    if sent:
        context["success"] = generic_success
    else:
        context["error"] = (
            "Could not send the reset email because email delivery isn’t configured. "
            "Please contact an administrator."
        )
        context["info"] = "Reset link logged on the server for troubleshooting."
    return templates.TemplateResponse("admin_reset_password.html", context)


@app.get("/admin/logout")
async def admin_logout():
    response = RedirectResponse(url="/admin/login", status_code=303)
    _clear_admin_session_cookie(response)
    return response


@app.get("/admin/whatsapp/templates")
async def admin_list_whatsapp_templates(request: Request):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    return JSONResponse(get_whatsapp_templates())


@app.post("/admin/drivers/{wa_id}/whatsapp/send", response_class=HTMLResponse)
async def admin_send_driver_whatsapp(
    request: Request,
    wa_id: str,
    template_id: str = Form(...),
    language: str = Form("en"),
    variables: str = Form(""),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    tmpl = next((t for t in get_whatsapp_templates() if t.get("id") == template_id), None)
    if not tmpl:
        return RedirectResponse(url=f"/admin/drivers/{wa_id}?wa_error=Unknown+template", status_code=303)
    try:
        body_params = []
        if variables:
            parsed = json.loads(variables)
            if isinstance(parsed, list):
                body_params = parsed
            elif isinstance(parsed, dict):
                if str(tmpl.get("parameter_format", "")).upper() == "NAMED":
                    body_params = parsed
                else:
                    # keep order of provided values
                    body_params = [parsed[k] for k in sorted(parsed.keys())]
    except Exception:
        return RedirectResponse(url=f"/admin/drivers/{wa_id}?wa_error=Invalid+variables+JSON", status_code=303)

    # Attachment deliberately not sent for templates to avoid upload failures here.
    outbound_id = send_whatsapp_template(
        wa_id,
        template_id,
        language or tmpl.get("language") or "en",
        body_params,
        None,
        param_names=tmpl.get("variables"),
        parameter_format=tmpl.get("parameter_format"),
    )
    if not outbound_id:
        return RedirectResponse(url=f"/admin/drivers/{wa_id}?wa_error=Send+failed", status_code=303)
    _record_outbound_template_context(
        wa_id,
        template_id,
        body_params,
        param_names=tmpl.get("variables"),
        parameter_format=tmpl.get("parameter_format"),
    )
    log_interaction(
        wa_id,
        channel="whatsapp",
        template_id=template_id,
        variables_json=json.dumps(body_params or []),
        admin_email=admin_user.get("email"),
        status="sent",
    )
    log_message(
        direction="OUTBOUND",
        wa_id=wa_id,
        text=f"template:{template_id}",
        intent=None,
        status="sent",
        wa_message_id=outbound_id,
        message_id=outbound_id,
        business_number=None,
        phone_number_id=None,
        origin_type="admin_console",
        raw_json={"template_id": template_id, "variables": body_params or []},
        timestamp_unix=str(int(time.time())),
    )
    return RedirectResponse(url=f"/admin/drivers/{wa_id}?wa_sent=1", status_code=303)


@app.post("/admin/drivers/{wa_id}/email/send", response_class=HTMLResponse)
async def admin_send_driver_email(
    request: Request,
    wa_id: str,
    subject: str = Form(...),
    message: str = Form(...),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    profile, _ = fetch_driver_profile(wa_id)
    recipient = (profile or {}).get("email")
    if not recipient:
        return RedirectResponse(url=f"/admin/drivers/{wa_id}?email_error=No+email+for+this+driver", status_code=303)
    ok = send_generic_email(recipient, subject or "Update", message or "")
    if not ok:
        return RedirectResponse(url=f"/admin/drivers/{wa_id}?email_error=Email+failed", status_code=303)
    log_interaction(
        wa_id,
        channel="email",
        template_id=None,
        variables_json=None,
        admin_email=admin_user.get("email"),
        status="sent",
    )
    return RedirectResponse(url=f"/admin/drivers/{wa_id}?email_sent=1", status_code=303)


@app.post("/admin/drivers/{wa_id}/call", response_class=HTMLResponse)
async def admin_call_driver(
    request: Request,
    wa_id: str,
    call_mode: Optional[str] = Form(None),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    profile, _ = fetch_driver_profile(wa_id)
    to_number = (profile or {}).get("wa_id") or wa_id
    if not to_number:
        return RedirectResponse(url=f"/admin/drivers/{wa_id}?call_error=No+number+available", status_code=303)
    mode = (call_mode or "").strip().lower()
    if mode != "softphone":
        ok = trigger_click_to_dial(to_number)
        if not ok:
            return RedirectResponse(url=f"/admin/drivers/{wa_id}?call_error=Call+initiation+failed", status_code=303)
    log_interaction(
        wa_id,
        channel="call",
        admin_email=admin_user.get("email"),
        status="sent",
    )
    return RedirectResponse(url=f"/admin/drivers/{wa_id}?call_sent=1", status_code=303)


@app.post("/admin/drivers/{wa_id}/ptp", response_class=HTMLResponse)
async def admin_save_ptp(
    request: Request,
    wa_id: str,
    amount: Optional[float] = Form(None),
    ptp_date: Optional[str] = Form(None),
    note: Optional[str] = Form(None),
    payment: Optional[float] = Form(None),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    status = "failed"
    if amount and payment:
        try:
            amt = float(amount)
            pay = float(payment)
            if pay >= amt and amt > 0:
                status = "completed"
            elif pay > 0:
                status = "partial"
            else:
                status = "failed"
        except Exception:
            status = "failed"
    log_interaction(
        wa_id,
        channel="ptp",
        amount=amount,
        ptp_date=ptp_date,
        ptp_payment=payment,
        variables_json=note,
        admin_email=admin_user.get("email"),
        status=status,
    )
    return RedirectResponse(url=f"/admin/drivers/{wa_id}?ptp_saved=1", status_code=303)

@app.get("/admin/drivers", response_class=HTMLResponse)
def admin_driver_directory(
    request: Request,
    name: Optional[str] = None,
    model: Optional[str] = None,
    reg: Optional[str] = None,
    phone: Optional[str] = None,
    collections_agent: Optional[str] = None,
    status: Optional[str] = None,
    driver_type: Optional[str] = None,
    payer_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    sanitized_limit = 50  # fixed page size
    sanitized_offset = max(0, offset)
    driver_filter_query = _build_driver_filter_query(
        name=name,
        model=model,
        reg=reg,
        phone=phone,
        collections_agent=collections_agent,
        status=status,
        driver_type=driver_type,
        payer_type=payer_type,
        limit=sanitized_limit,
        offset=sanitized_offset,
    )
    driver_export_query = _build_driver_filter_query(
        name=name,
        model=model,
        reg=reg,
        phone=phone,
        collections_agent=collections_agent,
        status=status,
        driver_type=driver_type,
        payer_type=payer_type,
    )
    drivers, error, drivers_total, collections_options, status_options, driver_type_options, payer_type_options = fetch_active_driver_profiles(
        name=name,
        model=model,
        reg=reg,
        phone=phone,
        collections_agent=collections_agent,
        status=status,
        driver_type=driver_type,
        payer_type=payer_type,
        limit=sanitized_limit,
        offset=sanitized_offset,
    )
    model_options: List[str] = []
    roster_drivers, roster_error, _roster_collections, _roster_driver_types, _roster_payer_types = _load_cached_driver_roster(0)
    if not roster_error and roster_drivers:
        model_options = sorted(
            {str(d.get("model")).strip() for d in roster_drivers if d.get("model")}
        )
    if not model_options:
        model_options = get_asset_model_options()
    if not model_options and drivers:
        model_options = sorted(
            {str(d.get("model")).strip() for d in drivers if d.get("model")}
        )
    if model and model not in model_options:
        model_options = [model] + model_options
    showing_start = sanitized_offset + 1 if drivers_total else 0
    showing_end = sanitized_offset + len(drivers)
    has_more = showing_end < drivers_total
    return templates.TemplateResponse(
        "admin_drivers.html",
        {
            "request": request,
            "admin": admin_user,
            "drivers": drivers,
            "name": name or "",
            "model": model or "",
            "reg": reg or "",
            "phone": phone or "",
            "collections_agent": collections_agent or "",
            "status": status or "",
            "driver_type": driver_type or "",
            "payer_type": payer_type or "",
            "limit": sanitized_limit,
            "offset": sanitized_offset,
            "nav_active": "drivers",
            "db_error": error,
            "drivers_total": drivers_total,
            "showing_start": showing_start,
            "showing_end": showing_end,
            "has_more": has_more,
            "format_rands": fmt_rands,
            "driver_filter_query": driver_filter_query,
            "driver_export_query": driver_export_query,
            "collections_options": collections_options,
            "status_options": status_options,
            "driver_type_options": driver_type_options,
            "payer_type_options": payer_type_options,
            "model_options": model_options,
        },
    )


@app.get("/admin/drivers/export.csv")
def admin_driver_directory_csv(
    request: Request,
    name: Optional[str] = None,
    model: Optional[str] = None,
    reg: Optional[str] = None,
    phone: Optional[str] = None,
    collections_agent: Optional[str] = None,
    status: Optional[str] = None,
    driver_type: Optional[str] = None,
    payer_type: Optional[str] = None,
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    drivers, error, _total, *_rest = fetch_active_driver_profiles(
        name=name,
        model=model,
        reg=reg,
        phone=phone,
        collections_agent=collections_agent,
        status=status,
        driver_type=driver_type,
        payer_type=payer_type,
        paginate=False,
    )
    if error:
        raise HTTPException(status_code=500, detail=error)

    def _format_cell(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, list):
            return "; ".join([str(v) for v in value if v not in (None, "")])
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    def _driver_reg(driver: Dict[str, Any]) -> str:
        return (
            driver.get("car_reg_number")
            or driver.get("vehicle")
            or driver.get("reg_number")
            or driver.get("registration_number")
            or driver.get("linked_vehicles")
            or ""
        )

    columns = [
        ("Display Name", "display_name"),
        ("WhatsApp", "wa_id"),
        ("Status", "status"),
        ("Model", "model"),
        ("Car Reg", _driver_reg),
        ("Contract Start Date", "contract_start_date"),
        ("Collections Agent", "collections_agent"),
        ("Driver Type", "efficiency_badge_label"),
        ("Payer Type", "payer_badge_label"),
        ("Online Hours", "online_hours"),
        ("Acceptance Rate", "acceptance_rate"),
        ("Gross Earnings", "gross_earnings"),
        ("Earnings Per Hour", "earnings_per_hour"),
        ("Xero Balance", "xero_balance"),
        ("Payments", "payments"),
        ("Bolt Wallet Payouts", "bolt_wallet_payouts"),
        ("Yesterday Wallet Balance", "yday_wallet_balance"),
        ("Trip Count", "trip_count"),
        ("Last Synced At", "last_synced_at"),
        ("Contact IDs", "contact_ids"),
    ]

    output = io.StringIO()
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow([header for header, _ in columns])
    for driver in drivers:
        row = []
        for _header, accessor in columns:
            if callable(accessor):
                value = accessor(driver)
            else:
                value = driver.get(accessor)
            row.append(_format_cell(value))
        writer.writerow(row)

    csv_bytes = output.getvalue().encode("utf-8")
    stamp = jhb_now().strftime("%Y%m%d_%H%M%S")
    filename = f"drivers_filtered_{stamp}.csv"
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/admin/drivers/scroll")
def admin_driver_scroll(
    request: Request,
    name: Optional[str] = None,
    model: Optional[str] = None,
    reg: Optional[str] = None,
    phone: Optional[str] = None,
    collections_agent: Optional[str] = None,
    status: Optional[str] = None,
    driver_type: Optional[str] = None,
    payer_type: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return JSONResponse(status_code=401, content={"error": "Authentication required."})
    sanitized_limit = min(max(10, limit), 50) if limit else 50
    sanitized_offset = max(0, offset)
    driver_filter_query = _build_driver_filter_query(
        name=name,
        model=model,
        reg=reg,
        phone=phone,
        collections_agent=collections_agent,
        status=status,
        driver_type=driver_type,
        payer_type=payer_type,
        limit=sanitized_limit,
        offset=sanitized_offset,
    )
    drivers, error, total, _collections_options, _status_options, _driver_type_options, _payer_type_options = fetch_active_driver_profiles(
        name=name,
        model=model,
        reg=reg,
        phone=phone,
        collections_agent=collections_agent,
        status=status,
        driver_type=driver_type,
        payer_type=payer_type,
        limit=sanitized_limit,
        offset=sanitized_offset,
    )
    if error:
        return JSONResponse(status_code=500, content={"error": error})
    try:
        cards_template = templates.env.get_template("_admin_driver_cards.html")
        html = cards_template.render(
            drivers=drivers,
            format_rands=fmt_rands,
            driver_filter_query=driver_filter_query,
        )
    except Exception as exc:
        log.error("admin_driver_scroll failed to render cards: %s", exc)
        html = ""
    next_offset = sanitized_offset + len(drivers)
    return JSONResponse(
        {
            "html": html,
            "next_offset": next_offset,
            "has_more": next_offset < total,
            "total": total,
        }
    )

def _calculate_running_balance(statement_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not statement_rows:
        return []

    # Xero statements are typically sorted DESC by date. We need to process them 
    # from oldest (bottom) to newest (top) to calculate a running balance.
    sorted_asc = list(statement_rows)
    sorted_asc.reverse()

    running_balance = 0.0
    processed_statement = []
    
    # Process from oldest to newest
    for row in sorted_asc:
        # Debits increase the balance (amount owed)
        debit = float(row.get("debit") or 0.0)
        # Credits decrease the balance (payment received)
        credit = float(row.get("credit") or 0.0)
        
        running_balance += debit
        running_balance -= credit
        
        row_copy = dict(row)
        row_copy["outstanding"] = running_balance
        
        processed_statement.append(row_copy)

    # Return in DESC order for display
    processed_statement.reverse()
    return processed_statement

@app.get("/admin/drivers/{wa_id}/statement.pdf")
async def admin_driver_statement_download(request: Request, wa_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    loop = asyncio.get_running_loop()

    # --- 1. Resolve Driver and Account ID ---
    # Fetch data needed to identify the account ID and driver name.
    roster_driver = await asyncio.get_running_loop().run_in_executor(None, _get_cached_roster_driver, wa_id)
    driver_lookup = await asyncio.get_running_loop().run_in_executor(None, lookup_driver_by_wa, wa_id) if roster_driver is None else roster_driver
    profile, _ = await asyncio.get_running_loop().run_in_executor(None, fetch_driver_profile, wa_id)
    profile = profile or {}

    def _first_id(source: Dict[str, Any]) -> Optional[str]:
        if not source: return None
        for key in ["account_id", "xero_contact_id", "contact_id", "xero_contact_id_bjj", "xero_contact_id_hakki", "xero_contact_id_a49"]:
            val = source.get(key)
            if val: return str(val)
        ids = source.get("xero_contact_ids") or []
        if ids: return str(ids[0])
        return None

    account_id = None
    if roster_driver:
        account_id = _first_id(roster_driver)
    if not account_id and driver_lookup:
        account_id = _first_id(driver_lookup)
    if not account_id and profile:
        ids = profile.get("contact_ids") or []
        if ids: account_id = str(ids[0])

    contact_ids: List[str] = []
    if profile:
        contact_ids.extend(profile.get("contact_ids") or [])
    if driver_lookup:
        contact_ids.extend(driver_lookup.get("xero_contact_ids") or [])
    if roster_driver:
        contact_ids.extend(roster_driver.get("contact_ids") or [])
    contact_ids = [c for c in dict.fromkeys([str(c).strip() for c in contact_ids if c])]

    # Try to align with the balance contact and pick the first account that returns rows.
    if contact_ids:
        balance_info = await asyncio.get_running_loop().run_in_executor(None, get_latest_xero_outstanding, contact_ids)
        balance_contact = None
        if balance_info and balance_info.get("contact_id") and balance_info.get("outstanding") is not None:
            balance_contact = str(balance_info["contact_id"])
            account_id = balance_contact
    else:
        balance_contact = None

    candidates: List[str] = []
    if balance_contact:
        candidates.append(balance_contact)
    candidates.extend(contact_ids)
    if account_id:
        candidates.append(account_id)
    statement_limit = _sanitize_statement_limit(
        request.query_params.get("stmt_limit"),
        default=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
        max_limit=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
    )
    picked_account_id, account_statement, error = await loop.run_in_executor(None, _pick_statement_account_sync, candidates, statement_limit)
    if picked_account_id:
        account_id = picked_account_id

    if not account_id:
        raise HTTPException(status_code=404, detail="Account ID not found for driver statement.")
    
    # --- 2. Fetch and calculate statement ---
    if error:
        raise HTTPException(status_code=500, detail=f"Error fetching statement: {error}")
    
    # Calculate running balance (requires the statement to be fetched first)
    statement_rows_display = _calculate_running_balance(account_statement)
    display_name = (profile.get("display_name") or driver_lookup.get("display_name") or "Driver").strip()
    model = profile.get("asset_model") or profile.get("model") or driver_lookup.get("asset_model")
    vehicle_reg = (
        profile.get("car_reg_number")
        or profile.get("vehicle")
        or profile.get("linked_vehicles")
        or profile.get("registration_number")
        or profile.get("reg_number")
        or driver_lookup.get("car_reg_number")
    )
    bank_details = bank_details_for_model(model)
    reference = profile.get("personal_code") or driver_lookup.get("personal_code")
    meta = {"model": model, "vehicle": vehicle_reg, "bank": bank_details, "reference": reference}
    
    # --- 3. Generate the PDF ---
    pdf_bytes = generate_statement_pdf(wa_id, display_name, statement_rows_display, meta=meta)

    # --- 4. Return as StreamingResponse ---
    filename = f"Statement_{display_name.replace(' ', '_')}_{wa_id}.pdf"
    
    return StreamingResponse(
        io.BytesIO(pdf_bytes),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )

@app.get("/admin/drivers/{wa_id}/statement.csv")
async def admin_driver_statement_csv(request: Request, wa_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    loop = asyncio.get_running_loop()

    roster_driver = await asyncio.get_running_loop().run_in_executor(None, _get_cached_roster_driver, wa_id)
    driver_lookup = await asyncio.get_running_loop().run_in_executor(None, lookup_driver_by_wa, wa_id) if roster_driver is None else roster_driver
    profile, _ = await asyncio.get_running_loop().run_in_executor(None, fetch_driver_profile, wa_id)
    profile = profile or {}

    def _first_id(source: Dict[str, Any]) -> Optional[str]:
        if not source: return None
        for key in ["account_id", "xero_contact_id", "contact_id", "xero_contact_id_bjj", "xero_contact_id_hakki", "xero_contact_id_a49"]:
            val = source.get(key)
            if val: return str(val)
        ids = source.get("xero_contact_ids") or []
        if ids: return str(ids[0])
        return None

    account_id = None
    if roster_driver:
        account_id = _first_id(roster_driver)
    if not account_id and driver_lookup:
        account_id = _first_id(driver_lookup)
    if not account_id and profile:
        ids = profile.get("contact_ids") or []
        if ids: account_id = str(ids[0])

    contact_ids: List[str] = []
    if profile:
        contact_ids.extend(profile.get("contact_ids") or [])
    if driver_lookup:
        contact_ids.extend(driver_lookup.get("xero_contact_ids") or [])
    if roster_driver:
        contact_ids.extend(roster_driver.get("contact_ids") or [])
    contact_ids = [c for c in dict.fromkeys([str(c).strip() for c in contact_ids if c])]

    balance_contact = None
    if contact_ids:
        balance_info = await asyncio.get_running_loop().run_in_executor(None, get_latest_xero_outstanding, contact_ids)
        if balance_info and balance_info.get("contact_id") and balance_info.get("outstanding") is not None:
            balance_contact = str(balance_info["contact_id"])
            account_id = balance_contact

    candidates: List[str] = []
    if balance_contact:
        candidates.append(balance_contact)
    candidates.extend(contact_ids)
    if account_id:
        candidates.append(account_id)
    statement_limit = _sanitize_statement_limit(
        request.query_params.get("stmt_limit"),
        default=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
        max_limit=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
    )
    picked_account_id, account_statement, error = await loop.run_in_executor(None, _pick_statement_account_sync, candidates, statement_limit)
    if picked_account_id:
        account_id = picked_account_id

    if not account_id:
        raise HTTPException(status_code=404, detail="Account ID not found for driver statement.")
    if error:
        raise HTTPException(status_code=500, detail=f"Error fetching statement: {error}")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Reference", "Type", "Debit", "Credit", "Outstanding"])
    for row in _calculate_running_balance(account_statement):
        writer.writerow([
            str((row.get("date") or "")).replace(" 00:00:00", ""),
            row.get("reference") or "",
            row.get("source") or "",
            row.get("debit") or "",
            row.get("credit") or "",
            row.get("outstanding") or "",
        ])
    csv_bytes = output.getvalue().encode("utf-8")
    display_name = (profile.get("display_name") or driver_lookup.get("display_name") or "Driver").strip()
    filename = f"Statement_{display_name.replace(' ', '_')}_{wa_id}.csv"
    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/admin/drivers/{wa_id}/statement/section", response_class=HTMLResponse, name="admin_driver_statement_section")
async def admin_driver_statement_section(request: Request, wa_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    statement_limit = _sanitize_statement_limit(
        request.query_params.get("stmt_limit"),
        default=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_DEFAULT,
        max_limit=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
    )
    statement_limit_next = min(
        ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
        statement_limit + max(10, ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_STEP),
    )

    detail = _get_cached_driver_detail(wa_id) or {}
    contact_ids = detail.get("contact_ids") or []
    balance_contact = detail.get("balance_contact_id")
    account_id = detail.get("account_id_used")

    if not contact_ids and not balance_contact and not account_id:
        loop = asyncio.get_running_loop()
        roster_driver = _get_cached_roster_driver(wa_id)
        profile, _ = await loop.run_in_executor(None, fetch_driver_profile, wa_id)
        profile = profile or {}
        driver_lookup = await loop.run_in_executor(None, lookup_driver_by_wa, wa_id) if roster_driver is None else roster_driver
        driver_lookup = driver_lookup or {}
        contact_ids = []
        contact_ids.extend(profile.get("contact_ids") or [])
        contact_ids.extend(driver_lookup.get("xero_contact_ids") or [])
        if roster_driver:
            contact_ids.extend(roster_driver.get("contact_ids") or [])
        contact_ids = [c for c in dict.fromkeys([str(c).strip() for c in contact_ids if c])]
        if contact_ids:
            balance_info = await loop.run_in_executor(None, get_latest_xero_outstanding, contact_ids)
            if balance_info and balance_info.get("contact_id") and balance_info.get("outstanding") is not None:
                balance_contact = str(balance_info.get("contact_id"))
                account_id = balance_contact
        detail.setdefault("contact_ids", contact_ids)
        if balance_contact:
            detail["balance_contact_id"] = balance_contact
        if account_id:
            detail["account_id_used"] = account_id
        _set_cached_driver_detail(wa_id, detail)

    candidates: List[str] = []
    if balance_contact:
        candidates.append(str(balance_contact))
    candidates.extend([str(c).strip() for c in (contact_ids or []) if c])
    if account_id:
        candidates.append(str(account_id))

    loop = asyncio.get_running_loop()
    picked_id, account_statement, account_statement_error = await loop.run_in_executor(
        None, _pick_statement_account_sync, candidates, statement_limit
    )
    if picked_id:
        detail["account_id_used"] = picked_id
    detail["account_statement"] = account_statement
    detail["account_statement_error"] = account_statement_error
    detail["statement_limit"] = statement_limit
    _set_cached_driver_detail(wa_id, detail)

    account_statement_display = _calculate_running_balance(account_statement or [])
    statement_section_url = str(request.url_for("admin_driver_statement_section", wa_id=wa_id))
    return templates.TemplateResponse(
        "_admin_statement_section.html",
        {
            "request": request,
            "wa_id": wa_id,
            "format_rands": fmt_rands,
            "account_statement_display": account_statement_display,
            "account_statement_error": account_statement_error,
            "statement_limit": statement_limit,
            "statement_limit_next": statement_limit_next,
            "statement_limit_max": ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
            "statement_limit_default": ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_DEFAULT,
            "statement_section_url": statement_section_url,
        },
    )


@app.get("/admin/drivers/{wa_id}/kpis/today/section", response_class=HTMLResponse, name="admin_driver_today_kpis_section")
async def admin_driver_today_kpis_section(request: Request, wa_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    detail = _get_cached_driver_detail(wa_id) or {}
    driver_lookup = detail.get("driver_lookup") or _get_cached_roster_driver(wa_id) or {}
    contact_ids = detail.get("contact_ids") or driver_lookup.get("xero_contact_ids") or []

    # Fetch a fresh set of KPIs so today's aggregates reflect the DB, even if the 7d roster cache is present.
    loop = asyncio.get_running_loop()

    def _fetch_kpis():
        return get_driver_kpis(wa_id, driver_lookup, use_cache=False, include_today=True)

    metrics, reason = await loop.run_in_executor(None, _fetch_kpis)
    today_kpi_sections = _build_today_kpi_sections(metrics or {})

    today_kpi_error = None
    if metrics is None and reason:
        today_kpi_error = reason
    elif not today_kpi_sections and contact_ids:
        # If KPI query returned but no today's fields populated, surface a softer message.
        today_kpi_error = None

    return templates.TemplateResponse(
        "_admin_today_kpis_section.html",
        {
            "request": request,
            "wa_id": wa_id,
            "today_kpi_sections": today_kpi_sections,
            "today_kpi_error": today_kpi_error,
        },
    )


@app.get("/admin/drivers/{wa_id}/snapshot/section", response_class=HTMLResponse, name="admin_driver_snapshot_section")
async def admin_driver_snapshot_section(request: Request, wa_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    detail = _get_cached_driver_detail(wa_id) or {}
    profile = detail.get("profile") or {}
    driver_lookup = detail.get("driver_lookup")
    roster_driver = _get_cached_roster_driver(wa_id)

    loop = asyncio.get_running_loop()

    if not driver_lookup:
        if roster_driver:
            driver_lookup = roster_driver
        elif profile:
            driver_lookup = profile
        else:
            driver_lookup = await loop.run_in_executor(None, lookup_driver_by_wa, wa_id)
        detail["driver_lookup"] = driver_lookup

    driver_ref = driver_lookup or roster_driver or profile or {}

    def _fetch_kpis():
        return get_driver_kpis(wa_id, driver_ref, include_today=False)

    kpi_metrics, kpi_error = await loop.run_in_executor(None, _fetch_kpis)

    statement_outstanding = None
    statement_rows = detail.get("account_statement") or []
    if statement_rows:
        calculated = _calculate_running_balance(statement_rows)
        if calculated:
            statement_outstanding = calculated[0].get("outstanding")

    if statement_outstanding is None:
        candidates: List[str] = []
        balance_contact = detail.get("balance_contact_id")
        account_id = detail.get("account_id_used")
        contact_ids = detail.get("contact_ids") or []
        if balance_contact:
            candidates.append(str(balance_contact))
        candidates.extend([str(c).strip() for c in contact_ids if c])
        if account_id:
            candidates.append(str(account_id))
        if candidates:
            stmt_limit = detail.get("statement_limit") or ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_DEFAULT
            picked_id, stmt_rows, stmt_error = await loop.run_in_executor(
                None, _pick_statement_account_sync, candidates, stmt_limit
            )
            if picked_id:
                detail["account_id_used"] = picked_id
            if stmt_rows is not None:
                detail["account_statement"] = stmt_rows
                detail["account_statement_error"] = stmt_error
                detail["statement_limit"] = stmt_limit
            if stmt_rows:
                calculated = _calculate_running_balance(stmt_rows)
                if calculated:
                    statement_outstanding = calculated[0].get("outstanding")

    if kpi_metrics and statement_outstanding is not None:
        kpi_metrics["xero_balance"] = statement_outstanding

    detail["kpi_metrics"] = kpi_metrics
    detail["kpi_error"] = kpi_error
    _set_cached_driver_detail(wa_id, detail)

    return templates.TemplateResponse(
        "_admin_driver_snapshot_section.html",
        {
            "request": request,
            "wa_id": wa_id,
            "kpi_metrics": kpi_metrics,
            "kpi_error": kpi_error,
            "format_rands": fmt_rands,
        },
    )


@app.get("/admin/drivers/{wa_id}/kpis/trend/section", response_class=HTMLResponse, name="admin_driver_kpi_trend_section")
async def admin_driver_kpi_trend_section(request: Request, wa_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    detail = _get_cached_driver_detail(wa_id) or {}
    driver_lookup = detail.get("driver_lookup") or _get_cached_roster_driver(wa_id) or {}
    if not driver_lookup:
        driver_lookup = await asyncio.get_running_loop().run_in_executor(None, lookup_driver_by_wa, wa_id)

    days = ADMIN_DRIVER_DETAIL_KPI_TREND_DAYS
    loop = asyncio.get_running_loop()

    def _fetch_trend():
        return _fetch_driver_kpi_trend(wa_id, driver_lookup, days=days)

    trend_rows, trend_error = await loop.run_in_executor(None, _fetch_trend)

    return templates.TemplateResponse(
        "_admin_driver_kpi_trend_section.html",
        {
            "request": request,
            "wa_id": wa_id,
            "trend_rows": trend_rows,
            "trend_error": trend_error,
            "trend_days": days,
        },
    )


@app.get("/admin/drivers/{wa_id}", response_class=HTMLResponse)
async def admin_driver_detail(request: Request, wa_id: str):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()

    timings: list[tuple[str, float]] = []

    def _t_start() -> float:
        return time.perf_counter()

    def _t_end(label: str, started: float) -> None:
        if not ADMIN_DRIVER_DETAIL_TIMING:
            return
        try:
            timings.append((label, (time.perf_counter() - started) * 1000.0))
        except Exception:
            pass

    statement_limit = _sanitize_statement_limit(
        request.query_params.get("stmt_limit"),
        default=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_DEFAULT,
        max_limit=ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
    )
    statement_limit_next = min(
        ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
        statement_limit + max(10, ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_STEP),
    )
    t0 = _t_start()
    cached = _get_cached_driver_detail(wa_id)
    _t_end("cache_get_detail", t0)

    def _first_id(source: Dict[str, Any]) -> Optional[str]:
        if not source:
            return None
        for key in ["account_id", "xero_contact_id", "contact_id", "xero_contact_id_bjj", "xero_contact_id_hakki", "xero_contact_id_a49"]:
            val = source.get(key)
            if val:
                return str(val)
        ids = source.get("xero_contact_ids") or []
        if ids:
            return str(ids[0])
        return None

    async def _resolve_contacts_and_account(
        profile: Dict[str, Any],
        driver_lookup: Optional[Dict[str, Any]],
        roster_driver: Optional[Dict[str, Any]],
    ) -> Tuple[List[str], Optional[str], Optional[str]]:
        contact_ids: List[str] = []
        if profile:
            contact_ids.extend(profile.get("contact_ids") or [])
        if driver_lookup:
            contact_ids.extend(driver_lookup.get("xero_contact_ids") or [])
        if roster_driver:
            contact_ids.extend(roster_driver.get("contact_ids") or [])
        contact_ids = [c for c in dict.fromkeys([str(c).strip() for c in contact_ids if c])]

        account_id = None
        if roster_driver:
            account_id = _first_id(roster_driver)
        if not account_id and driver_lookup:
            account_id = _first_id(driver_lookup)
        if not account_id and profile:
            ids = profile.get("contact_ids") or []
            if ids:
                account_id = str(ids[0])

        balance_contact = None
        if contact_ids:
            t_bal = _t_start()
            balance_info = await asyncio.get_running_loop().run_in_executor(None, get_latest_xero_outstanding, contact_ids)
            _t_end("xero_outstanding", t_bal)
            if balance_info and balance_info.get("contact_id") and balance_info.get("outstanding") is not None:
                balance_contact = str(balance_info.get("contact_id"))
                account_id = balance_contact

        return contact_ids, account_id, balance_contact

    if cached is None:
        roster_driver = _get_cached_roster_driver(wa_id)
        loop = asyncio.get_running_loop()
        # Run independent lookups concurrently to reduce wall time.
        t_prof = _t_start()
        profile_future = loop.run_in_executor(None, fetch_driver_profile, wa_id)
        _t_end("spawn_fetch_driver_profile", t_prof)
        t_lookup = _t_start()
        lookup_future = loop.run_in_executor(None, lookup_driver_by_wa, wa_id) if roster_driver is None else None
        _t_end("spawn_lookup_driver_by_wa", t_lookup)

        t_prof_wait = _t_start()
        profile, profile_error = await profile_future
        _t_end("await_fetch_driver_profile", t_prof_wait)
        profile = profile or {}
        t_lookup_wait = _t_start()
        driver_lookup = await lookup_future if lookup_future else None
        _t_end("await_lookup_driver_by_wa", t_lookup_wait)
        if roster_driver and not driver_lookup:
            driver_lookup = roster_driver

        t_contacts = _t_start()
        contact_ids, account_id, balance_contact = await _resolve_contacts_and_account(profile, driver_lookup, roster_driver)
        _t_end("resolve_contacts_account", t_contacts)

        kpi_metrics = roster_driver
        kpi_error = None
        order_stats = None
        order_error = None

        detail = {
            "profile": profile,
            "profile_error": profile_error,
            "driver_lookup": driver_lookup,
            "kpi_metrics": kpi_metrics,
            "kpi_error": kpi_error,
            "contact_ids": contact_ids,
            "order_stats": order_stats,
            "order_error": order_error,
            "account_id_used": account_id,
            "balance_contact_id": balance_contact,
        }
        _set_cached_driver_detail(wa_id, detail)
    else:
        detail = cached

    profile = detail.get("profile") or {}
    profile_error = detail.get("profile_error")
    driver_lookup = detail.get("driver_lookup")
    kpi_metrics = detail.get("kpi_metrics")
    kpi_error = detail.get("kpi_error")
    contact_ids = detail.get("contact_ids") or []
    order_stats = detail.get("order_stats")
    order_error = detail.get("order_error")

    # Statement is lazy-loaded via /admin/drivers/{wa_id}/statement/section after page render.
    # Today's KPIs are lazy-loaded via /admin/drivers/{wa_id}/kpis/today/section after page render.

    def _pick_metric(*keys: str) -> Optional[Any]:
        for source in (kpi_metrics, driver_lookup, profile):
            if not source:
                continue
            for key in keys:
                val = source.get(key)
                if val not in (None, ""):
                    return val
        return None

    trips_val = _coerce_float(_pick_metric("total_finished_orders", "trip_count", "total_trips_sent", "finished_trips")) or 0.0
    gmv_val = _coerce_float(_pick_metric("gross_earnings", "total_gmv")) or 0.0
    hours_val = _coerce_float(_pick_metric("online_hours", "total_online_hours")) or 0.0
    efficiency_badge_label, efficiency_badge_state = _efficiency_badge_for_driver(
        int(trips_val),
        gmv_val,
        hours_val,
    )

    xero_balance_val = _coerce_float(_pick_metric("xero_balance"))
    payments_val = _coerce_float(_pick_metric("payments")) or 0.0
    bolt_wallet_val = _coerce_float(_pick_metric("bolt_wallet_payouts")) or 0.0
    yday_wallet_val = _coerce_float(_pick_metric("yday_wallet_balance"))
    rental_balance_val = _coerce_float(_pick_metric("rental_balance"))
    payer_badge_label, payer_badge_state = _payer_badge(
        xero_balance_val,
        payments_val + bolt_wallet_val,
        yday_wallet_val,
        rental_balance_val,
    )

    order_display = None
    if order_stats:
        order_display = {
            "today_orders": order_stats["today"]["orders"],
            "today_earnings": fmt_rands(order_stats["today"]["earnings"]),
            "last7_orders": order_stats["last_7_days"]["orders"],
            "last7_earnings": fmt_rands(order_stats["last_7_days"]["earnings"]),
            "source": order_stats.get("table"),
        }

    display_name = profile.get("display_name") or (driver_lookup or {}).get("display_name") or "Driver"
    profile_wa = profile.get("wa_id") or (driver_lookup or {}).get("whatsapp_number") or wa_id
    wa_template_defaults = {
        "driver_update": [display_name, fmt_rands(xero_balance_val or 0)],
    }
    # Surface rental_balance from KPI metrics into profile fields for roster details if missing.
    if kpi_metrics and profile is not None and not profile.get("rental_balance"):
        if kpi_metrics.get("rental_balance") is not None:
            profile["rental_balance"] = kpi_metrics.get("rental_balance")
    fields = _filter_roster_fields(summarize_profile_fields(profile))
    wa_templates = get_whatsapp_templates()
    wa_sent = request.query_params.get("wa_sent")
    wa_error = request.query_params.get("wa_error")
    email_sent = request.query_params.get("email_sent")
    email_error = request.query_params.get("email_error")
    call_sent = request.query_params.get("call_sent")
    call_error = request.query_params.get("call_error")
    t_hist = _t_start()
    interaction_history = get_interaction_history(wa_id, limit=30)
    _t_end("interaction_history", t_hist)
    t_msgs = _t_start()
    message_history = get_message_history(wa_id, limit=50)
    _t_end("message_history", t_msgs)

    def _parse_int(raw: Optional[str], default: int) -> int:
        if raw is None:
            return default
        try:
            return int(str(raw).strip())
        except (TypeError, ValueError):
            return default

    name = request.query_params.get("name")
    model = request.query_params.get("model")
    reg = request.query_params.get("reg")
    phone = request.query_params.get("phone")
    collections_agent = request.query_params.get("collections_agent")
    status = request.query_params.get("status")
    driver_type = request.query_params.get("driver_type")
    payer_type = request.query_params.get("payer_type")
    limit_val = _parse_int(request.query_params.get("limit"), 50)
    if limit_val <= 0:
        limit_val = 50
    offset_val = _parse_int(request.query_params.get("offset"), 0)
    if offset_val < 0:
        offset_val = 0

    base_driver_query = _build_driver_filter_query(
        name=name,
        model=model,
        reg=reg,
        phone=phone,
        collections_agent=collections_agent,
        status=status,
        driver_type=driver_type,
        payer_type=payer_type,
        limit=limit_val,
        offset=offset_val,
    )
    driver_directory_url = "/admin/drivers"
    if base_driver_query:
        driver_directory_url = f"{driver_directory_url}?{base_driver_query}"

    next_driver_url = None
    next_driver_name = None
    normalized_current = _normalize_wa_id(wa_id) or wa_id
    try:
        loop = asyncio.get_running_loop()

        def _load_filtered():
            return fetch_active_driver_profiles(
                name=name,
                model=model,
                reg=reg,
                phone=phone,
                collections_agent=collections_agent,
                status=status,
                driver_type=driver_type,
                payer_type=payer_type,
                limit=0,
                offset=0,
                paginate=False,
            )

        filtered_drivers, list_error, _total, *_rest = await loop.run_in_executor(None, _load_filtered)
        if list_error:
            filtered_drivers = []
    except Exception:
        filtered_drivers = []

    if filtered_drivers:
        current_index = None
        for idx, driver in enumerate(filtered_drivers):
            if _normalize_wa_id(driver.get("wa_id")) == normalized_current:
                current_index = idx
                break
        if current_index is not None:
            current_offset = (current_index // limit_val) * limit_val if limit_val else 0
            current_query = _build_driver_filter_query(
                name=name,
                model=model,
                reg=reg,
                phone=phone,
                collections_agent=collections_agent,
                status=status,
                driver_type=driver_type,
                payer_type=payer_type,
                limit=limit_val,
                offset=current_offset,
            )
            driver_directory_url = "/admin/drivers"
            if current_query:
                driver_directory_url = f"{driver_directory_url}?{current_query}"
            next_index = current_index + 1
            if next_index < len(filtered_drivers):
                next_driver = filtered_drivers[next_index]
                next_offset = (next_index // limit_val) * limit_val if limit_val else 0
                next_query = _build_driver_filter_query(
                    name=name,
                    model=model,
                    reg=reg,
                    phone=phone,
                    collections_agent=collections_agent,
                    status=status,
                    driver_type=driver_type,
                    payer_type=payer_type,
                    limit=limit_val,
                    offset=next_offset,
                )
                next_wa = next_driver.get("wa_id")
                if next_wa:
                    next_driver_url = f"/admin/drivers/{next_wa}"
                    if next_query:
                        next_driver_url = f"{next_driver_url}?{next_query}"
                next_driver_name = next_driver.get("display_name") or next_wa

    if ADMIN_DRIVER_DETAIL_TIMING:
        total_ms = sum(ms for _label, ms in timings)
        timing_str = " ".join([f"{label}={ms:.1f}ms" for label, ms in timings])
        log.info(
            "[ADMIN-DRV-DETAIL] wa_id=%s cached=%s timings_total=%.1fms %s",
            wa_id,
            bool(cached),
            total_ms,
            timing_str,
        )
    return templates.TemplateResponse(
        "admin_driver_detail.html",
        {
            "request": request,
            "admin": admin_user,
            "profile": profile,
            "profile_error": profile_error,
            "profile_fields": fields,
            "kpi_metrics": kpi_metrics,
            "kpi_error": kpi_error,
            "order_stats": order_display,
            "order_error": order_error,
            "nav_active": "drivers",
            "driver_lookup": driver_lookup,
            "format_rands": fmt_rands,
	            "wa_id": wa_id,
	            "display_name": display_name,
	            "profile_wa": profile_wa,
	            "statement_limit": statement_limit,
	            "statement_limit_next": statement_limit_next,
	            "statement_limit_max": ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_MAX,
	            "statement_limit_default": ADMIN_DRIVER_DETAIL_STATEMENT_LIMIT_DEFAULT,
            "wa_templates": wa_templates,
            "wa_template_defaults": wa_template_defaults,
            "wa_sent": wa_sent,
            "wa_error": wa_error,
            "email_sent": email_sent,
            "email_error": email_error,
            "call_sent": call_sent,
            "call_error": call_error,
            "interaction_history": interaction_history,
            "message_history": message_history,
            "driver_directory_url": driver_directory_url,
            "next_driver_url": next_driver_url,
            "next_driver_name": next_driver_name,
            "efficiency_badge_label": efficiency_badge_label,
            "efficiency_badge_state": efficiency_badge_state,
            "payer_badge_label": payer_badge_label,
            "payer_badge_state": payer_badge_state,
            "softphone_url": SOFTPHONE_URL,
        },
    )


@app.get("/admin/interactions", response_class=HTMLResponse)
def admin_interactions_page(request: Request, limit: int = 200):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    rows = get_all_interactions(limit=limit)
    return templates.TemplateResponse(
        "admin_interactions.html",
        {
            "request": request,
            "admin": admin_user,
            "rows": rows,
            "limit": limit,
            "nav_active": "interactions",
        },
    )


@app.get("/admin/tickets", response_class=HTMLResponse)
def admin_ticket_list(request: Request, status: Optional[str] = None, limit: int = 50):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    tickets = fetch_driver_issue_tickets(limit=limit, status_filter=status)
    status_options = get_ticket_status_options()
    return templates.TemplateResponse(
        "admin_tickets.html",
        {
            "request": request,
            "admin": admin_user,
            "tickets": tickets,
            "status_filter": status or "",
            "status_options": status_options,
            "nav_active": "tickets",
        },
    )


@app.get("/admin/tickets/{ticket_id}", response_class=HTMLResponse)
def admin_ticket_detail(request: Request, ticket_id: int):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    ticket = fetch_driver_issue_ticket(ticket_id)
    if not ticket:
        return PlainTextResponse("Ticket not found", status_code=404)
    status_options = get_ticket_status_options()
    message = request.query_params.get("msg")
    logs = fetch_driver_issue_logs(ticket_id, limit=100)
    conversation = fetch_ticket_conversation(ticket.get("wa_id"), limit=40)
    return templates.TemplateResponse(
        "admin_ticket_detail.html",
        {
            "request": request,
            "admin": admin_user,
            "ticket": ticket,
            "status_options": status_options,
            "message": message,
            "logs": logs,
            "conversation": conversation,
            "nav_active": "tickets",
        },
    )


@app.get("/admin/tickets/{ticket_id}/media/{media_index}")
async def admin_ticket_media(request: Request, ticket_id: int, media_index: int):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    ticket = fetch_driver_issue_ticket(ticket_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")
    media_list = ticket.get("media_list") or []
    if media_index < 0 or media_index >= len(media_list):
        raise HTTPException(status_code=404, detail="Media not found")
    media_entry = media_list[media_index] or {}
    media_id = media_entry.get("id")
    media_url = media_entry.get("url")
    mime_type = media_entry.get("mime_type") or "application/octet-stream"
    filename = media_entry.get("filename") or f"ticket-{ticket_id}-media-{media_index}"

    # Refresh short-lived URLs if we have the media ID.
    if media_id:
        fresh = fetch_media_url(media_id)
        if fresh:
            media_url = fresh
    if not media_url:
        raise HTTPException(status_code=404, detail="Media URL unavailable")

    data = download_media_bytes(media_url)
    if not data:
        raise HTTPException(status_code=404, detail="Unable to download media")

    headers = {"Content-Disposition": f'inline; filename="{filename}"'}
    return StreamingResponse(io.BytesIO(data), media_type=mime_type, headers=headers)


@app.post("/admin/tickets/{ticket_id}/status")
async def admin_ticket_update_status(
    request: Request,
    ticket_id: int,
    new_status: str = Form(...),
    note: Optional[str] = Form(None),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    ticket = fetch_driver_issue_ticket(ticket_id)
    prev_status = ticket.get("status") if ticket else None
    new_status_clean = (new_status or "").strip()
    note_clean = (note or "").strip()
    status_updated = False
    note_updated = False
    if new_status_clean:
        if ticket and new_status_clean == (ticket.get("status") or ""):
            status_updated = False
        else:
            status_updated = update_driver_issue_status(ticket_id, new_status_clean)
    if note_clean:
        timestamp = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
        patch = {
            "admin_last_note": note_clean,
            "admin_last_note_by": admin_user.get("email"),
            "admin_last_note_at": timestamp,
        }
        note_updated = update_driver_issue_metadata(ticket_id, patch)
    if status_updated or note_updated:
        if status_updated and note_updated:
            action_type = "status_note"
        elif status_updated:
            action_type = "status_change"
        else:
            action_type = "note"
        log_driver_issue_ticket_event(
            ticket_id,
            admin_email=admin_user.get("email"),
            action_type=action_type,
            from_status=prev_status if status_updated else None,
            to_status=new_status_clean if status_updated else None,
            note=note_clean if note_updated else None,
        )
        if (
            status_updated
            and _is_ticket_status_closed(new_status_clean)
            and not _is_ticket_status_closed(prev_status)
            and ticket
        ):
            reason = note_clean
            if not reason:
                meta = ticket.get("metadata_dict") or {}
                reason = (meta.get("admin_last_note") or "").strip()
            if not reason:
                reason = "Closed by ops."
                timestamp = jhb_now().strftime("%Y-%m-%d %H:%M:%S")
                update_driver_issue_metadata(
                    ticket_id,
                    {
                        "admin_last_note": reason,
                        "admin_last_note_by": admin_user.get("email"),
                        "admin_last_note_at": timestamp,
                    },
                )
            _notify_driver_ticket_closed(
                ticket=ticket,
                reason=reason,
                admin_email=admin_user.get("email"),
            )
    if not (status_updated or note_updated):
        msg = "no_changes"
    else:
        msg = "updated"
    redirect_url = f"/admin/tickets/{ticket_id}?msg={msg}"
    return RedirectResponse(url=redirect_url, status_code=303)


@app.post("/admin/tickets/{ticket_id}/reply")
async def admin_ticket_reply(
    request: Request,
    ticket_id: int,
    reply_text: str = Form(...),
):
    admin_user = get_authenticated_admin(request)
    if not admin_user:
        return _redirect_to_login()
    reply_text = (reply_text or "").strip()
    if not reply_text:
        return RedirectResponse(
            url=f"/admin/tickets/{ticket_id}?msg=reply_empty",
            status_code=303,
        )
    ticket = fetch_driver_issue_ticket(ticket_id)
    if not ticket:
        return PlainTextResponse("Ticket not found", status_code=404)
    wa_id = ticket.get("wa_id")
    if not wa_id:
        return RedirectResponse(
            url=f"/admin/tickets/{ticket_id}?msg=reply_missing_wa",
            status_code=303,
        )
    outbound_id = send_whatsapp_text(wa_id, reply_text)
    send_status = "sent" if outbound_id else "send_failed"
    timestamp_unix = str(int(time.time()))
    s_label, s_score, s_raw = analyze_sentiment(reply_text)
    log_message(
        direction="OUTBOUND",
        wa_id=wa_id,
        text=reply_text,
        intent=None,
        status=send_status,
        wa_message_id=outbound_id,
        message_id=outbound_id,
        business_number=None,
        phone_number_id=None,
        origin_type="admin_console",
        raw_json={"ticket_id": ticket_id, "admin_reply": True},
        timestamp_unix=timestamp_unix,
        sentiment=s_label,
        sentiment_score=s_score,
        intent_label=None,
        ai_raw=s_raw,
        conversation_id=f"admin-reply-{ticket_id}-{timestamp_unix}",
    )
    update_driver_issue_metadata(
        ticket_id,
        {
            "admin_last_reply": reply_text,
            "admin_last_reply_at": jhb_now().strftime("%Y-%m-%d %H:%M:%S"),
            "admin_last_reply_by": admin_user.get("email"),
        },
    )
    log_driver_issue_ticket_event(
        ticket_id,
        admin_email=admin_user.get("email"),
        action_type="reply",
        note=reply_text,
    )
    msg = "reply_sent" if outbound_id else "reply_failed"
    return RedirectResponse(
        url=f"/admin/tickets/{ticket_id}?msg={msg}",
        status_code=303,
    )

@app.get("/webhook", response_class=PlainTextResponse)
async def verify(request: Request):
    params = request.query_params
    mode = params.get("hub.mode") or params.get("mode") or ""
    token = params.get("hub.verify_token") or params.get("token") or ""
    challenge = params.get("hub.challenge") or params.get("challenge") or ""
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return PlainTextResponse(challenge)
    raise HTTPException(status_code=403, detail="Verification failed")

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()
    log.info("📩 Payload: %s", json.dumps(data, ensure_ascii=False))

    try:
        entry = data["entry"][0]["changes"][0]["value"]
    except Exception:
        return {"ok": True}

    meta = entry.get("metadata", {})
    business_number = meta.get("display_phone_number")
    phone_number_id = meta.get("phone_number_id")
    origin_type = entry.get("messaging_product", "whatsapp")

    # status events
    for st in entry.get("statuses", []) or []:
        log_status_event(
            status_event=st,
            business_number=business_number,
            phone_number_id=phone_number_id,
            origin_type=origin_type,
        )
    if "messages" not in entry:
        return {"ok": True}

    m = entry["messages"][0]
    msg_id = m.get("id")
    timestamp_unix = m.get("timestamp")

    # simple dedupe (in-memory)
    _dups = getattr(webhook, "_dups", {})
    nowts = time.time()
    for k,v in list(_dups.items()):
        if nowts - v > 120: _dups.pop(k, None)
    if msg_id in _dups:
        return {"ok": True}
    _dups[msg_id] = nowts
    setattr(webhook, "_dups", _dups)

    wa_id  = m.get("from")
    message_type = m.get("type") or "text"
    text: str = ""
    media_url: Optional[str] = None
    media_id: Optional[str] = None
    caption_text: str = ""
    location_payload: Optional[Dict[str, Any]] = None

    transcript: Optional[str] = None
    transcription_error: Optional[str] = None

    if message_type == "text":
        text = (m.get("text") or {}).get("body", "").strip()
    else:
        media_obj = m.get(message_type) or {}
        caption_text = (media_obj.get("caption") or media_obj.get("filename") or "").strip()
        text = caption_text or ""
        media_id = media_obj.get("id")
        media_url = media_obj.get("link") or fetch_media_url(media_id)
        if message_type == "location":
            location_payload = {
                "latitude": media_obj.get("latitude"),
                "longitude": media_obj.get("longitude"),
                "name": media_obj.get("name"),
                "address": media_obj.get("address"),
            }
            if not text:
                text = media_obj.get("name") or media_obj.get("address") or "[location]"
        if message_type in AUDIO_MESSAGE_TYPES:
            media_bytes = download_media_bytes(media_url)
            if media_bytes:
                transcript, transcribe_err = transcribe_audio_bytes(media_bytes, mime_type=media_obj.get("mime_type"))
                if transcript:
                    text = transcript.strip()
                else:
                    transcription_error = transcribe_err or "transcription_failed"
            else:
                transcription_error = "download_failed"
            if not text:
                placeholder = "voice note" if message_type == "voice" else "audio message"
                text = f"[{placeholder}]"
        else:
            if not text:
                text = f"[{message_type} message]"

    driver = lookup_driver_by_wa(wa_id)
    ctx_f  = load_context_file(wa_id)
    ctx    = dict(ctx_f)
    _clear_closed_ticket_context(ctx)

    if media_url:
        ctx["_last_media"] = {
            "id": media_id,
            "type": message_type,
            "url": media_url,
            "caption": caption_text,
            "timestamp": timestamp_unix,
        }

    if message_type in AUDIO_MESSAGE_TYPES:
        if transcript:
            ctx["_audio_transcript_status"] = "ok"
            ctx["_last_audio_transcript"] = {
                "text": transcript,
                "captured_at": time.time(),
                "media_id": media_id,
            }
            ctx.pop("_last_transcript_error", None)
        else:
            ctx["_audio_transcript_status"] = "failed"
            if transcription_error:
                ctx["_last_transcript_error"] = transcription_error
    else:
        ctx.pop("_audio_transcript_status", None)
    s_label, s_score, s_raw = analyze_sentiment(text)
    intent = detect_intent(text, ctx)
    if message_type in AUDIO_MESSAGE_TYPES and ctx.get("_audio_transcript_status") == "failed":
        intent = "voice_unavailable"

    resolved_intent = resolve_context_intent(
        detected_intent=intent,
        message_type=message_type,
        ctx=ctx,
        message_text=text,
    )

    raw_payload = {
        "whatsapp": entry,
        "transcript": transcript,
        "transcription_error": transcription_error,
    }

    last_nudge_outbound_id = ctx.get("_last_nudge_outbound_id")
    if (
        last_nudge_outbound_id
        and not ctx.get("_last_nudge_response_logged")
        and resolved_intent not in {ZERO_TRIP_NUDGE_INTENT, None}
    ):
        try:
            last_ts_raw = ctx.get("_last_nudge_at")
            last_ts_float = float(last_ts_raw) if last_ts_raw else None
            response_dt = jhb_now()
            latency_sec: Optional[float] = None
            if last_ts_float is not None:
                latency_sec = max(0.0, response_dt.timestamp() - last_ts_float)
            updated = _record_nudge_response(
                whatsapp_message_id=last_nudge_outbound_id,
                response_message_id=msg_id,
                response_ts=response_dt,
                response_latency_sec=latency_sec,
                response_intent=resolved_intent,
            )
            if updated:
                ctx["_last_nudge_response_logged"] = True
                ctx["_last_nudge_response_msg_id"] = msg_id
        except Exception as exc:
            log.warning("Failed to record nudge response for %s: %s", wa_id, exc)

    media_payload: Optional[Dict[str, Any]] = None
    if media_url:
        media_payload = {
            "type": message_type,
            "url": media_url,
            "id": media_id,
            "caption": caption_text,
            "mime_type": media_obj.get("mime_type"),
            "filename": media_obj.get("filename"),
        }

    # INBOUND LOG
    log_message(
        direction="INBOUND",
        wa_id=wa_id,
        text=text,
        media_url=media_url,
        intent=resolved_intent,
        status="received",
        wa_message_id=msg_id,
        driver_id=None,
        message_id=msg_id,
        business_number=business_number,
        phone_number_id=phone_number_id,
        origin_type=origin_type,
        raw_json=raw_payload,
        timestamp_unix=timestamp_unix,
        sentiment=s_label,
        sentiment_score=s_score,
        intent_label=resolved_intent,
        ai_raw=s_raw,
        conversation_id=msg_id,
    )

    last_reply = ctx.get("_last_reply")
    last_reply_at = ctx.get("_last_reply_at")
    last_inbound = ctx.get("_last_user_message")
    reply = build_reply(
        resolved_intent,
        text,
        driver,
        ctx,
        wa_id,
        message_type=message_type,
        media=media_payload,
        location=location_payload,
    )
    override_intent = ctx.pop("_intent_override", None)
    if override_intent:
        resolved_intent = override_intent
    ctx["_last_intent"] = resolved_intent

    if not reply:
        save_context_file(wa_id, ctx)
        save_context_db(wa_id, resolved_intent, last_reply or "", ctx)
        return {"ok": True}

    if last_reply and reply == last_reply and last_reply_at:
        try:
            inbound_match = False
            if last_inbound:
                inbound_match = re.sub(r"\s+", " ", text.strip().lower()) == re.sub(
                    r"\s+", " ", str(last_inbound).strip().lower()
                )
            if inbound_match and (time.time() - float(last_reply_at)) < 900:
                save_context_file(wa_id, ctx)
                save_context_db(wa_id, resolved_intent, last_reply or "", ctx)
                return {"ok": True}
        except Exception:
            pass

    display_name = (driver.get("display_name") or driver.get("first_name") or "Driver").strip()
    reply = with_greet(ctx, display_name, reply, resolved_intent)

    ctx["_last_reply"] = reply
    ctx["_last_reply_at"] = time.time()

    # Save context
    save_context_file(wa_id, ctx)
    save_context_db(wa_id, resolved_intent, reply, ctx)

    # Send reply
    outbound_id = send_whatsapp_text(wa_id, reply)

    # OUTBOUND LOG
    s2_label, s2_score, s2_raw = analyze_sentiment(reply or "")
    try:
        inbound_ts = int(timestamp_unix) if timestamp_unix and str(timestamp_unix).isdigit() else int(time.time())
        response_time = max(0.0, time.time() - inbound_ts)
    except Exception:
        response_time = None

    log_message(
        direction="OUTBOUND",
        wa_id=wa_id,
        text=reply,
        intent=resolved_intent,
        status="sent" if outbound_id else "send_failed",
        wa_message_id=outbound_id,
        driver_id=None,
        message_id=outbound_id,
        business_number=business_number,
        phone_number_id=phone_number_id,
        origin_type=origin_type,
        raw_json={"reply": reply, "conversation_id": msg_id},
        timestamp_unix=str(int(time.time())),
        sentiment=s2_label,
        sentiment_score=s2_score,
        intent_label=resolved_intent,
        ai_raw=s2_raw,
        conversation_id=msg_id,
        response_time_sec=response_time,
    )

    return {"ok": True}

# -----------------------------------------------------------------------------
# Run with: uvicorn app_min:app --host 0.0.0.0 --port 8000
# -----------------------------------------------------------------------------
def _load_suburb_map() -> List[Tuple[str, str]]:
    global SUBURB_MAP_CACHE
    if SUBURB_MAP_CACHE is not None:
        return SUBURB_MAP_CACHE
    mapping: List[Tuple[str, str]] = []
    path = (SUBURB_MAP_PATH or "").strip()
    if path and os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                for line in fh:
                    raw = line.strip()
                    if not raw or raw.startswith("#"):
                        continue
                    if "," not in raw:
                        continue
                    pattern, suburb = raw.split(",", 1)
                    pattern = pattern.strip().lower()
                    suburb = suburb.strip()
                    if pattern and suburb:
                        mapping.append((pattern, suburb))
        except Exception as exc:
            log.warning("suburb map load failed: %s", exc)
    SUBURB_MAP_CACHE = mapping
    return mapping

def _lookup_suburb_from_address(address: str) -> Optional[str]:
    if not address:
        return None
    lowered = address.lower()
    for pattern, suburb in _load_suburb_map():
        if pattern and pattern in lowered:
            return suburb
    return None

def _normalize_pickup_area(area: str) -> Optional[str]:
    if not area:
        return None
    cleaned = area.strip()
    if not cleaned:
        return None
    lower_clean = cleaned.lower()
    if re.search(r"\b(ortia|jnb|o\.?\s*r\.?\s*tambo|or\s*tambo|tambo\s+international|tambo\s+airport)\b", lower_clean):
        return "OR Tambo Airport"
    mapped = _lookup_suburb_from_address(cleaned)
    if mapped:
        return mapped
    parts = [p.strip() for p in BOLT_ADDRESS_SPLIT_RE.split(cleaned) if p.strip()]
    if not parts:
        parts = [cleaned]

    mall_tokens = ("mall", "centre", "center", "plaza", "square", "market")
    retail_tokens = (
        "pick n pay", "pick 'n pay", "pnp", "checkers", "shoprite", "woolworths",
        "spar", "makro", "game", "builders", "dis-chem", "clicks", "boxer", "ok foods",
    )
    entrance_match = re.search(r"\b(entrance|gate|entry)\b", lower_clean)
    if entrance_match:
        trimmed = re.sub(r"\b(entrance|gate|entry)\b.*", "", cleaned, flags=re.IGNORECASE).strip(" ,-/")
        if trimmed:
            cleaned = trimmed
            lower_clean = cleaned.lower()
    street_tokens = (
        "street", "st", "road", "rd", "avenue", "ave", "drive", "dr", "lane", "ln",
        "close", "crescent", "boulevard", "blvd", "highway", "hwy", "way", "place",
        "pl", "circle", "cres", "terrace", "park", "parkway", "pkwy",
    )
    city_tokens = {
        "johannesburg", "joburg", "soweto", "pretoria", "tshwane",
        "durban", "cape town", "south africa", "gauteng",
    }

    def normalize_place(value: str) -> str:
        text = re.sub(r"^[0-9]+\\s+", "", value.strip())
        text = re.sub(r"\\s+[0-9]{3,5}$", "", text)
        text = re.sub(r"\\s{2,}", " ", text).strip()
        return text.title()

    def looks_like_street(value: str) -> bool:
        val = value.lower()
        if any(tok in val for tok in street_tokens):
            return True
        return bool(re.search(r"\\b\\d+\\b", val) and len(val) > 6)

    for part in parts:
        lower = part.lower()
        if any(tok in lower for tok in retail_tokens):
            return normalize_place(part)
        if any(tok in lower for tok in mall_tokens):
            return normalize_place(part)

    candidates = [p for p in parts if not looks_like_street(p)]
    for part in candidates or parts:
        normalized = normalize_place(part)
        if not normalized:
            continue
        if normalized.lower() in city_tokens:
            continue
        return normalized

    if parts:
        return normalize_place(parts[0])
    return None

def bank_details_for_model(model: str) -> Optional[Dict[str, str]]:
    m = (model or "").lower()
    if not m:
        return None
    # Check whole-token matches first (e.g., "bls" should match before generic "vitz").
    tokens = set(re.findall(r"[a-z0-9]+", m))
    for patterns, bank, acct, branch in BANK_DETAILS_BY_MODEL:
        if any(p.strip("%") in tokens for p in patterns):
            return {"bank_name": bank, "account_number": acct, "branch_code": branch}
    # Fallback to substring contains
    for patterns, bank, acct, branch in BANK_DETAILS_BY_MODEL:
        if any(p in m for p in patterns):
            return {"bank_name": bank, "account_number": acct, "branch_code": branch}
    return None
