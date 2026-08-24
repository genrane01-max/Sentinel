"""
InnovestX Automated Trading Bot — Price Action 2H Strategy
(v7, เข้าซื้อด้วย 2 ชม.เป็นหลัก / ชม.3 ห้ามซื้อถ้าลงแรง / bid-offer ลงฐานที่ 2)

ของใหม่ในเวอร์ชันนี้:
1. สูตรเข้าซื้อใช้ 2 ชั่วโมงเป็นหลัก (ชม.1 + ชม.2 ต้องขึ้น, สุทธิ 2 ชม. ต้องบวก)
2. ชม.ที่ 3 ไม่ยืนยันซื้อแล้ว — ใช้แค่ห้ามซื้อถ้าลงแรงเกิน −1.5%
3. แยก Firebase: ฐานหลัก = สถานะเทรด/ตั้งค่า, ฐานที่ 2 = bid/offer + ประวัติราคา
4. ตัดสินใจซื้อเทียบ offer (ask), ขายเทียบ bid, กันสเปรดกว้าง
5. แดชบอร์ดใช้สูตรเดียวกับบอท

ของเดิมยังอยู่ครบ: แดชบอร์ด, หยุด/เริ่มเทรด, รหัสผ่าน, % ขาดทุนต่อวัน, อัตราเงินต่อไม้,
จำนวนไม้ขาดทุนติดกัน, price_history ต่อเหรียญ, ค่าธรรมเนียม, circuit breaker, Decimal, graceful shutdown
"""

import time
import uuid
import hmac
import hashlib
import json
import os
import re
import signal
import logging
import threading
import urllib.parse
from string import Template
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import firebase_admin
from firebase_admin import credentials, db
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot_transactions.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("Sentinel")

# ==================== Firebase (state persistence) ====================
# ฐานหลัก (DB1): bot_control, shared risk, สถานะไม้
# ฐานที่ 2 (DB2): bid/offer + ประวัติราคา — ตั้ง FIREBASE_DATABASE_URL_2 เมื่อพร้อม
MARKET_APP_NAME = "market"
_market_db_enabled = False


def init_firebase():
    global _market_db_enabled
    try:
        if not firebase_admin._apps:
            cred_path = os.getenv("FIREBASE_CREDENTIALS", "/etc/secrets/firebase.json")
            cred = credentials.Certificate(cred_path)
            db_url = os.getenv("FIREBASE_DATABASE_URL")
            firebase_admin.initialize_app(cred, {"databaseURL": db_url})
            logger.info("Firebase DB1 (สถานะเทรด) เชื่อมต่อสำเร็จ")

            db_url_2 = (os.getenv("FIREBASE_DATABASE_URL_2") or "").strip()
            if db_url_2:
                cred2_path = os.getenv("FIREBASE_CREDENTIALS_2", "/etc/secrets/firebase2.json")
                cred2 = credentials.Certificate(cred2_path) if cred2_path and os.path.isfile(cred2_path) else cred
                firebase_admin.initialize_app(cred2, {"databaseURL": db_url_2}, name=MARKET_APP_NAME)
                _market_db_enabled = True
                logger.info("Firebase DB2 (bid/offer + ประวัติราคา) เชื่อมต่อสำเร็จ")
            else:
                logger.warning(
                    "ยังไม่มี FIREBASE_DATABASE_URL_2 — bid/offer และประวัติราคาจะลงฐานหลักไปก่อน "
                    "จนกว่าจะเพิ่มฐานที่สอง"
                )
        else:
            try:
                firebase_admin.get_app(MARKET_APP_NAME)
                _market_db_enabled = True
            except (ValueError, KeyError):
                _market_db_enabled = False
    except Exception as e:
        logger.error(f"Firebase Init Error: {e}")


def primary_ref(path):
    """โหนดบนฐานหลัก (สถานะเทรด / ตั้งค่า)"""
    return db.reference(path)


def market_ref(path):
    """โหนดบนฐานที่ 2 ถ้ามี ไม่งั้นใช้ฐานหลักที่ path เดิม"""
    if _market_db_enabled:
        try:
            return db.reference(path, app=firebase_admin.get_app(MARKET_APP_NAME))
        except Exception as e:
            logger.warning(f"ใช้ DB2 ไม่ได้ ถอยไปฐานหลัก: {e}")
    return db.reference(path)


def market_db_ready():
    return _market_db_enabled


def load_symbol_view(symbol):
    """รวมสถานะเทรดจาก DB1 กับ bid/offer+ประวัติจาก DB2 สำหรับแดชบอร์ด"""
    try:
        state = primary_ref(f"bots/{symbol}/state").get() or {}
    except Exception:
        state = {}
    try:
        market = market_ref(f"market/{symbol}").get() or {}
    except Exception:
        market = {}
    if market.get("price_history"):
        state["price_history"] = market["price_history"]
    if market.get("quote"):
        state["quote"] = market["quote"]
    return state


init_firebase()

# ==================== การควบคุมบอทจากหน้าเว็บ ====================
DEFAULT_SYMBOL = os.environ.get("SYMBOL", "BTCTHB").upper()
RUNNING_SYMBOL = {"value": DEFAULT_SYMBOL}
RUNNING_WATCHLIST = {"value": [DEFAULT_SYMBOL]}
LIVE_BOTS = {}
STOP_ALL = {"value": False}
SHARED_RISK_PATH = "bots/_shared/risk"
SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}$")
KNOWN_QUOTE_SUFFIXES = ("THB", "USDT", "USD")
# เข้าซื้อด้วย 2 ชม.เป็นหลัก — ชม.3 ใช้แค่ห้ามซื้อ ไม่เอามายืนยัน
MIN_CHANGE_1H_PERCENT = 0.5      # ชม.ล่าสุดต้องขึ้นอย่างน้อยเท่านี้ (สูงกว่าค่าฟี ~0.4%)
MIN_NET_2H_PERCENT = 0.7         # สุทธิ 2 ชม. ต้องบวกจริง
MAX_CHANGE_1H_PERCENT = 2.5      # ชม.ล่าสุดพุ่งเกินนี้ = ไล่หัว ไม่ซื้อ
HOUR3_VETO_PERCENT = -1.5        # ชม.3 ลงแรงกว่านี้ = ขาลงใหญ่ยังไม่จบ ห้ามซื้อ
# คะแนนสะสม: ชม.1 ผ่าน +50, ชม.2 ขึ้น +30, สุทธิ 2 ชม.ผ่าน +20, ชม.2 ลงหักเหลือ ~10
# ซื้อเมื่อ confidence >= MIN_CONFIDENCE_TO_BUY และไม่โดน veto (ไล่หัว / ชม.3 ลงแรง)
MIN_CONFIDENCE_TO_BUY = 80
MAX_SPREAD_PERCENT = 0.5         # ไม่ซื้อถ้า (ask-bid)/mid กว้างเกินนี้ (%)
ORDER_SEND_PATH = "/api/v1/digital-asset/order/send"
PENDING_ORDER_TTL_SEC = 120
MARKET_HISTORY_FLUSH_SEC = 120
PRICE_TICK_MIN_INTERVAL_SEC = 60
PRICE_TICK_MIN_MOVE_PERCENT = 0.05
_SYMBOL_CATALOG = {"ts": 0.0, "symbols": None}
_SHUTDOWN_HANDLERS_INSTALLED = False


def _pct_change(newer, older):
    if newer is None or older is None or older == 0:
        return None
    return (newer - older) / older * 100


def _safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_book_price(levels):
    if not levels:
        return None
    row = levels[0]
    if isinstance(row, dict):
        for key in ("price", "px", "p", "lastTradePrice"):
            if key in row:
                val = _safe_float(row[key])
                if val is not None and val > 0:
                    return val
    elif isinstance(row, (list, tuple)) and row:
        val = _safe_float(row[0])
        if val is not None and val > 0:
            return val
    else:
        val = _safe_float(row)
        if val is not None and val > 0:
            return val
    return None


def parse_market_quote(payload):
    """แยก last / bid / offer จาก orderbook หรือ ticker — ไม่ยุบเป็นราคาเดียว"""
    quote = {"last": None, "bid": None, "ask": None, "mid": None, "spread_pct": None}
    if not isinstance(payload, dict) or payload.get("code") != "0000":
        return quote
    data = payload.get("data")
    record = None
    if isinstance(data, dict):
        record = data
    elif isinstance(data, list) and data:
        record = data[0] if isinstance(data[0], dict) else None
    if not isinstance(record, dict):
        return quote
    for key in ("lastTradePrice", "last", "close", "lastPrice", "price"):
        val = _safe_float(record.get(key))
        if val is not None and val > 0:
            quote["last"] = val
            break
    bids = record.get("bids") or record.get("bid") or []
    asks = record.get("asks") or record.get("ask") or []
    quote["bid"] = _first_book_price(bids)
    quote["ask"] = _first_book_price(asks)
    if quote["bid"] and quote["ask"]:
        quote["mid"] = (quote["bid"] + quote["ask"]) / 2.0
        if quote["mid"] > 0:
            quote["spread_pct"] = (quote["ask"] - quote["bid"]) / quote["mid"] * 100
    elif quote["bid"] or quote["ask"]:
        quote["mid"] = quote["bid"] or quote["ask"]
    return quote


def parse_market_price(payload):
    """ดึงราคาเดียวจาก orderbook/ticker — ชอบ last แล้วค่อย mid ของ bid/offer"""
    quote = parse_market_quote(payload)
    return quote["last"] or quote["mid"] or quote["bid"] or quote["ask"]


def quote_mark_price(quote, side="last"):
    """ราคาที่ใช้ตัดสินใจ: ซื้อใช้ offer, ขายใช้ bid, ประวัติใช้ last/mid"""
    quote = quote or {}
    if side == "buy":
        return quote.get("ask") or quote.get("last") or quote.get("mid") or quote.get("bid")
    if side == "sell":
        return quote.get("bid") or quote.get("last") or quote.get("mid") or quote.get("ask")
    return quote.get("last") or quote.get("mid") or quote.get("ask") or quote.get("bid")


def evaluate_entry_signal(current_price, price_1h_ago, price_2h_ago, price_3h_ago=None):
    """ตัดสินใจซื้อด้วย 2 ชม.เป็นหลัก ชม.3 ใช้แค่ห้ามซื้อถ้าลงแรง

    คะแนน: ชม.1 ขึ้นพอ +50, ชม.2 ขึ้น +30, สุทธิ 2 ชม.พอ +20
    ชม.2 ลงหักเหลือ ~10 / ชม.1 อ่อน = 0 / ไล่หัว / ชม.3 ลงแรง → ไม่ซื้อ
    ซื้อเมื่อ confidence >= MIN_CONFIDENCE_TO_BUY และไม่โดน veto
    """
    result = {
        "direction": None,
        "confidence": 0,
        "should_buy": False,
        "reason": "unknown",
        "change_1h": 0.0,
        "change_2h": None,
        "change_3h": None,
        "net_2h": None,
        "vetoed": False,
    }
    if current_price is None or price_1h_ago is None or price_1h_ago == 0:
        result["reason"] = "no_price"
        return result

    change_1h = _pct_change(current_price, price_1h_ago)
    change_2h = _pct_change(price_1h_ago, price_2h_ago)
    change_3h = _pct_change(price_2h_ago, price_3h_ago)
    net_2h = _pct_change(current_price, price_2h_ago)
    result["change_1h"] = change_1h or 0.0
    result["change_2h"] = change_2h
    result["change_3h"] = change_3h
    result["net_2h"] = net_2h

    if change_1h is None or change_1h == 0:
        result["reason"] = "flat"
        return result

    direction = "up" if change_1h > 0 else "down"
    result["direction"] = direction
    if direction == "down":
        result["reason"] = "down"
        return result

    if price_2h_ago is None:
        result["reason"] = "gap"
        return result

    confidence = 0
    reason = None

    if change_1h >= MIN_CHANGE_1H_PERCENT:
        confidence += 50
    else:
        reason = "weak_1h"

    if change_1h > MAX_CHANGE_1H_PERCENT:
        result["confidence"] = confidence
        result["reason"] = "chase"
        return result

    if change_2h is None:
        result["confidence"] = confidence
        result["reason"] = "gap"
        return result
    if change_2h <= 0:
        confidence = max(0, confidence - 40)
        reason = "hour2_down"
    else:
        confidence += 30

    if net_2h is not None and net_2h >= MIN_NET_2H_PERCENT:
        confidence += 20
    elif reason is None:
        reason = "weak_net"

    if change_3h is not None and change_3h < HOUR3_VETO_PERCENT:
        result["confidence"] = confidence
        result["vetoed"] = True
        result["reason"] = "hour3_veto"
        return result

    result["confidence"] = confidence
    result["should_buy"] = confidence >= MIN_CONFIDENCE_TO_BUY
    result["reason"] = None if result["should_buy"] else (reason or "weak")
    return result


def net_pnl_thb(entry_price, sell_price, qty, roundtrip_fee_percent):
    """กำไร/ขาดทุนสุทธิหลังหักค่าธรรมเนียมไป-กลับ (ซื้อขาหนึ่ง + ขายขาหนึ่ง)"""
    entry_price = float(entry_price or 0.0)
    sell_price = float(sell_price or 0.0)
    qty = float(qty or 0.0)
    fee_pct = float(roundtrip_fee_percent or 0.0)
    if entry_price <= 0 or sell_price <= 0 or qty <= 0:
        return 0.0
    one_way = (fee_pct / 2.0) / 100.0
    buy_cost = entry_price * qty * (1.0 + one_way)
    sell_proceeds = sell_price * qty * (1.0 - one_way)
    return sell_proceeds - buy_cost


def is_mutating_order_path(path):
    p = (path or "").split("?")[0].rstrip("/")
    return p.endswith("/order/send") or p.endswith("/order/cancel")


def looks_like_pair(symbol):
    """คู่เหรียญต้องเป็นตัวอักษร/ตัวเลข และลงท้ายด้วยสกุลเงินอ้างอิง เช่น BTCTHB"""
    sym = _normalize_symbol(symbol)
    if not SYMBOL_RE.match(sym):
        return False
    return any(sym.endswith(q) and len(sym) > len(q) for q in KNOWN_QUOTE_SUFFIXES)


def should_append_price_tick(history, now, price, min_interval=PRICE_TICK_MIN_INTERVAL_SEC,
                             min_move_pct=PRICE_TICK_MIN_MOVE_PERCENT):
    """ไม่บันทึกทุก 15 วิถ้ายังไม่ครบช่วง และราคาขยับน้อย — ลด payload ที่เขียนขึ้น Firebase"""
    if not history:
        return True
    try:
        last_ts, last_px = history[-1][0], history[-1][1]
    except (TypeError, ValueError, IndexError):
        return True
    if now - float(last_ts or 0) >= min_interval:
        return True
    last_px = float(last_px or 0)
    if last_px > 0 and abs(float(price) - last_px) / last_px * 100.0 >= min_move_pct:
        return True
    return False


def signed_request(api_key, api_secret, method, path, query="", body=None,
                   timeout_sec=10, max_retries=3, allow_retry=True,
                   host="api.innovestxonline.com", _retry_count=0):
    """ส่ง request พร้อม HMAC-SHA256 — คำสั่งซื้อ/ขายห้าม retry ตอน timeout"""
    url = f"https://{host}" + path + query
    body_str = json.dumps(body) if body else ""
    timestamp = str(int(time.time() * 1000))
    request_uid = str(uuid.uuid4())
    content_type = "application/json"

    content_to_sign = (
        api_key + method.upper() + host + path + query +
        content_type + request_uid + timestamp + body_str
    )
    signature = hmac.new(
        api_secret.encode("utf-8"),
        content_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    headers = {
        "Content-Type": content_type,
        "X-INVX-REQUEST-UID": request_uid,
        "X-INVX-TIMESTAMP": timestamp,
        "X-INVX-SIGNATURE": signature,
        "X-INVX-APIKEY": api_key,
    }

    mutating = is_mutating_order_path(path)
    can_retry = allow_retry and not mutating

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, timeout=timeout_sec)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, data=body_str, timeout=timeout_sec)
        else:
            raise ValueError(f"Method {method} ไม่รองรับ")
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        if mutating:
            logger.error(
                f"[UID {request_uid}] timeout/ขาดการเชื่อมตอนส่งคำสั่งซื้อขาย — ไม่ยิงซ้ำ "
                f"(ออเดอร์อาจเข้าไปแล้ว) path={path} err={e}"
            )
            return {"code": "TIMEOUT_INDETERMINATE", "message": str(e), "path": path, "uid": request_uid}
        logger.warning(f"[UID {request_uid}] เชื่อมต่อไม่สำเร็จ ({e}) — retry {_retry_count + 1}/{max_retries}")
        if can_retry and _retry_count < max_retries:
            time.sleep(2 ** _retry_count)
            return signed_request(
                api_key, api_secret, method, path, query, body,
                timeout_sec, max_retries, allow_retry, host, _retry_count + 1,
            )
        logger.error(f"[UID {request_uid}] ยกเลิกหลัง retry ครบจำนวน: {e}")
        return None

    if response.status_code == 429 or response.status_code >= 500:
        retry_this = can_retry or (response.status_code == 429 and not mutating)
        logger.warning(f"[UID {request_uid}] HTTP {response.status_code} — retry {_retry_count + 1}/{max_retries}")
        if mutating and response.status_code >= 500:
            logger.error(
                f"[UID {request_uid}] HTTP {response.status_code} ตอนส่งคำสั่งซื้อขาย — ไม่ยิงซ้ำ "
                f"(ออเดอร์อาจเข้าไปแล้ว)"
            )
            return {"code": "TIMEOUT_INDETERMINATE", "message": f"HTTP {response.status_code}", "path": path, "uid": request_uid}
        if retry_this and _retry_count < max_retries:
            time.sleep(2 ** _retry_count)
            return signed_request(
                api_key, api_secret, method, path, query, body,
                timeout_sec, max_retries, allow_retry, host, _retry_count + 1,
            )
        logger.error(f"[UID {request_uid}] HTTP {response.status_code} หลัง retry ครบจำนวน")
        return None

    try:
        data = response.json()
    except ValueError:
        logger.error(f"[UID {request_uid}] HTTP {response.status_code} — response ไม่ใช่ JSON: {response.text[:300]}")
        return None

    code = data.get("code")
    logger.info(f"[UID {request_uid}] {method} {path} -> code={code}")

    if code == "4005":
        logger.error(f"[UID {request_uid}] 4005 Invalid Signature — ตรวจสอบลำดับ string-to-sign และ API Secret")
    elif code == "4011":
        logger.error(f"[UID {request_uid}] 4011 Timestamp mismatch — นาฬิกาเครื่องอาจคลาดจาก UTC เกิน 150 วิ "
                     f"กรุณาซิงค์เวลาเครื่อง (NTP) ก่อนรันต่อ")
    elif code == "4019":
        logger.warning(f"[UID {request_uid}] 4019 Insufficient Balance")
    elif code == "4042":
        logger.warning(f"[UID {request_uid}] 4042 Symbol not found — ตรวจสอบ symbol")

    return data


def fetch_exchange_symbol_catalog(force=False):
    """รายการคู่เหรียญบน InnovestX — cache 10 นาที"""
    now = time.time()
    cached = _SYMBOL_CATALOG.get("symbols")
    if not force and cached is not None and (now - float(_SYMBOL_CATALOG.get("ts") or 0)) < 600:
        return cached
    api_key = os.environ.get("INVX_API_KEY")
    api_secret = os.environ.get("INVX_API_SECRET")
    bot = next(iter(LIVE_BOTS.values()), None)
    res = None
    if bot is not None:
        res = bot.send_request("GET", "/api/v1/digital-asset/symbols")
    elif api_key and api_secret:
        res = signed_request(api_key, api_secret, "GET", "/api/v1/digital-asset/symbols")
    if not res or res.get("code") != "0000":
        return cached
    symbols = {
        str(row.get("symbol") or "").upper()
        for row in (res.get("data") or [])
        if row.get("symbol")
    }
    _SYMBOL_CATALOG["symbols"] = symbols
    _SYMBOL_CATALOG["ts"] = now
    return symbols


def validate_watchlist_symbol(raw, catalog=None):
    """ตรวจทั้งรูปแบบคู่เหรียญ และว่ามีบนตลาดจริง (ถ้าดึงรายการได้)"""
    sym = _normalize_symbol(raw)
    if not looks_like_pair(sym):
        return None, f"รูปแบบไม่ถูกต้อง ต้องเป็นคู่เหรียญเช่น BTCTHB/ETHTHB ไม่ใช่ '{raw}'"
    if catalog is None:
        catalog = fetch_exchange_symbol_catalog()
    if catalog is not None and sym not in catalog:
        return None, f"ไม่พบคู่ {sym} บน InnovestX"
    return sym, None


def _handle_process_shutdown(signum, frame):
    logger.info(f"ได้รับสัญญาณหยุด ({signum}) กำลังบันทึกสถานะทุกเหรียญก่อนปิดบอท...")
    STOP_ALL["value"] = True
    for bot in list(LIVE_BOTS.values()):
        bot._stop_requested = True
        try:
            bot.save_state()
            bot.save_market(force_history=True)
        except Exception:
            logger.exception(f"[{getattr(bot, 'symbol', '?')}] บันทึกตอนปิดไม่สำเร็จ")


def install_shutdown_handlers():
    """ลงทะเบียน SIGINT/SIGTERM ครั้งเดียวต่อโปรเซส — ห้ามให้แต่ละเหรียญทับของเดิม"""
    global _SHUTDOWN_HANDLERS_INSTALLED
    if _SHUTDOWN_HANDLERS_INSTALLED:
        return
    signal.signal(signal.SIGINT, _handle_process_shutdown)
    signal.signal(signal.SIGTERM, _handle_process_shutdown)
    _SHUTDOWN_HANDLERS_INSTALLED = True
    logger.info("ลงทะเบียน SIGINT/SIGTERM ระดับโปรเซสแล้ว")


REASON_TH = {
    "no_price": "ยังไม่มีราคา",
    "flat": "ราคานิ่ง",
    "down": "ชม.ล่าสุดลง — ไม่ซื้อ",
    "gap": "ข้อมูลราคาขาดช่วง",
    "weak_1h": f"ชม.ล่าสุดขึ้นไม่ถึง {MIN_CHANGE_1H_PERCENT}%",
    "chase": f"ชม.ล่าสุดพุ่งเกิน {MAX_CHANGE_1H_PERCENT}% — กันไล่หัว",
    "hour2_down": "ชม.ก่อนหน้าลง — หักลบแล้วไม่ซื้อ",
    "weak_net": f"สุทธิ 2 ชม. ยังไม่ถึง +{MIN_NET_2H_PERCENT}%",
    "hour3_veto": f"ชม.3 ลงแรงกว่า {HOUR3_VETO_PERCENT}% — ห้ามซื้อ",
    "wide_spread": f"สเปรดกว้างเกิน {MAX_SPREAD_PERCENT}% — ไม่ซื้อ",
    "weak": "คะแนนยังไม่ถึงเกณฑ์",
    "waiting_history": "รอสะสมข้อมูล 2 ชั่วโมง",
    "unknown": "ยังประเมินไม่ได้",
}


def _normalize_symbol(raw):
    return str(raw or "").strip().upper()


def _normalize_watchlist(raw, fallback_symbol):
    """รับ list / ข้อความ / ค่าว่าง แล้วคืนรายการเหรียญที่ไม่ซ้ำ ตัวพิมพ์ใหญ่

    None = ยังไม่เคยตั้งค่า → ใช้เหรียญเริ่มต้น
    list ว่าง / ข้อความว่าง = ผู้ใช้ลบจนหมด → ว่างได้ (บอทจะรอเพิ่มเหรียญจากหน้าเว็บ)
    """
    symbols = []
    if isinstance(raw, list):
        source = raw
    elif isinstance(raw, str) and raw.strip():
        source = re.split(r"[\s,]+", raw.strip())
    else:
        source = []
    for item in source:
        sym = _normalize_symbol(item)
        if looks_like_pair(sym) and sym not in symbols:
            symbols.append(sym)
    if symbols:
        return symbols
    if raw is None:
        fallback = _normalize_symbol(fallback_symbol) or DEFAULT_SYMBOL
        if looks_like_pair(fallback):
            return [fallback]
        return [DEFAULT_SYMBOL]
    return []


def load_control():
    """อ่านคำสั่งควบคุมล่าสุดจากหน้าเว็บ (รายการเหรียญที่เฝ้า, หยุดชั่วคราว, ความเสี่ยง, จำนวนช่องถือพร้อมกัน)"""
    try:
        data = primary_ref("bot_control").get() or {}
    except Exception as e:
        logger.warning(f"อ่าน bot_control จาก Firebase ไม่ได้ ใช้ค่าเริ่มต้น: {e}")
        data = {}

    try:
        max_daily_loss_percent = float(data.get("max_daily_loss_percent", InnovestXTradingBot.MAX_DAILY_LOSS_PERCENT))
    except (TypeError, ValueError):
        max_daily_loss_percent = InnovestXTradingBot.MAX_DAILY_LOSS_PERCENT
    if not (0.1 <= max_daily_loss_percent <= 100):
        max_daily_loss_percent = InnovestXTradingBot.MAX_DAILY_LOSS_PERCENT

    try:
        trade_size_percent = float(data.get("trade_size_percent", InnovestXTradingBot.DEFAULT_TRADE_SIZE_PERCENT))
    except (TypeError, ValueError):
        trade_size_percent = InnovestXTradingBot.DEFAULT_TRADE_SIZE_PERCENT
    if not (1 <= trade_size_percent <= 100):
        trade_size_percent = InnovestXTradingBot.DEFAULT_TRADE_SIZE_PERCENT

    try:
        max_consecutive_losses = int(float(data.get("max_consecutive_losses", InnovestXTradingBot.MAX_CONSECUTIVE_LOSSES)))
    except (TypeError, ValueError):
        max_consecutive_losses = InnovestXTradingBot.MAX_CONSECUTIVE_LOSSES
    if not (1 <= max_consecutive_losses <= 20):
        max_consecutive_losses = InnovestXTradingBot.MAX_CONSECUTIVE_LOSSES

    try:
        max_open_positions = int(float(data.get("max_open_positions", InnovestXTradingBot.DEFAULT_MAX_OPEN_POSITIONS)))
    except (TypeError, ValueError):
        max_open_positions = InnovestXTradingBot.DEFAULT_MAX_OPEN_POSITIONS
    if not (1 <= max_open_positions <= 10):
        max_open_positions = InnovestXTradingBot.DEFAULT_MAX_OPEN_POSITIONS

    try:
        trailing_stop_percent = float(data.get("trailing_stop_percent", InnovestXTradingBot.DEFAULT_TRAILING_STOP_PERCENT))
    except (TypeError, ValueError):
        trailing_stop_percent = InnovestXTradingBot.DEFAULT_TRAILING_STOP_PERCENT
    if not (0.1 <= trailing_stop_percent <= 20):
        trailing_stop_percent = InnovestXTradingBot.DEFAULT_TRAILING_STOP_PERCENT

    try:
        stop_loss_percent = float(data.get("stop_loss_percent", InnovestXTradingBot.DEFAULT_STOP_LOSS_PERCENT))
    except (TypeError, ValueError):
        stop_loss_percent = InnovestXTradingBot.DEFAULT_STOP_LOSS_PERCENT
    if not (0.1 <= stop_loss_percent <= 50):
        stop_loss_percent = InnovestXTradingBot.DEFAULT_STOP_LOSS_PERCENT

    fallback_symbol = _normalize_symbol(data.get("active_symbol") or DEFAULT_SYMBOL)
    watchlist = _normalize_watchlist(data.get("watchlist"), fallback_symbol)

    return {
        "active_symbol": watchlist[0] if watchlist else fallback_symbol,
        "watchlist": watchlist,
        "paused": bool(data.get("paused", False)),
        "max_daily_loss_percent": max_daily_loss_percent,
        "trade_size_percent": trade_size_percent,
        "max_consecutive_losses": max_consecutive_losses,
        "max_open_positions": max_open_positions,
        "trailing_stop_percent": trailing_stop_percent,
        "stop_loss_percent": stop_loss_percent,
        "unlock_requested": bool(data.get("unlock_requested", False)),
    }


def save_control(control):
    try:
        if control.get("watchlist"):
            control["active_symbol"] = control["watchlist"][0]
        elif not control.get("active_symbol"):
            control["active_symbol"] = DEFAULT_SYMBOL
        primary_ref("bot_control").set(control)
    except Exception as e:
        logger.error(f"บันทึก bot_control ไป Firebase ไม่สำเร็จ: {e}")


def load_shared_risk():
    """ความเสี่ยงระดับบัญชี (ใช้ร่วมทุกเหรียญ) — ขาดทุนวันนี้ / ไม้ติดกัน / HALTED"""
    default = {
        "status": "IDLE",
        "trade_date": None,
        "daily_start_balance": 0.0,
        "daily_realized_pnl": 0.0,
        "consecutive_losses": 0,
    }
    try:
        saved = primary_ref(SHARED_RISK_PATH).get()
        if saved:
            default.update(saved)
    except Exception as e:
        logger.warning(f"อ่าน shared risk จาก Firebase ไม่ได้ ใช้ค่าเริ่มต้น: {e}")
    return default


def save_shared_risk(risk):
    try:
        primary_ref(SHARED_RISK_PATH).set(risk)
    except Exception as e:
        logger.error(f"บันทึก shared risk ไป Firebase ไม่สำเร็จ: {e}")


class InnovestXTradingBot:
    # ---- ค่าคงที่ที่ปรับได้ ----
    MAX_DAILY_LOSS_PERCENT = 5.0        # หยุดเทรดถ้าขาดทุนสะสมวันนี้เกิน % ของทุนเริ่มวัน (ปรับได้จากหน้าเว็บ)
    MAX_CONSECUTIVE_LOSSES = 3          # หยุดเทรดถ้าขาดทุนติดกันกี่ไม้ (ปรับได้จากหน้าเว็บ)
    DEFAULT_TRADE_SIZE_PERCENT = 95.0   # % ของ "เงินที่แบ่งให้ไม้นี้" ที่จะใช้จริง (หลังหาร slots ว่างแล้ว) — ปรับได้จากหน้าเว็บ
    DEFAULT_MAX_OPEN_POSITIONS = 3      # ถือได้พร้อมกันกี่เหรียญ (ปรับได้จากหน้าเว็บ)
    DEFAULT_TRAILING_STOP_PERCENT = 1.0 # ขึ้นไปแล้วย่อลงจากจุดสูงสุดเกิน % นี้ → ขาย (ปรับได้จากหน้าเว็บ)
    DEFAULT_STOP_LOSS_PERCENT = 3.0     # เข้าซื้อแล้วราคาลงจากต้นทุนเกิน % นี้ → ขายตัดขาดทุนทันที (ปรับได้จากหน้าเว็บ)
    DEFAULT_ROUNDTRIP_FEE_PERCENT = 0.40  # fallback ถ้าดึงค่าธรรมเนียมจริงไม่ได้ — ยึดตามที่ยืนยัน: ทุก 1,000 บาท เก็บไม่เกิน 2 บาทต่อขา (0.2%) รวมไป-กลับ 0.4%
    # เพดาน "กันขยะ" กรณี API คืนค่าผิดเพี้ยนจนคำนวณได้สูงเวอร์ (เช่นหน่วยผิด) — ไม่ใช่เพดานค่าธรรมเนียมจริง
    # ตั้งไว้กว้างกว่า DEFAULT_ROUNDTRIP_FEE_PERCENT มาก เพื่อไม่ให้ไปบีบค่าธรรมเนียมจริงที่สูงกว่า 0.40%
    # ให้เหลือแค่ 0.40% อย่างผิด ๆ (ซึ่งจะทำให้ breakeven ต่ำกว่าจริง แล้วขายทั้งที่ยังขาดทุนสุทธิอยู่)
    MAX_ROUNDTRIP_FEE_PERCENT = 2.0
    MIN_ORDER_THB = 100.0
    MAX_ACCEPTABLE_SLIPPAGE_PERCENT = 1.0  # ถ้าราคาจริงเพี้ยนจากที่คาดเกิน % นี้จะแจ้งเตือน
    REQUEST_TIMEOUT_SEC = 10
    MAX_RETRIES = 3
    FEE_ESTIMATE_PATH = "/api/v1/digital-asset/order/fee/inquiry"
    RECONCILE_INTERVAL_SEC = 300  # เช็ค state กับพอร์ตจริงซ้ำทุกกี่วิระหว่างบอทรันอยู่ (นอกเหนือจากตอน startup) — กันเคสขายเหรียญนอกบอทระหว่างที่บอทยังรันค้างอยู่

    def __init__(self, api_key, api_secret, symbol="BTCTHB", base_currency="THB",
                 target_currency=None, trailing_stop_percent=None, stop_loss_percent=None):
        if trailing_stop_percent is None:
            trailing_stop_percent = self.DEFAULT_TRAILING_STOP_PERCENT
        if stop_loss_percent is None:
            stop_loss_percent = self.DEFAULT_STOP_LOSS_PERCENT
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.base_currency = base_currency

        if target_currency is None:
            target_currency = symbol[:-len(base_currency)] if symbol.endswith(base_currency) else symbol
        self.target_currency = target_currency

        self.host = "api.innovestxonline.com"
        self.base_url = f"https://{self.host}"
        self.state_path = f"bots/{symbol}/state"
        self.market_path = f"market/{symbol}"

        self.trailing_stop_percent = trailing_stop_percent
        self.stop_loss_percent = stop_loss_percent

        # ปรับได้จากหน้าเว็บระหว่างรัน (ไม่ต้อง restart) — ค่าเริ่มต้นใช้ค่าคงที่ของคลาสไปก่อน
        # ตัวแปรพวกนี้จะถูกอัปเดตจริงทุกรอบ loop ใน __main__ จากค่าที่เก็บบน Firebase (bot_control)
        self.max_daily_loss_percent = self.MAX_DAILY_LOSS_PERCENT
        self.max_consecutive_losses = self.MAX_CONSECUTIVE_LOSSES
        self.trade_size_percent = self.DEFAULT_TRADE_SIZE_PERCENT

        self._stop_requested = False
        self._last_history_flush_ts = 0.0
        install_shutdown_handlers()

        self._last_reconcile_ts = 0.0  # บังคับให้ reconcile รอบแรกใน loop ทำงานตามปกติ (ไม่ต้องรอครบ RECONCILE_INTERVAL_SEC)

        self.state = self.load_state()
        LIVE_BOTS[self.symbol] = self

    # ==================== State management ====================
    def load_state(self):
        default_state = {
            "status": "IDLE",  # IDLE / HOLDING / HALTED
            "entry_price": 0.0,
            "highest_price": 0.0,
            "quantity": 0.0,
            "roundtrip_fee_percent": self.DEFAULT_ROUNDTRIP_FEE_PERCENT,
            "price_history": [],  # [[timestamp_sec, price], ...] เก็บย้อนหลัง 3 ชม. อยู่บน DB2
            "quote": {},          # bid/ask/last/spread ล่าสุด อยู่บน DB2
            "trade_date": None,   # "YYYY-MM-DD" (UTC) สำหรับรีเซ็ต circuit breaker รายวัน
            "daily_start_balance": 0.0,
            "daily_realized_pnl": 0.0,
            "consecutive_losses": 0,
            "pending_order": None,  # คำสั่งที่ timeout แล้วยังไม่รู้ว่าเข้าหรือไม่ — ห้ามยิงซ้ำ
            "halt_cleared_by_user": False,
        }
        try:
            saved = primary_ref(self.state_path).get()
            if saved:
                default_state.update(saved)
                logger.info(f"โหลดสถานะบอทจาก Firebase สำเร็จ: status={default_state['status']}")
            else:
                logger.info("ยังไม่มีสถานะเดิมบน Firebase (path ว่าง) — เริ่มจากค่าเริ่มต้น")
        except Exception as e:
            logger.warning(f"อ่านสถานะจาก Firebase ไม่ได้ กำลังใช้ค่าเริ่มต้น: {e}")
        default_state["price_history"] = list(default_state.get("price_history") or [])
        default_state["quote"] = dict(default_state.get("quote") or {})
        self.state = default_state
        self.load_market()
        return self.state

    def load_market(self):
        """โหลด bid/offer + ประวัติราคาจากฐานที่ 2 (หรือฐานหลักถ้ายังไม่เพิ่ม DB2)"""
        try:
            saved = market_ref(self.market_path).get() or {}
        except Exception as e:
            logger.warning(f"[{self.symbol}] อ่านข้อมูลตลาดจาก DB2 ไม่ได้: {e}")
            saved = {}
        hist = saved.get("price_history") or []
        quote = saved.get("quote") or {}
        if not hist and self.state.get("price_history"):
            hist = self.state["price_history"]
            logger.info(f"[{self.symbol}] ย้ายประวัติราคาจากฐานหลักไป path ตลาด")
        self.state["price_history"] = hist
        if quote:
            self.state["quote"] = quote

    def save_state(self):
        try:
            payload = {k: v for k, v in self.state.items() if k not in ("price_history", "quote")}
            primary_ref(self.state_path).set(payload)
        except Exception as e:
            logger.error(f"บันทึกสถานะไป Firebase ไม่สำเร็จ: {e}")

    def save_market(self, force_history=True):
        """บันทึก quote บ่อยได้ แต่ประวัติราคาทั้งก้อนไม่เขียนทุก 15 วิ"""
        try:
            payload = {"quote": self.state.get("quote") or {}}
            now = time.time()
            if force_history or (now - float(getattr(self, "_last_history_flush_ts", 0) or 0)) >= MARKET_HISTORY_FLUSH_SEC:
                payload["price_history"] = self.state.get("price_history") or []
                self._last_history_flush_ts = now
            market_ref(self.market_path).update(payload)
        except Exception as e:
            logger.error(f"บันทึก bid/offer ไปฐานตลาดไม่สำเร็จ: {e}")

    def _handle_shutdown(self, signum, frame):
        _handle_process_shutdown(signum, frame)

    def reconcile_state_on_startup(self):
        """เช็คว่า state ตรงกับยอดจริงในพอร์ตหรือไม่ — เรียกตอน start และเรียกซ้ำเป็นระยะระหว่างบอทรันผ่าน maybe_reconcile_periodically()"""
        logger.info(f"[{self.symbol}] กำลังตรวจสอบสถานะกับยอดจริงในพอร์ต (Reconcile)...")
        _, coin_free, has_pending = self.get_free_balance()

        if has_pending:
            # มีออเดอร์ค้างอยู่ในระบบ (เช่น เพิ่งส่งคำสั่งซื้อ/ขายไปแต่ยังไม่ fill) — ยอดพอร์ตช่วงนี้ไม่นิ่ง
            # ข้าม reconcile รอบนี้ไปก่อน ไม่อัปเดต _last_reconcile_ts เพื่อให้เช็คใหม่ในรอบ loop ถัดไปทันที
            logger.info("Reconcile: มีออเดอร์ค้างอยู่ในระบบ ข้ามการเช็ครอบนี้ (จะลองใหม่รอบถัดไป)")
            return

        rules = self.get_symbol_rules()
        dust_threshold = float(rules["quantity_increment"])

        if self.state["status"] == "HOLDING" and coin_free <= dust_threshold:
            logger.error(f"⚠️ RECONCILE MISMATCH: state บอก HOLDING แต่ในพอร์ตมีแค่ {coin_free} "
                         f"(อาจขายไปแล้วตอนบอทออฟไลน์ หรือขายมือระหว่างบอทรันอยู่) รีเซ็ตเป็น IDLE เพื่อความปลอดภัย")
            self.state.update({"status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0})
            self.save_state()
        elif self.state["status"] == "IDLE" and coin_free > dust_threshold:
            if self.state.get("halt_cleared_by_user"):
                logger.warning(
                    f"[{self.symbol}] มีเหรียญค้าง {coin_free} แต่ผู้ใช้ปลดล็อกแล้ว — "
                    f"ไม่ HALTED ซ้ำ จะถือเป็น HOLDING ด้วยราคาตลาด"
                )
                self.resume_from_halt()
            else:
                logger.error(f"⚠️ RECONCILE MISMATCH: state บอก IDLE แต่มีเหรียญค้างอยู่ {coin_free} "
                             f"(ไม่รู้ต้นทุนจริง) บอทจะไม่เทรดอัตโนมัติจนกว่าจะปลดล็อกจากหน้าเว็บ")
                self.state["status"] = "HALTED"
                self.save_state()
        elif self.state["status"] == "HALTED" and coin_free <= dust_threshold:
            logger.info(f"[{self.symbol}] HALTED แต่เหรียญในพอร์ตหมดแล้ว — ปลดเป็น IDLE ให้เฝ้าต่อได้")
            self.state.update({
                "status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0,
                "halt_cleared_by_user": False,
            })
            self.save_state()
        else:
            logger.info(f"Reconcile ผ่าน: state={self.state['status']} ตรงกับพอร์ตจริง "
                        f"({coin_free} {self.target_currency})")

        self._last_reconcile_ts = time.time()

    def maybe_reconcile_periodically(self):
        """เรียก reconcile ซ้ำเป็นระยะทุก RECONCILE_INTERVAL_SEC ระหว่างบอทรันอยู่ (ไม่ใช่แค่ตอน start)
        กันเคสที่มีการขาย/โอนเหรียญออกนอกบอทระหว่างที่บอทยังรันค้างอยู่ในหน่วยความจำ — ปกติ reconcile_state_on_startup()
        รันแค่ตอนเริ่มโปรเซสเท่านั้น ถ้าไม่เรียกซ้ำ บอทจะไม่มีทางรู้ตัวจนกว่าจะ restart"""
        if time.time() - self._last_reconcile_ts >= self.RECONCILE_INTERVAL_SEC:
            self.reconcile_state_on_startup()

    # ==================== HTTP / signing ====================
    def send_request(self, method, path, query="", body=None, _retry_count=0):
        """ส่ง request พร้อม HMAC-SHA256 — คำสั่งซื้อ/ขายไม่ retry ตอน timeout"""
        return signed_request(
            self.api_key, self.api_secret, method, path, query, body,
            timeout_sec=self.REQUEST_TIMEOUT_SEC,
            max_retries=self.MAX_RETRIES,
            allow_retry=not is_mutating_order_path(path),
            host=self.host,
            _retry_count=_retry_count,
        )

    # ==================== Market data / price history ====================
    def get_latest_quote(self):
        """ดึง last + best bid/offer จาก orderbook แล้วค่อย fallback ticker"""
        path = "/api/v1/digital-asset/orderbook/lvl2"
        body = {"symbol": self.symbol, "depth": 1}
        res = self.send_request("POST", path, body=body)
        quote = parse_market_quote(res)
        if quote_mark_price(quote):
            return quote

        ticker_res = self.send_request(
            "POST", "/api/v1/digital-asset/ticker/subscribe", body={"symbol": self.symbol}
        )
        quote = parse_market_quote(ticker_res)
        if quote_mark_price(quote):
            logger.info(f"[{self.symbol}] ใช้ราคาจาก ticker แทน orderbook")
            return quote

        if res and res.get("code") == "0000":
            logger.warning(f"[{self.symbol}] ดึงราคาไม่ได้ โครงสร้างไม่ตรงที่คาด: {json.dumps(res)[:800]}")
        else:
            logger.warning(f"[{self.symbol}] ดึงราคาล่าสุดล้มเหลว")
        return None

    def get_latest_price(self):
        """ดึงราคาล่าสุดสำหรับประวัติแนวโน้ม (last หรือ mid) พร้อมเก็บ bid/offer ไว้ใน state"""
        quote = self.get_latest_quote()
        if not quote:
            return None
        self.state["quote"] = quote
        return quote_mark_price(quote, "last")

    def _record_price_tick(self, price):
        now = time.time()
        hist = self.state.get("price_history") or []
        if should_append_price_tick(hist, now, price):
            hist.append([now, price])
            cutoff = now - 10800  # เก็บย้อนหลังแค่ 3 ชั่วโมงพอสำหรับกลยุทธ์
            self.state["price_history"] = [t for t in hist if t[0] >= cutoff]
            self.save_market(force_history=False)
        else:
            # ราคาไม่ขยับพอ — อัปเดตแค่ quote ไม่เขียนประวัติทั้งก้อน
            self.save_market(force_history=False)

    def _price_at_offset(self, seconds_ago, tolerance_sec=300):
        history = self.state["price_history"]
        if not history:
            return None
        target = time.time() - seconds_ago
        closest = min(history, key=lambda t: abs(t[0] - target))
        if abs(closest[0] - target) > tolerance_sec:
            return None  # ข้อมูล ณ ช่วงเวลานั้นขาดหายไปเกินไป ถือว่าไม่พอ
        return closest[1]

    def _has_enough_history(self):
        history = self.state["price_history"]
        if not history:
            return False
        return (time.time() - history[0][0]) >= 7200  # อย่างน้อย 2 ชั่วโมง

    def _trend_confidence(self, current_price, price_1h_ago, price_2h_ago, price_3h_ago):
        """wrapper ไว้ให้โค้ดเก่าที่เรียกอยู่ — สูตรจริงอยู่ที่ evaluate_entry_signal()"""
        signal = evaluate_entry_signal(current_price, price_1h_ago, price_2h_ago, price_3h_ago)
        return signal["direction"], signal["confidence"]

    def get_symbol_rules(self):
        products_res = self.send_request("GET", "/api/v1/digital-asset/products")
        decimal_places = 8
        if products_res and products_res.get("code") == "0000":
            for prod in products_res.get("data", []):
                if prod.get("product") == self.target_currency:
                    decimal_places = prod.get("decimalPlaces", 8)
                    break

        symbols_res = self.send_request("GET", "/api/v1/digital-asset/symbols")
        quantity_increment = "0.00001000"
        price_increment = "0.01000000"
        if symbols_res and symbols_res.get("code") == "0000":
            for sym in symbols_res.get("data", []):
                if sym.get("symbol") == self.symbol:
                    quantity_increment = str(sym.get("quantityIncrement", quantity_increment))
                    price_increment = str(sym.get("priceIncrement", price_increment))
                    break

        return {
            "decimal_places": decimal_places,
            "quantity_increment": quantity_increment,  # เก็บเป็น string เพื่อความแม่นยำระดับ Decimal
            "price_increment": price_increment,
        }

    def _floor_to_increment(self, value, increment_str):
        """ปัดลง (floor) ให้ตรงกับ step size ของ exchange โดยใช้ Decimal กันปัญหาความแม่นยำของ float"""
        try:
            value_d = Decimal(str(value))
            increment_d = Decimal(str(increment_str))
            if increment_d == 0:
                return float(value_d)
            floored = (value_d // increment_d) * increment_d
            return float(floored)
        except (InvalidOperation, ZeroDivisionError):
            return value

    def _format_to_increment(self, value, increment_str):
        """ปัดลงแล้วคืนเป็น string ตามจำนวนทศนิยมของ step — ส่งเข้า API เป็น string กัน float เพี้ยน"""
        floored = self._floor_to_increment(value, increment_str)
        try:
            inc = Decimal(str(increment_str))
            exp = -inc.as_tuple().exponent if inc.as_tuple().exponent < 0 else 0
            return format(Decimal(str(floored)), f".{exp}f")
        except (InvalidOperation, ValueError, TypeError):
            return str(floored)

    def estimate_roundtrip_fee_percent(self):
        """พยายามดึงค่าธรรมเนียมจริงจาก API และคำนวณเป็นเปอร์เซ็นต์; ถ้าทำไม่ได้ให้ใช้ค่า default แทน"""
        dummy_amount = 0.01
        dummy_price = 100000.0
        total_value = dummy_amount * dummy_price
        body = {"symbol": self.symbol, "amount": dummy_amount, "price": dummy_price, "side": 0}
        res = self.send_request("POST", self.FEE_ESTIMATE_PATH, body=body)

        if res and res.get("code") == "0000":
            try:
                order_fee_str = res["data"].get("orderFee", "0")
                order_fee = float(order_fee_str)
                if order_fee > 0 and total_value > 0:
                    buy_fee_pct = (order_fee / total_value) * 100
                    roundtrip_pct = buy_fee_pct * 2  # คูณ 2 เพื่อประมาณการค่าฟีแบบไป-กลับ (ซื้อ + ขาย)
                    if roundtrip_pct > self.MAX_ROUNDTRIP_FEE_PERCENT:
                        # ค่าที่ API คืนมาสูงผิดปกติมาก (เกินเพดานกันขยะ) — น่าจะเป็นเพราะ path/พารามิเตอร์ผิด
                        # หรือหน่วยเพี้ยน ไม่ใช่ค่าธรรมเนียมจริงที่สมเหตุสมผล จึงใช้ default แทนดีกว่าใช้ค่าที่ผิดเพี้ยน
                        logger.warning(
                            f"ค่าธรรมเนียมที่คำนวณได้ {roundtrip_pct:.3f}% สูงผิดปกติเกินเพดานกันขยะ "
                            f"{self.MAX_ROUNDTRIP_FEE_PERCENT}% — ใช้ default {self.DEFAULT_ROUNDTRIP_FEE_PERCENT}% แทน "
                            f"(ควรตรวจสอบ FEE_ESTIMATE_PATH)"
                        )
                        return self.DEFAULT_ROUNDTRIP_FEE_PERCENT
                    return roundtrip_pct
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"เกิดข้อผิดพลาดในการคำนวณค่าธรรมเนียมจริง: {e}")

        logger.warning(f"ดึงค่าธรรมเนียมจริงไม่ได้ ใช้ default {self.DEFAULT_ROUNDTRIP_FEE_PERCENT}% แทน "
                       f"(ควรตรวจสอบ FEE_ESTIMATE_PATH และพารามิเตอร์)")
        return self.DEFAULT_ROUNDTRIP_FEE_PERCENT

    # ==================== Account ====================
    def get_free_balance(self):
        balance_res = self.send_request("GET", "/api/v1/digital-asset/account/balance/inquiry")
        thb_free = 0.0
        coin_free = 0.0
        if balance_res and balance_res.get("code") == "0000":
            for asset in balance_res.get("data", []):
                prod_name = asset.get("product")
                total_amount = _safe_float(asset.get("amount"), 0.0) or 0.0
                hold_amount = _safe_float(asset.get("hold"), 0.0) or 0.0
                free_amount = total_amount - hold_amount
                if prod_name == self.base_currency:
                    thb_free = free_amount
                elif prod_name == self.target_currency:
                    coin_free = free_amount

        open_orders_res = self.send_request("GET", "/api/v1/digital-asset/order/open/inquiry")
        has_pending_orders = False
        if open_orders_res and open_orders_res.get("code") == "0000":
            for order in open_orders_res.get("data", []):
                if order.get("symbol") == self.symbol and order.get("orderState") == "Working":
                    has_pending_orders = True
                    break

        return thb_free, coin_free, has_pending_orders

    # ==================== Orders ====================
    def _has_unresolved_order(self):
        pending = self.state.get("pending_order") or {}
        if not pending:
            return False
        age = time.time() - float(pending.get("ts") or 0)
        if age > PENDING_ORDER_TTL_SEC:
            logger.warning(
                f"[{self.symbol}] pending_order อายุ {age:.0f} วิ เกิน {PENDING_ORDER_TTL_SEC} วิ "
                f"— ถือว่าออเดอร์ไม่เข้า อนุญาตยิงใหม่"
            )
            self.state["pending_order"] = None
            self.save_state()
            return False
        return True

    def _recover_order_after_timeout(self, side, max_attempts=5, delay_sec=1.5):
        """หลัง timeout ตอนส่งคำสั่ง: หาออเดอร์ในระบบแทนการยิงซ้ำ"""
        for attempt in range(1, max_attempts + 1):
            open_res = self.send_request("GET", "/api/v1/digital-asset/order/open/inquiry")
            if open_res and open_res.get("code") == "0000":
                for order in open_res.get("data") or []:
                    if order.get("symbol") == self.symbol and int(order.get("side", -1)) == int(side):
                        logger.info(
                            f"[{self.symbol}] พบออเดอร์ค้างในระบบหลัง timeout: {order.get('orderId')}"
                        )
                        self.state["pending_order"] = None
                        self.save_state()
                        return {"code": "0000", "data": order}

            hist_res = self.send_request(
                "POST", "/api/v1/digital-asset/order/history/inquiry",
                body={"symbol": self.symbol},
            )
            if hist_res and hist_res.get("code") == "0000":
                for order in hist_res.get("data") or []:
                    if order.get("symbol") and order.get("symbol") != self.symbol:
                        continue
                    try:
                        order_side = int(order.get("side", -1))
                    except (TypeError, ValueError):
                        continue
                    if order_side != int(side):
                        continue
                    logger.info(
                        f"[{self.symbol}] พบออเดอร์ในประวัติหลัง timeout: {order.get('orderId')}"
                    )
                    self.state["pending_order"] = None
                    self.save_state()
                    return {"code": "0000", "data": order}

            logger.info(
                f"[{self.symbol}] ยังไม่เจอออเดอร์หลัง timeout (ครั้งที่ {attempt}/{max_attempts})"
            )
            time.sleep(delay_sec)

        logger.error(
            f"[{self.symbol}] timeout แล้วยังไม่เจอออเดอร์ในระบบ — จะไม่ยิงซ้ำ "
            f"(pending_order ค้างไว้จนกว่าจะหมดอายุ)"
        )
        return None

    def execute_market_order(self, side, value=None, quantity=None):
        if side == 0 and value is not None and value < self.MIN_ORDER_THB:
            logger.warning(f"ยกเลิกคำสั่งซื้อ: มูลค่า {value} THB ต่ำกว่าขั้นต่ำ {self.MIN_ORDER_THB} THB")
            return None

        if self._has_unresolved_order():
            pending_side = (self.state.get("pending_order") or {}).get("side", side)
            logger.warning(
                f"[{self.symbol}] มีคำสั่งที่ timeout ค้างอยู่ — ไม่ยิงซ้ำ กำลังตรวจสถานะในระบบ"
            )
            return self._recover_order_after_timeout(pending_side)

        path = ORDER_SEND_PATH
        body = {"symbol": self.symbol, "timeInForce": 1, "side": side, "orderType": 1}
        if side == 0 and value is not None:
            body["value"] = round(value, 2)
        elif side == 1 and quantity is not None:
            body["quantity"] = quantity

        logger.info(f"กำลังส่งคำสั่งเทรด: {'ซื้อ' if side == 0 else 'ขาย'} -> {body}")
        res = self.send_request("POST", path, body=body)

        if res and res.get("code") == "TIMEOUT_INDETERMINATE":
            self.state["pending_order"] = {
                "side": side,
                "value": value,
                "quantity": quantity,
                "ts": time.time(),
            }
            self.save_state()
            recovered = self._recover_order_after_timeout(side)
            return recovered

        if res and res.get("code") == "0000":
            self.state["pending_order"] = None
        return res

    def confirm_fill_price(self, order_id, max_attempts=8, delay_sec=1.5):
        """Poll ยืนยันราคาเฉลี่ยที่ match จริง แทนการ sleep คงที่"""
        path = "/api/v1/digital-asset/order/history/inquiry"
        body = {"symbol": self.symbol, "orderId": order_id}

        for attempt in range(1, max_attempts + 1):
            res = self.send_request("POST", path, body=body)
            if res and res.get("code") == "0000":
                orders = res.get("data", [])
                if orders:
                    avg_price = _safe_float(orders[0].get("avgPrice"), 0.0) or 0.0
                    if avg_price > 0:
                        return avg_price
            logger.info(f"รอ order {order_id} matching... (ครั้งที่ {attempt}/{max_attempts})")
            time.sleep(delay_sec)

        logger.warning(f"ไม่สามารถยืนยันราคาเฉลี่ยของ order {order_id} ได้หลัง {max_attempts} ครั้ง")
        return 0.0

    def _check_slippage(self, expected_price, actual_price, context=""):
        if expected_price <= 0:
            return
        slippage_percent = abs(actual_price - expected_price) / expected_price * 100
        if slippage_percent >= self.MAX_ACCEPTABLE_SLIPPAGE_PERCENT:
            logger.error(f"⚠️ SLIPPAGE สูงผิดปกติ ({context}): คาดไว้ {expected_price:.2f} ได้จริง {actual_price:.2f} "
                         f"(ห่างกัน {slippage_percent:.2f}%) — ตลาดผันผวนหนักหรือสภาพคล่องบาง ควรตรวจสอบด้วยตา")
        else:
            logger.info(f"Slippage ({context}): {slippage_percent:.2f}%")

    # ==================== Circuit breaker ====================
    def _today_str(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def _maybe_reset_daily_counters(self):
        today = self._today_str()
        risk = load_shared_risk()
        if risk.get("trade_date") == today:
            return
        thb_free, _, _ = self.get_free_balance()
        
        # คำนวณมูลค่าพอร์ตรวม (Total Equity) ณ เที่ยงคืนเพื่อใช้เป็นฐานคำนวณ % ขาดทุนที่แท้จริง
        total_equity = thb_free
        if self.state["status"] == "HOLDING" and self.state["quantity"] > 0:
            latest_price = self.get_latest_price()
            if latest_price:
                total_equity += (self.state["quantity"] * latest_price)
            else:
                total_equity += (self.state["quantity"] * self.state["entry_price"])
                
        self.state["trade_date"] = today
        self.state["daily_start_balance"] = total_equity  # ใช้ยอดพอร์ตรวมเป็นฐานคำนวณแทนเงินสดว่าง
        self.state["daily_realized_pnl"] = 0.0
        self.state["consecutive_losses"] = 0
        self.save_state()

        risk["trade_date"] = today
        risk["daily_start_balance"] = total_equity
        risk["daily_realized_pnl"] = 0.0
        risk["consecutive_losses"] = 0
        if risk.get("status") == "HALTED":
            risk["status"] = "IDLE"
        save_shared_risk(risk)
        logger.info(f"เริ่มวันใหม่ ({today}) มูลค่าพอร์ตรวมตั้งต้น: {total_equity:.2f} THB")

    def _register_trade_result(self, pnl_thb):
        risk = load_shared_risk()
        risk["daily_realized_pnl"] = float(risk.get("daily_realized_pnl", 0.0) or 0.0) + pnl_thb
        if pnl_thb < 0:
            risk["consecutive_losses"] = int(risk.get("consecutive_losses", 0) or 0) + 1
        else:
            risk["consecutive_losses"] = 0

        # เก็บสำเนาไว้ที่ state ของเหรียญนี้ด้วย เพื่อให้แดชบอร์ดเหรียญเดียวยังอ่านได้
        self.state["daily_realized_pnl"] = risk["daily_realized_pnl"]
        self.state["consecutive_losses"] = risk["consecutive_losses"]
        if risk.get("daily_start_balance"):
            self.state["daily_start_balance"] = risk["daily_start_balance"]

        daily_loss_percent = 0.0
        start = float(risk.get("daily_start_balance", 0.0) or 0.0)
        if start > 0:
            daily_loss_percent = -risk["daily_realized_pnl"] / start * 100

        halt_reason = None
        if daily_loss_percent >= self.max_daily_loss_percent:
            halt_reason = f"ขาดทุนสะสมวันนี้ {daily_loss_percent:.2f}% เกินเพดาน {self.max_daily_loss_percent}%"
        elif risk["consecutive_losses"] >= self.max_consecutive_losses:
            halt_reason = f"ขาดทุนติดกัน {risk['consecutive_losses']} ไม้ ถึงเพดาน {self.max_consecutive_losses} ไม้"

        if halt_reason:
            risk["status"] = "HALTED"
            logger.error(f"🛑 CIRCUIT BREAKER ทำงาน: {halt_reason} — หยุดเปิดออเดอร์ซื้อใหม่ "
                         f"(โพซิชันที่ถืออยู่ยังถูกดูแลต่อ จนกว่าจะขาย) "
                         f"ปลดล็อกอัตโนมัติวันถัดไป หรือกดปลดล็อกจากหน้าเว็บ")
        save_shared_risk(risk)
        self.save_state()

    # ==================== Strategy ====================
    def analyze_trend(self, current_price):
        """ประเมินขาขึ้น/ลงด้วย Price Action 2H — ชม.3 ใช้แค่ห้ามซื้อ"""
        empty = {
            "ready": False,
            "should_buy": False,
            "direction": None,
            "confidence": 0,
            "change_1h": 0.0,
            "change_2h": None,
            "change_3h": None,
            "net_2h": None,
            "vetoed": False,
            "reason": "unknown",
        }
        if current_price is None:
            empty["reason"] = "no_price"
            return empty

        if not self._has_enough_history():
            logger.info(f"[{self.symbol}] ข้อมูลราคาย้อนหลังยังไม่ครบ 2 ชั่วโมง รอสะสมข้อมูลต่อ")
            empty["reason"] = "waiting_history"
            return empty

        price_1h_ago = self._price_at_offset(3600)
        price_2h_ago = self._price_at_offset(7200)
        price_3h_ago = self._price_at_offset(10800)

        if price_1h_ago is None or price_2h_ago is None:
            logger.info(f"[{self.symbol}] ข้อมูลราคาบางช่วงขาดหาย (gap) รอรอบถัดไป")
            empty["reason"] = "gap"
            return empty

        signal = evaluate_entry_signal(current_price, price_1h_ago, price_2h_ago, price_3h_ago)
        quote = self.state.get("quote") or {}
        spread = quote.get("spread_pct")
        if signal["should_buy"] and spread is not None and spread > MAX_SPREAD_PERCENT:
            signal["should_buy"] = False
            signal["reason"] = "wide_spread"
            logger.info(
                f"[{self.symbol}] สเปรด {spread:.3f}% กว้างเกิน {MAX_SPREAD_PERCENT}% "
                f"(bid {quote.get('bid')} / offer {quote.get('ask')}) — ไม่ซื้อ"
            )
        reason_th = REASON_TH.get(signal["reason"], signal["reason"] or "พร้อมซื้อ")
        net_txt = f"{signal['net_2h']:+.2f}%" if signal["net_2h"] is not None else "n/a"
        h2_txt = f"{signal['change_2h']:+.2f}%" if signal["change_2h"] is not None else "n/a"

        logger.info(
            f"[{self.symbol}] ทิศทาง: {signal['direction'] or 'flat'} "
            f"(คะแนน {signal['confidence']}%) ชม.1 {signal['change_1h']:+.2f}% "
            f"ชม.2 {h2_txt} สุทธิ 2ชม. {net_txt}"
        )
        if signal["should_buy"]:
            logger.info(f"[{self.symbol}] ผ่านเกณฑ์เข้าซื้อ")
        else:
            logger.info(f"[{self.symbol}] ไม่เข้าซื้อ — {reason_th}")

        return {
            "ready": True,
            "should_buy": signal["should_buy"],
            "direction": signal["direction"],
            "confidence": signal["confidence"],
            "change_1h": signal["change_1h"],
            "change_2h": signal["change_2h"],
            "change_3h": signal["change_3h"],
            "net_2h": signal["net_2h"],
            "vetoed": signal["vetoed"],
            "reason": signal["reason"],
        }

    def try_enter_position(self, current_price, available_slots=1):
        """เข้าซื้อ — ใช้ % ของ 'เงินที่แบ่งให้ไม้นี้' ไม่ใช่ % ของเงินว่างทั้งหมด

        available_slots คือจำนวนช่องว่างที่เหลือ ณ ตอนนี้ (รวมไม้นี้ด้วย) เช่นถือได้สูงสุด 3
        ช่อง ตอนนี้ว่าง 3 ช่อง → available_slots=3 → เงินที่แบ่งให้ไม้นี้ = เงินว่างทั้งหมด / 3
        วิธีนี้กันไม่ให้ไม้แรกใช้เงินเกือบหมดจนไม้ถัดไปในเหรียญอื่นไม่มีที่ให้ซื้อ
        (ของเดิมใช้ % ของเงินว่างทั้งก้อนตรงๆ ถ้าตั้ง trade_size_percent สูงๆ เช่น 95%
        จะเหลือเงินไม่พอให้เหรียญอื่นซื้อในรอบเดียวกันแทบทุกครั้ง)
        """
        if self.state["status"] != "IDLE":
            return False

        quote = self.state.get("quote") or {}
        spread = quote.get("spread_pct")
        if spread is not None and spread > MAX_SPREAD_PERCENT:
            logger.info(
                f"[{self.symbol}] ข้ามการซื้อ: สเปรด {spread:.3f}% กว้างเกิน {MAX_SPREAD_PERCENT}%"
            )
            return False
        expected_buy = quote_mark_price(quote, "buy") or current_price

        thb_free, _, has_pending = self.get_free_balance()
        if has_pending:
            logger.info(f"[{self.symbol}] ข้ามการซื้อ: มีออเดอร์ค้างอยู่ในระบบ")
            return False
        if thb_free <= self.MIN_ORDER_THB:
            logger.info(f"[{self.symbol}] เงินว่างไม่พอสำหรับซื้อขั้นต่ำ (คงเหลือ {thb_free:.2f} THB)")
            return False

        slots = max(1, int(available_slots or 1))
        while slots > 1 and round((thb_free / slots) * (self.trade_size_percent / 100.0), 2) < self.MIN_ORDER_THB:
            slots -= 1
        rules = self.get_symbol_rules()
        buy_value = round((thb_free / slots) * (self.trade_size_percent / 100.0), 2)
        if buy_value < self.MIN_ORDER_THB:
            logger.info(
                f"[{self.symbol}] เงินที่แบ่งได้ {buy_value:.2f} THB ต่ำกว่าขั้นต่ำ {self.MIN_ORDER_THB} THB — ข้าม"
            )
            return False
        order_res = self.execute_market_order(side=0, value=buy_value)

        if not (order_res and order_res.get("code") == "0000"):
            logger.warning(f"[{self.symbol}] ยิงออเดอร์ซื้อล้มเหลว: {order_res}")
            return False

        order_id = (order_res.get("data") or {}).get("orderId")
        if not order_id:
            logger.warning(f"[{self.symbol}] ซื้อสำเร็จแต่ไม่มี orderId: {order_res}")
            return False
        logger.info(f"[{self.symbol}] ✔ ส่งคำสั่งซื้อสำเร็จ Order ID: {order_id} กำลังยืนยันราคาจับคู่จริง...")

        avg_price = self.confirm_fill_price(order_id)
        if not avg_price:
            avg_price = expected_buy or current_price
            logger.warning(f"[{self.symbol}] ยืนยันราคาจับคู่จริงไม่ได้ ใช้ราคา offer ณ ขณะนั้นแทน (ควรตรวจสอบ order ด้วยมือ)")
        if not avg_price:
            logger.error(f"[{self.symbol}] ไม่มีราคาสำหรับคำนวณจำนวนเหรียญ — ตั้ง HOLDING ชั่วคราว รอ reconcile")
            self.state.update({"status": "HOLDING", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0})
            self.save_state()
            return True

        self._check_slippage(expected_buy or current_price, avg_price, "ซื้อ")

        fee_pct = self.estimate_roundtrip_fee_percent()
        one_way_fee_factor = (fee_pct / 2) / 100

        raw_qty = (buy_value / avg_price) * (1 - one_way_fee_factor)
        estimated_qty = self._floor_to_increment(raw_qty, rules["quantity_increment"])

        self.state.update({
            "status": "HOLDING",
            "entry_price": avg_price,
            "highest_price": avg_price,
            "quantity": estimated_qty,
            "roundtrip_fee_percent": fee_pct,
        })
        self.save_state()

        logger.info(
            f"[{self.symbol}] ซื้อสำเร็จ ต้นทุนเฉลี่ย {avg_price} THB จำนวน {estimated_qty} "
            f"(ใช้เงิน {buy_value:.2f} จาก {slots} ช่องว่าง, ค่าธรรมเนียม round-trip โดยประมาณ {fee_pct:.3f}%)"
        )
        return True

    def run_strategy(self, current_price):
        if self.state["status"] == "IDLE":
            logger.info(f"[{self.symbol}] สถานะ IDLE กำลังวิเคราะห์ราคาปัจจุบัน: {current_price} THB")
            signal = self.analyze_trend(current_price)
            if not signal["should_buy"]:
                return
            self.try_enter_position(current_price)

        elif self.state["status"] == "HOLDING":
            entry_price = self.state["entry_price"]
            highest_price = self.state["highest_price"]
            qty = self.state["quantity"]
            fee_pct = self.state.get("roundtrip_fee_percent", self.DEFAULT_ROUNDTRIP_FEE_PERCENT)
            quote = self.state.get("quote") or {}
            sell_mark = quote_mark_price(quote, "sell") or current_price

            logger.info(
                f"[{self.symbol}] สถานะ HOLDING ต้นทุน {entry_price} THB "
                f"last {current_price} THB bid {quote.get('bid') or '-'} offer {quote.get('ask') or '-'}"
            )

            if current_price > highest_price:
                self.state["highest_price"] = current_price
                self.save_state()
                highest_price = current_price
                logger.info(f"🚀 จุดสูงสุดใหม่: {current_price} THB")

            # trailing stop ควรทำงานเฉพาะตอนที่ "เคยขึ้นไปเหนือทุนจริง ๆ" แล้วเท่านั้น
            # ถ้ายังไม่เคย (highest_price ยัง <= entry_price เพราะราคาไหลลงตรงๆ ตั้งแต่ซื้อ)
            # ต้องปล่อยให้ stop_loss_percent เป็นตัวตัดสินใจแทน ไม่งั้นถ้า trailing_stop_percent
            # ตั้งไว้ต่ำกว่า stop_loss_percent (ค่า default 1% < 3%) trailing จะแย่งตัดขาย
            # ก่อน stop loss ทุกครั้งที่ราคาลง ทำให้ "หยุดขาดทุนสูงสุด" ที่ตั้งไว้ 3% ไม่เคยมีผลจริง
            has_peaked = highest_price > entry_price
            trailing_threshold = highest_price * (1 - self.trailing_stop_percent / 100)
            stop_loss_threshold = entry_price * (1 - self.stop_loss_percent / 100)
            breakeven_price = entry_price * (1 + fee_pct / 100)  # breakeven จริงหลังหักค่าธรรมเนียม

            if sell_mark <= stop_loss_threshold:
                logger.warning("🚨 ถึงจุด Hard Stop Loss ขายทันทีเพื่อจำกัดความเสียหาย")
                self.sell_position(qty, sell_mark)
            elif has_peaked and sell_mark <= trailing_threshold:
                # ขายทันทีที่ราคาย่อลงมาเกิน trailing_stop_percent จากจุดสูงสุด ไม่รอเช็ค breakeven อีกต่อไป
                # (ของเดิมจะถือต่อถ้ายังไม่คุ้มค่าธรรมเนียม ทำให้บางครั้งไม่ขายเลย)
                if sell_mark > breakeven_price:
                    logger.info(f"💰 ถึงจุด Trailing Stop ({self.trailing_stop_percent}%) และคุ้มค่าธรรมเนียม "
                                f"(breakeven {breakeven_price:.2f}) ขายล็อกกำไร")
                else:
                    logger.warning(f"⚠️ ถึงจุด Trailing Stop ({self.trailing_stop_percent}%) แต่ยังไม่คุ้มค่าธรรมเนียม "
                                   f"(breakeven {breakeven_price:.2f}) — ขายตามคำสั่งใหม่ (ไม่ถือรอแล้ว)")
                self.sell_position(qty, sell_mark)

    def sell_position(self, qty, current_price=None):
        _, coin_free, has_pending = self.get_free_balance()
        if has_pending:
            logger.info("ข้ามการขาย: มีออเดอร์ค้างอยู่ในระบบ")
            return

        sell_qty = min(float(qty or 0), float(coin_free or 0))
        rules = self.get_symbol_rules()
        sell_qty_str = self._format_to_increment(sell_qty, rules["quantity_increment"])
        try:
            sell_qty = float(Decimal(sell_qty_str))
        except (InvalidOperation, ValueError, TypeError):
            sell_qty = self._floor_to_increment(sell_qty, rules["quantity_increment"])

        if sell_qty <= 0:
            logger.warning(f"ไม่มียอดเหรียญ {self.target_currency} พร้อมขาย (คงเหลือจริง {coin_free})")
            return

        order_res = self.execute_market_order(side=1, quantity=sell_qty_str)
        if order_res and order_res.get("code") == "TIMEOUT_INDETERMINATE":
            logger.error(f"[{self.symbol}] timeout ตอนขาย — ไม่ถือว่าขายสำเร็จ จะไม่ยิงซ้ำ")
            return
        if not (order_res and order_res.get("code") == "0000"):
            logger.warning(f"สั่งขายล้มเหลว: {order_res}")
            return

        order_id = (order_res.get("data") or {}).get("orderId")
        if not order_id:
            logger.warning(f"สั่งขายล้มเหลว: ไม่มี orderId {order_res}")
            return
        sell_avg_price = self.confirm_fill_price(order_id)

        if sell_avg_price > 0 and current_price is not None:
            self._check_slippage(current_price, sell_avg_price, "ขาย")

        entry_price = float(self.state.get("entry_price") or 0.0)
        fee_pct = self.state.get("roundtrip_fee_percent", self.DEFAULT_ROUNDTRIP_FEE_PERCENT)

        if sell_avg_price <= 0:
            _, coin_left, still_pending = self.get_free_balance()
            dust = float(self.get_symbol_rules()["quantity_increment"])
            if still_pending:
                logger.warning(
                    f"[{self.symbol}] ขายแล้วยัง matching อยู่ — คง HOLDING ไว้ ไม่ถือว่าขายสำเร็จ"
                )
                return
            if coin_left > dust:
                logger.warning(
                    f"[{self.symbol}] ยืนยันราคาขายไม่ได้ และยังมีเหรียญในพอร์ต {coin_left} "
                    f"— คง HOLDING ไม่ถือว่าขายสำเร็จ"
                )
                return
            logger.warning(
                f"[{self.symbol}] เหรียญหมดจากพอร์ตแต่ยืนยันราคาขายไม่ได้ — ตั้ง IDLE โดยไม่บันทึก PnL"
            )
            self.state.update({
                "status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0,
                "pending_order": None,
            })
            self.save_state()
            return

        pnl_thb = net_pnl_thb(entry_price, sell_avg_price, sell_qty, fee_pct)
        logger.info(
            f"✔ ขายสำเร็จ ราคาเฉลี่ย {sell_avg_price} PnL สุทธิหลังหักค่าธรรมเนียม "
            f"{fee_pct:.3f}% = {pnl_thb:.2f} THB"
        )

        self.state.update({
            "status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0,
            "pending_order": None,
        })
        self._register_trade_result(pnl_thb)

    def resume_from_halt(self):
        """ปลด HALTED ระดับเหรียญจากหน้าเว็บ — ถ้ายังมีของค้าง ให้ถือเป็น HOLDING ด้วยราคาตลาด"""
        _, coin_free, has_pending = self.get_free_balance()
        if has_pending:
            logger.warning(f"[{self.symbol}] ยังมีออเดอร์ค้าง — ยังไม่ปลด HALTED รอรอบถัดไป")
            return False
        rules = self.get_symbol_rules()
        dust_threshold = float(rules["quantity_increment"])
        if coin_free > dust_threshold:
            px = self.get_latest_price()
            if not px:
                logger.error(f"[{self.symbol}] ปลด HALTED ไม่ได้ชั่วคราว: ดึงราคาตลาดไม่ได้")
                return False
            logger.warning(
                f"[{self.symbol}] ปลด HALTED ตามคำขอจากหน้าเว็บ: ยังมี {coin_free} เหรียญ "
                f"— ตั้ง HOLDING ต้นทุนประมาณ {px} (ราคาตลาด ณ ตอนปลด) เพื่อให้บอทขายต่อได้"
            )
            self.state.update({
                "status": "HOLDING",
                "entry_price": px,
                "highest_price": max(px, float(self.state.get("highest_price") or 0.0)),
                "quantity": coin_free,
                "halt_cleared_by_user": True,
            })
            self.save_state()
            return True
        logger.info(f"[{self.symbol}] ปลด HALTED ตามคำขอจากหน้าเว็บ: ไม่มีเหรียญค้าง — กลับ IDLE")
        self.state.update({
            "status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0,
            "halt_cleared_by_user": True,
        })
        self.save_state()
        return True

    # ==================== Main loop ====================
    def poll_price(self):
        """ดึงราคาและบันทึกประวัติ — ใช้ตอนสแกนหลายเหรียญ"""
        self.maybe_reconcile_periodically()
        price = self.get_latest_price()
        if price is not None:
            self._record_price_tick(price)
        return price

    def run_once(self):
        """รันหนึ่งรอบของ loop หลัก (ไม่ sleep) — เรียกจาก run() หรือจาก supervisor loop ใน __main__"""
        self.maybe_reconcile_periodically()
        self._maybe_reset_daily_counters()

        price = self.get_latest_price()
        if price is None:
            return
        self._record_price_tick(price)

        risk = load_shared_risk()
        if risk.get("status") == "HALTED" and self.state.get("status") != "HOLDING":
            logger.warning(f"[{self.symbol}] ⛔ บัญชีถูก HALTED จาก Circuit Breaker — ไม่เปิดออเดอร์ซื้อใหม่")
            return

        self.run_strategy(price)

    def run(self, poll_interval_sec=60):
        """เรียกใช้ตรงๆ ได้ถ้าไม่ต้องการ dashboard control (เทรดเหรียญเดียวตลอด ไม่มีปุ่มหยุด)"""
        logger.info(f"เริ่มบอทเทรด {self.symbol} (poll ทุก {poll_interval_sec} วิ) — กด Ctrl+C เพื่อหยุดอย่างปลอดภัย")
        self.reconcile_state_on_startup()

        while not self._stop_requested:
            try:
                self.run_once()
            except Exception:
                logger.exception("เกิดข้อผิดพลาดไม่คาดคิดใน main loop — บอทจะพยายามทำงานต่อในรอบถัดไป")

            for _ in range(poll_interval_sec):
                if self._stop_requested:
                    break
                time.sleep(1)

        logger.info("บอทหยุดทำงานเรียบร้อย (state ถูกบันทึกแล้ว)")


# ==================== Dashboard (หน้าเว็บสถานะ + ควบคุมบอท) ====================
DASHBOARD_TEMPLATE = Template("""<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sentinel · เฝ้า ${watch_count} เหรียญ</title>
<style>
  :root {
    --bg: #1E1C18; --card: #2A2822; --border: #3D3A32;
    --text: #EDE9DD; --text-soft: #9C9585;
    --accent: #E08A65; --accent-soft: #3D2C22;
    --green: #85B893; --green-soft: #24332A;
    --red: #E08277; --red-soft: #3A2523;
  }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    -webkit-font-smoothing:antialiased; min-height:100vh;
    display:flex; justify-content:center; padding:0 0 40px; }
  .app { width:100%; max-width:480px; }

  .topbar { position:sticky; top:0; z-index:10; display:flex; align-items:center;
    justify-content:space-between; padding:18px 20px; background:rgba(42,40,34,0.95);
    backdrop-filter:blur(8px); border-bottom:1px solid var(--border); }
  .brand { display:flex; align-items:center; gap:10px; }
  .spark { width:22px; height:22px; flex-shrink:0; }
  .spark path { fill: var(--accent); }
  .brand-title { font-size:15px; font-weight:700; letter-spacing:-0.01em; color:var(--text); }
  .brand-sub { font-size:12px; color:var(--text-soft); margin-top:1px; }
  .status-pill { display:inline-flex; align-items:center; gap:6px; padding:6px 12px;
    border-radius:999px; font-size:12px; font-weight:600; letter-spacing:0.02em; }
  .status-holding { background:var(--green-soft); color:var(--green); }
  .status-idle { background:#33312A; color:var(--text-soft); }
  .status-halted { background:var(--red-soft); color:var(--red); }
  .dot { width:7px; height:7px; border-radius:50%; background:currentColor; }
  .status-holding .dot { animation:pulse 1.8s ease-in-out infinite; }
  @keyframes pulse { 0%,100%{opacity:1;transform:scale(1);} 50%{opacity:.45;transform:scale(.8);} }
  @media (prefers-reduced-motion:reduce){ .status-holding .dot{animation:none;} }

  .hero { padding:32px 24px 20px; text-align:center; }
  .hero-label { font-size:13px; color:var(--text-soft); font-weight:500; }
  .hero-price { font-size:clamp(34px,9vw,44px); font-weight:800; letter-spacing:-0.02em;
    font-variant-numeric:tabular-nums; margin-top:4px; }
  .hero-decimal { font-size:.55em; color:var(--text-soft); font-weight:700; }
  .hero-delta { display:inline-block; margin-top:10px; font-size:13px; font-weight:600;
    padding:4px 10px; border-radius:999px; }
  .hero-delta.positive { color:var(--green); background:var(--green-soft); }
  .hero-delta.negative { color:var(--red); background:var(--red-soft); }
  .hero-sub { margin-top:10px; font-size:13px; color:var(--text-soft); }

  .banner { margin:0 20px 12px; padding:12px 14px; border-radius:12px; font-size:13px;
    font-weight:600; line-height:1.5; }
  .banner-danger { background:var(--red-soft); color:var(--red); }
  .banner-info { background:var(--accent-soft); color:var(--accent); }
  .banner-warning { background:#3D3420; color:#E8C468; }

  .progress-card { margin:0 20px 16px; background:var(--card); border:1px solid var(--border);
    border-radius:16px; padding:14px 16px; }
  .progress-label { font-size:12px; color:var(--text-soft); font-weight:600; }
  .progress-bar { margin-top:8px; height:8px; border-radius:999px; background:#33312A; overflow:hidden; }
  .progress-fill { height:100%; background:var(--accent); border-radius:999px; }
  .progress-sub { margin-top:6px; font-size:12px; color:var(--text-soft); }

  .grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; padding:0 20px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:16px;
    padding:14px 16px; }
  .card-label { font-size:12px; color:var(--text-soft); font-weight:500; }
  .card-value { font-size:17px; font-weight:700; margin-top:4px; font-variant-numeric:tabular-nums;
    letter-spacing:-0.01em; }
  .card-value.positive { color:var(--green); }
  .card-value.negative { color:var(--red); }
  .card-value.accent { color:var(--accent); }
  .card-sub { font-size:11px; color:var(--text-soft); margin-top:2px; }

  .coin-list { display:flex; flex-direction:column; gap:10px; padding:16px 20px 0; }
  .coin-card { background:var(--card); border:1px solid var(--border); border-radius:16px; padding:14px 16px; }
  .coin-card.is-holding { border-color:#3A5344; }
  .coin-head { display:flex; align-items:baseline; justify-content:space-between; gap:8px; }
  .coin-sym { font-size:15px; font-weight:700; letter-spacing:0.02em; }
  .coin-price { font-size:15px; font-weight:700; font-variant-numeric:tabular-nums; }
  .coin-meta { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
  .chip { font-size:11px; font-weight:600; padding:3px 8px; border-radius:999px; background:#33312A; color:var(--text-soft); }
  .chip.up { background:var(--green-soft); color:var(--green); }
  .chip.down { background:var(--red-soft); color:var(--red); }
  .chip.hold { background:var(--green-soft); color:var(--green); }
  .coin-sub { margin-top:8px; font-size:12px; color:var(--text-soft); line-height:1.45; }

  .control { margin:28px 20px 0; }
  .control-heading { display:flex; align-items:baseline; justify-content:space-between;
    margin-bottom:14px; padding:0 2px; }
  .control-title { font-size:15px; font-weight:700; }
  .control-heading-sub { font-size:12px; color:var(--text-soft); }

  .control-card { background:var(--card); border:1px solid var(--border); border-radius:16px;
    padding:16px; margin-bottom:12px; }
  .control-card:last-child { margin-bottom:0; }
  .control-card.halted { border-color:var(--red);
    background:linear-gradient(180deg, var(--red-soft), var(--card) 60%); }

  .toggle-row { display:flex; align-items:center; justify-content:space-between; gap:12px; }
  .label-row { display:flex; align-items:center; }
  .control-label { font-size:13px; font-weight:600; color:var(--text); }
  .control-sub { font-size:12px; color:var(--text-soft); margin-top:5px; line-height:1.5; }

  .watch-list { display:flex; flex-direction:column; gap:8px; margin-top:12px; }
  .watch-row { display:flex; align-items:center; justify-content:space-between; gap:10px;
    padding:10px 12px; background:var(--bg); border:1px solid var(--border); border-radius:12px;
    cursor:pointer; }
  .watch-row-main { min-width:0; }
  .watch-row input[type="checkbox"] { width:18px; height:18px; flex-shrink:0; accent-color:var(--red); }
  .watch-remove { display:flex; align-items:center; gap:6px; font-size:11px; font-weight:700;
    color:var(--red); flex-shrink:0; white-space:nowrap; }
  .watch-sym { font-size:13px; font-weight:700; letter-spacing:0.03em; }
  .watch-meta { font-size:11px; color:var(--text-soft); margin-top:2px; }

  .symbol-input { width:100%; margin-top:10px; padding:10px 12px; border:1px solid var(--border);
    border-radius:10px; font-size:14px; font-weight:600; letter-spacing:.02em;
    text-transform:uppercase; background:var(--bg); color:var(--text); }
  .symbol-input:focus { outline:none; border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-soft); }

  .quick-picks { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
  .pick { font-size:11px; font-weight:600; color:var(--text-soft); background:var(--bg);
    border:1px solid var(--border); border-radius:999px; padding:6px 10px; cursor:pointer; }
  .pick:hover { border-color:var(--accent); color:var(--accent); }

  .field-row { display:flex; align-items:center; gap:8px; margin-top:12px; }
  .password-input { flex:1; min-width:0; padding:10px 12px; border:1px solid var(--border);
    border-radius:10px; font-size:13px; background:var(--bg); color:var(--text); }
  .password-input:focus { outline:none; border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-soft); }

  .btn { border:none; border-radius:10px; padding:10px 16px; font-size:13px; font-weight:700;
    cursor:pointer; white-space:nowrap; flex-shrink:0; }
  .btn:hover { filter:brightness(1.08); }
  .btn:disabled { opacity:.45; cursor:not-allowed; }
  .btn-accent { background:var(--accent); color:#fff; }
  .btn-neutral { background:#33312A; color:var(--text); }
  .btn-danger { background:var(--red); color:#fff; }
  .btn-sm { padding:8px 12px; font-size:12px; }

  .pause-row { display:flex; align-items:center; gap:8px; font-size:13px; color:var(--text); cursor:pointer; }
  .pause-row input { width:16px; height:16px; flex-shrink:0; }
  .settings-fold { margin-top:12px; border-top:1px solid var(--border); padding-top:4px; }
  .settings-fold:first-child { margin-top:0; border-top:none; padding-top:0; }
  .settings-fold > summary { display:flex; align-items:center; gap:8px; cursor:pointer;
    list-style:none; padding:10px 2px; user-select:none; }
  .settings-fold > summary::-webkit-details-marker { display:none; }
  .settings-fold > summary::after { content:""; width:7px; height:7px; margin-left:auto;
    border-right:2px solid var(--text-soft); border-bottom:2px solid var(--text-soft);
    transform:rotate(45deg); flex-shrink:0; transition:transform .15s ease; }
  .settings-fold[open] > summary::after { transform:rotate(-135deg); margin-top:4px; }
  .settings-summary-title { font-size:13px; font-weight:700; color:var(--text); }
  .settings-preview { font-size:11px; font-weight:600; color:var(--text-soft);
    white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .settings-body { padding:0 0 4px; }

  footer { display:flex; align-items:center; justify-content:center; gap:8px; margin-top:20px;
    font-size:12px; color:var(--text-soft); }
  .sep { opacity:.5; }
  .refresh-link { color:var(--accent); text-decoration:none; font-weight:600; }
</style>
</head>
<body>
  <div class="app">
    <header class="topbar">
      <div class="brand">
        <svg class="spark" viewBox="0 0 24 24"><path d="M12 0c.6 3.8 2.2 6.4 5 8.2 2.8 1.8 5.4 2 7 2-1.6 0-4.2.2-7 2C14.2 14 12.6 16.6 12 20.4c-.6-3.8-2.2-6.4-5-8.2-2.8-1.8-5.4-2-7-2 1.6 0 4.2-.2 7-2C9.8 6.4 11.4 3.8 12 0z"/></svg>
        <div>
          <div class="brand-title">Sentinel</div>
          <div class="brand-sub">เฝ้า ${watch_count} เหรียญ · Price Action 2H</div>
        </div>
      </div>
      <div class="status-pill ${status_class}"><span class="dot"></span> ${status_label}</div>
    </header>
    <section class="hero">
      <div class="hero-label">${hero_label}</div>
      <div class="hero-price">${hero_price}</div>
      ${hero_delta_html}
      ${freshness_html}
    </section>
    ${banners_html}
    ${progress_html}
    <div class="grid">
      ${cards_html}
    </div>
    ${coin_cards_html}
<section class="control">

<div class="control-heading">
<div class="control-title">ควบคุมบอท</div>
<div class="control-heading-sub">บันทึกแล้วมีผลทันที</div>
</div>

${unlock_button_html}

<form class="control-card" method="POST" action="/control/settings">
<label class="pause-row">
<input type="checkbox" name="paused" value="1" ${paused_checked}>
หยุดเทรดอัตโนมัติชั่วคราว (${pause_sub})
</label>
<details id="settings-panel" class="settings-fold">
<summary>
<span class="settings-summary-title">ตั้งค่าบอท</span>
<span class="settings-preview">เงิน ${trade_size_percent}% · ${max_open_positions} ช่อง · trail ${trailing_stop_percent}% · SL ${stop_loss_percent}%</span>
</summary>
<div class="settings-body">
<div class="control-sub">ปรับค่าไหนก็ได้พร้อมกัน ใส่รหัสครั้งเดียวแล้วกดบันทึกทีเดียว</div>

<div class="control-label" style="font-size:12px; margin-top:16px;">อัตราเงินที่ใช้เข้าซื้อต่อไม้ (% ของเงินที่แบ่งให้ไม้นี้)</div>
<input class="symbol-input" style="text-transform:none;" type="number" step="1" min="1" max="100"
name="trade_size_percent" value="${trade_size_percent}">
<div class="control-sub">ระบบแบ่งเงินว่างตามจำนวนช่องว่าง ณ ตอนนั้นให้อัตโนมัติแล้ว (เช่นว่าง 3 ช่อง = แบ่งเงินว่างเป็น 3 ส่วน)
ค่านี้คือ % ของเงินที่แบ่งให้ไม้นี้เท่านั้นที่จะใช้จริง ไม่ต้องคำนวณเองอีก</div>

<div class="control-label" style="font-size:12px; margin-top:16px;">ถือได้พร้อมกันกี่เหรียญ</div>
<input class="symbol-input" style="text-transform:none;" type="number" step="1" min="1" max="10"
name="max_open_positions" value="${max_open_positions}">
<div class="control-sub">${size_hint_html}</div>

<div class="control-label" style="font-size:12px; margin-top:16px;">ขาดทุนติดกันกี่ไม้ถึงหยุด</div>
<input class="symbol-input" style="text-transform:none;" type="number" step="1" min="1" max="20"
name="max_consecutive_losses" value="${max_consecutive_losses}">
<div class="control-sub">นับรวมทุกเหรียญในบัญชี ถ้าขาดทุนติดกันครบจำนวนนี้ จะหยุดเปิดไม้ใหม่</div>

<div class="control-label" style="font-size:12px; margin-top:16px;">ขาดทุนสูงสุดที่ยอมรับต่อวัน (%)</div>
<input class="symbol-input" style="text-transform:none;" type="number" step="0.1" min="0.1" max="100"
name="max_daily_loss_percent" value="${max_daily_loss_percent}">
<div class="control-sub">ถ้าขาดทุนสะสมวันนี้ถึง % นี้ บอทจะหยุดเปิดไม้ใหม่ (โพซิชันที่ถืออยู่ยังถูกดูแลต่อ)</div>

<div class="control-label" style="font-size:12px; margin-top:16px;">ขึ้นไปแล้วย่อลงจากจุดสูงสุดเกิน (%) → ขาย (Trailing Stop)</div>
<input class="symbol-input" style="text-transform:none;" type="number" step="0.1" min="0.1" max="20"
name="trailing_stop_percent" value="${trailing_stop_percent}">
<div class="control-sub">ใช้เฉพาะตอนที่ราคาเคยขึ้นเหนือทุนจริงแล้วเท่านั้น ถ้าซื้อแล้วราคาไม่เคยขึ้นเลย จะใช้ Stop Loss ด้านล่างตัดสินใจแทน</div>

<div class="control-label" style="font-size:12px; margin-top:16px;">เข้าซื้อแล้วราคาลงจากต้นทุนเกิน (%) → ขายตัดขาดทุน (Stop Loss)</div>
<input class="symbol-input" style="text-transform:none;" type="number" step="0.1" min="0.1" max="50"
name="stop_loss_percent" value="${stop_loss_percent}">
<div class="control-sub">เข้าซื้อแล้วราคาไม่ขึ้น ร่วงจากราคาต้นทุนเกิน % นี้ จะขายทันทีเพื่อจำกัดความเสียหาย</div>
</div>
</details>
<div class="field-row">
${password_field_html}
<button type="submit" class="btn btn-accent">บันทึกการตั้งค่าทั้งหมด</button>
</div>
</form>

<form class="control-card" method="POST" action="/control/watchlist">
<details id="watchlist-panel" class="settings-fold">
<summary>
<span class="settings-summary-title">เหรียญที่เฝ้าอยู่</span>
<span class="settings-preview">${watch_count} เหรียญ</span>
</summary>
<div class="settings-body">
<div class="control-sub">ติ๊กเอาออก แล้วพิมพ์เพิ่มเหรียญใหม่ได้เลย (คั่นด้วยช่องว่างหรือจุลภาคถ้าเพิ่มหลายตัว) กดบันทึกครั้งเดียวจบ</div>

${watchlist_rows_html}

<div class="control-label" style="font-size:12px; margin-top:16px;">เพิ่มเหรียญ (พิมพ์ได้หลายตัว)</div>
<input class="symbol-input" type="text" name="add_symbols" id="add-symbols-input" placeholder="เช่น ETHTHB SOLTHB"
autocapitalize="characters" autocomplete="off">
<div class="quick-picks">
<span class="pick" onclick="addSymbolPick('BTCTHB')">BTCTHB</span>
<span class="pick" onclick="addSymbolPick('ETHTHB')">ETHTHB</span>
<span class="pick" onclick="addSymbolPick('XRPTHB')">XRPTHB</span>
<span class="pick" onclick="addSymbolPick('SOLTHB')">SOLTHB</span>
</div>
</div>
</details>
<div class="field-row">
${password_field_html}
<button type="submit" class="btn btn-accent">บันทึกรายการเหรียญ</button>
</div>
</form>

</section>
    <footer>
      <span>อัปเดตล่าสุด ${last_updated}</span>
      <span class="sep">·</span>
      <a href="#" class="refresh-link" onclick="location.reload();return false;">รีเฟรชตอนนี้</a>
    </footer>
  </div>
<script>
(function(){
  function persistFold(id, key){
    var d = document.getElementById(id);
    if (!d) return;
    try { if (sessionStorage.getItem(key) === "1") d.open = true; } catch (e) {}
    d.addEventListener("toggle", function(){
      try { sessionStorage.setItem(key, d.open ? "1" : "0"); } catch (e) {}
    });
  }
  persistFold("settings-panel", "sentinel-settings-open");
  persistFold("watchlist-panel", "sentinel-watchlist-open");
  setTimeout(function(){
    var ae = document.activeElement;
    if (ae && (ae.tagName === "INPUT" || ae.tagName === "TEXTAREA" || ae.tagName === "SELECT")) return;
    location.reload();
  }, 30000);
})();
function addSymbolPick(sym){
  var el = document.getElementById('add-symbols-input');
  if (!el) return;
  var parts = el.value.split(/[\\s,]+/).filter(Boolean);
  if (parts.indexOf(sym) === -1) { parts.push(sym); }
  el.value = parts.join(' ');
}
</script>
</body>
</html>""")


def _fmt_thb(value):
    try:
        return f"{value:,.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _display_symbol(symbol):
    symbol = symbol or ""
    if symbol.endswith("THB") and len(symbol) > 3:
        return f"{symbol[:-3]}/{symbol[-3:]}"
    return symbol


def _render_card(label, value, sub="", value_class=""):
    sub_html = f'<div class="card-sub">{sub}</div>' if sub else ""
    return f'<div class="card"><div class="card-label">{label}</div><div class="card-value {value_class}">{value}</div>{sub_html}</div>'


def _trend_from_state(state):
    """สูตรเดียวกับบอท: 2 ชม.เป็นหลัก ชม.3 แค่ห้ามซื้อ"""
    empty = {
        "direction": None,
        "confidence": 0,
        "current": None,
        "change_1h": 0.0,
        "change_2h": None,
        "change_3h": None,
        "net_2h": None,
        "elapsed": 0.0,
        "should_buy": False,
        "vetoed": False,
        "reason": "unknown",
    }
    history = state.get("price_history") or []
    if not history:
        return empty
    current = history[-1][1]
    now = time.time()
    empty["current"] = current
    empty["elapsed"] = max(0.0, now - history[0][0])

    def price_at(seconds_ago, tolerance=300):
        target = now - seconds_ago
        closest = min(history, key=lambda t: abs(t[0] - target))
        if abs(closest[0] - target) > tolerance:
            return None
        return closest[1]

    p1 = price_at(3600)
    p2 = price_at(7200)
    p3 = price_at(10800)
    if empty["elapsed"] < 7200:
        empty["reason"] = "waiting_history"
        if p1 and current:
            empty["change_1h"] = _pct_change(current, p1) or 0.0
        return empty
    signal = evaluate_entry_signal(current, p1, p2, p3)
    signal["current"] = current
    signal["elapsed"] = empty["elapsed"]
    return signal


def render_dashboard(watchlist, states_by_symbol, control, shared_risk):
    watchlist = watchlist or []
    states_by_symbol = states_by_symbol or {}
    shared_risk = shared_risk or {}
    now = time.time()
    PRICE_STALE_THRESHOLD_SEC = 150

    daily_pnl = float(shared_risk.get("daily_realized_pnl", 0.0) or 0.0)
    daily_start = float(shared_risk.get("daily_start_balance", 0.0) or 0.0)
    consecutive_losses = int(shared_risk.get("consecutive_losses", 0) or 0)
    account_halted = shared_risk.get("status") == "HALTED"
    active_max_consecutive_losses = control.get("max_consecutive_losses", InnovestXTradingBot.MAX_CONSECUTIVE_LOSSES)
    max_open = int(control.get("max_open_positions", InnovestXTradingBot.DEFAULT_MAX_OPEN_POSITIONS) or 3)

    # เหรียญที่ถูกลบออกจาก watchlist แล้ว แต่ backend ยังถืออยู่ (รอขายก่อนเลิกเฝ้า)
    # ต้องรวมเข้ามานับ/แสดงผลด้วย ไม่งั้นตัวเลข "ถืออยู่ X/Y ช่อง" และ PnL รวมบนหน้าเว็บจะต่ำกว่าความจริง
    pending_remove = [
        s for s in (RUNNING_WATCHLIST.get("value") or [])
        if s not in watchlist
    ]
    display_symbols = list(dict.fromkeys(watchlist + pending_remove))
    halted_coins = [
        s for s in display_symbols
        if (states_by_symbol.get(s) or {}).get("status") == "HALTED"
    ]

    holding_symbols = []
    newest_ts = None
    total_unrealized = 0.0
    waiting_history = 0

    for sym in display_symbols:
        st = states_by_symbol.get(sym) or {}
        if st.get("status") == "HOLDING":
            holding_symbols.append(sym)
            hist = st.get("price_history") or []
            px = hist[-1][1] if hist else None
            entry = float(st.get("entry_price", 0.0) or 0.0)
            qty = float(st.get("quantity", 0.0) or 0.0)
            if px is not None and entry:
                total_unrealized += qty * (px - entry)
        hist = st.get("price_history") or []
        if hist:
            ts = hist[-1][0]
            if newest_ts is None or ts > newest_ts:
                newest_ts = ts
            if (now - hist[0][0]) < 7200 and st.get("status") != "HOLDING":
                waiting_history += 1
        elif st.get("status") != "HOLDING":
            waiting_history += 1

    holding_count = len(holding_symbols)
    if account_halted:
        status_class, status_label = "status-halted", "HALTED"
    elif halted_coins:
        status_class, status_label = "status-halted", "HALTED"
    elif holding_count:
        status_class, status_label = "status-holding", f"HOLDING {holding_count}"
    else:
        status_class, status_label = "status-idle", "SCANNING"

    hero_label = f"ถืออยู่ {holding_count} / {max_open} ช่อง"
    hero_price = f'{holding_count}<span class="hero-decimal">/{max_open}</span>'
    if holding_count:
        sign = "+" if total_unrealized >= 0 else ""
        cls = "positive" if total_unrealized >= 0 else "negative"
        names = ", ".join(_display_symbol(s) for s in holding_symbols)
        hero_delta_html = (
            f'<div class="hero-delta {cls}">{sign}{_fmt_thb(total_unrealized)} ฿ ยังไม่ขาย</div>'
            f'<div class="hero-sub">ถืออยู่: {names}</div>'
        )
    else:
        hero_delta_html = '<div class="hero-sub">รอสัญญาณขาขึ้นจากเหรียญในรายการ...</div>'

    freshness_html = ""
    price_age_sec = (now - newest_ts) if newest_ts is not None else None
    if price_age_sec is not None:
        age_min = int(price_age_sec // 60)
        age_sec = int(price_age_sec % 60)
        age_text = f"{age_min} นาที {age_sec} วิ" if age_min > 0 else f"{age_sec} วิ"
        if price_age_sec > PRICE_STALE_THRESHOLD_SEC:
            freshness_html = f'<div class="hero-delta negative">ราคาอาจไม่สด — ดึงมาเมื่อ {age_text} ที่แล้ว</div>'
        else:
            freshness_html = f'<div class="hero-sub">อัปเดตราคาเมื่อ {age_text} ที่แล้ว</div>'

    banners = []
    if price_age_sec is not None and price_age_sec > PRICE_STALE_THRESHOLD_SEC:
        age_min = int(price_age_sec // 60)
        banners.append(
            f'<div class="banner banner-danger">ราคาที่แสดงอาจไม่ใช่ราคาสด (เก่าไปแล้ว {age_min}+ นาที) '
            f"— ตรวจสอบ log บน Render ว่าดึงราคาล้มเหลวซ้ำๆ หรือไม่</div>"
        )

    reason = ""
    if account_halted:
        active_threshold = control.get("max_daily_loss_percent", InnovestXTradingBot.MAX_DAILY_LOSS_PERCENT)
        daily_loss_percent = -daily_pnl / daily_start * 100 if daily_start > 0 else 0.0
        if daily_loss_percent >= active_threshold:
            reason = f"ขาดทุนสะสมวันนี้เกิน {active_threshold:.1f}%"
        elif consecutive_losses >= active_max_consecutive_losses:
            reason = f"ขาดทุนติดกัน {consecutive_losses} ไม้ ครบเพดาน {active_max_consecutive_losses} ไม้"
        else:
            reason = "Circuit breaker ทำงาน (ดูรายละเอียดใน log)"
        banners.append(
            f'<div class="banner banner-danger">บอทหยุดเปิดไม้ใหม่ — {reason} '
            f"(โพซิชันที่ถืออยู่ยังถูกดูแลต่อ / ปลดล็อกอัตโนมัติวันถัดไป หรือกดปลดล็อกด้านล่าง)</div>"
        )

    if halted_coins:
        names = ", ".join(_display_symbol(s) for s in halted_coins)
        banners.append(
            f'<div class="banner banner-danger">เหรียญค้างโดยไม่รู้ต้นทุน: {names} '
            f"— กดปลดล็อกด้านล่าง เพื่อให้บอทถือต่อด้วยราคาตลาดแล้วขายได้ "
            f"(ของเดิมปลดจากเว็บไม่ได้)</div>"
        )

    if control.get("paused"):
        banners.append(
            '<div class="banner banner-info">บอทหยุดเทรดชั่วคราว (สั่งจากหน้านี้) '
            "— จะไม่เปิดออเดอร์ซื้อใหม่ แต่ยังดูแลไม้ที่ถืออยู่ (stop loss / trailing)</div>"
        )

    if pending_remove:
        banners.append(
            f'<div class="banner banner-info">กำลังรอขายก่อนเอาออกจากรายการ: '
            f"{', '.join(_display_symbol(s) for s in pending_remove)}</div>"
        )

    if not os.environ.get("DASHBOARD_PASSWORD"):
        banners.append(
            '<div class="banner banner-warning">หน้านี้ยังไม่มีรหัสผ่านป้องกัน '
            "ใครมีลิงก์นี้ก็สั่งหยุด/เพิ่มเหรียญได้ — แนะนำตั้งค่า DASHBOARD_PASSWORD ใน Render</div>"
        )

    banners_html = "".join(banners)

    progress_html = ""
    if waiting_history and holding_count == 0:
        # แสดงความคืบหน้าของเหรียญที่ช้าที่สุด
        slowest_elapsed = 7200
        slowest_has_hist = False
        for sym in watchlist:
            st = states_by_symbol.get(sym) or {}
            hist = st.get("price_history") or []
            if hist:
                slowest_has_hist = True
                slowest_elapsed = min(slowest_elapsed, max(0.0, now - hist[0][0]))
            else:
                slowest_elapsed = 0.0
                slowest_has_hist = False
                break
        if slowest_elapsed < 7200:
            pct = min(100, slowest_elapsed / 7200 * 100)
            minutes_done = int(slowest_elapsed // 60)
            minutes_left = max(0, 120 - minutes_done)
            sub = (
                f"{minutes_done} / 120 นาที · เหลืออีกประมาณ {minutes_left} นาที "
                f"({waiting_history} เหรียญยังไม่ครบข้อมูล)"
                if slowest_has_hist else
                "ยังไม่มีข้อมูลราคาเลย รอรอบแรกของบอท"
            )
            progress_html = (
                '<section class="progress-card">'
                '<div class="progress-label">กำลังสะสมข้อมูลราคา (ต้องครบ 2 ชั่วโมงต่อเหรียญก่อนเริ่มซื้อ)</div>'
                f'<div class="progress-bar"><div class="progress-fill" style="width:{pct:.0f}%"></div></div>'
                f'<div class="progress-sub">{sub}</div>'
                "</section>"
            )

    cards = [
        _render_card(
            "กำไรวันนี้ (รับรู้แล้ว)",
            f"{'+' if daily_pnl >= 0 else ''}{_fmt_thb(daily_pnl)} ฿",
            value_class="positive" if daily_pnl >= 0 else "negative",
        ),
        _render_card("ทุนเริ่มต้นวันนี้", f"{_fmt_thb(daily_start)} ฿"),
        _render_card("ขาดทุนติดกัน", f"{consecutive_losses} / {active_max_consecutive_losses} ไม้"),
        _render_card("เหรียญในรายการ", f"{len(watchlist)} ตัว", sub=f"ถือได้สูงสุด {max_open} ช่อง", value_class="accent"),
    ]
    cards_html = "".join(cards)

    coin_parts = ['<div class="coin-list">']
    if not display_symbols:
        coin_parts.append(
            '<div class="coin-card"><div class="coin-sub">ยังไม่มีเหรียญในรายการ — เพิ่มด้านล่างได้เลย</div></div>'
        )
    for sym in display_symbols:
        st = states_by_symbol.get(sym) or {}
        status = st.get("status", "IDLE")
        trend = _trend_from_state(st)
        direction = trend.get("direction")
        confidence = trend.get("confidence") or 0
        current = trend.get("current")
        change_1h = trend.get("change_1h") or 0.0
        change_2h = trend.get("change_2h")
        net_2h = trend.get("net_2h")
        elapsed = trend.get("elapsed") or 0.0
        holding_cls = " is-holding" if status == "HOLDING" else ""
        price_txt = f"{_fmt_thb(current)} ฿" if current is not None else "—"
        chips = []
        if sym in pending_remove:
            chips.append('<span class="chip down">รอขายก่อนเอาออก</span>')
        if status == "HOLDING":
            chips.append('<span class="chip hold">ถืออยู่</span>')
            entry = float(st.get("entry_price", 0.0) or 0.0)
            if current is not None and entry:
                d = (current - entry) / entry * 100
                chips.append(f'<span class="chip {"up" if d >= 0 else "down"}">{d:+.2f}% จากต้นทุน</span>')
        elif status == "HALTED":
            chips.append('<span class="chip down">HALTED — ของค้างไม่รู้ต้นทุน</span>')
        else:
            chips.append('<span class="chip" >เฝ้าอยู่</span>')
        if trend.get("should_buy") and status != "HOLDING":
            chips.append('<span class="chip up">พร้อมซื้อ</span>')
        elif trend.get("vetoed"):
            chips.append('<span class="chip down">ชม.3 ห้ามซื้อ</span>')
        if direction == "up":
            chips.append(f'<span class="chip up">ขาขึ้น {confidence}%</span>')
        elif direction == "down":
            chips.append('<span class="chip down">ขาลง</span>')
        if elapsed and elapsed < 7200 and status != "HOLDING":
            chips.append(f'<span class="chip">รอข้อมูล {int(elapsed // 60)}/120 นาที</span>')
        if change_1h:
            chips.append(f'<span class="chip {"up" if change_1h >= 0 else "down"}">1 ชม. {change_1h:+.2f}%</span>')
        if change_2h is not None:
            chips.append(f'<span class="chip {"up" if change_2h >= 0 else "down"}">2 ชม. {change_2h:+.2f}%</span>')
        if net_2h is not None:
            chips.append(f'<span class="chip {"up" if net_2h >= 0 else "down"}">สุทธิ 2 ชม. {net_2h:+.2f}%</span>')
        quote = st.get("quote") or {}
        bid, ask, spread = quote.get("bid"), quote.get("ask"), quote.get("spread_pct")
        if bid and ask:
            chips.append(f'<span class="chip">bid {_fmt_thb(bid)} / offer {_fmt_thb(ask)}</span>')
        if spread is not None:
            chips.append(
                f'<span class="chip {"down" if spread > MAX_SPREAD_PERCENT else ""}">สเปรด {spread:.2f}%</span>'
            )
        if status == "HOLDING":
            sub = f"ต้นทุน {_fmt_thb(float(st.get('entry_price', 0) or 0))} ฿ · ดูแลด้วย trailing / stop loss แบบเดิม"
        elif status == "HALTED":
            sub = "ของค้างไม่รู้ต้นทุน — กดปลดล็อกด้านล่าง เพื่อให้บอทถือต่อด้วยราคาตลาดแล้วขายได้"
        elif elapsed and elapsed < 7200:
            sub = "รอข้อมูลครบ 2 ชั่วโมงก่อนเริ่มประเมินเข้าซื้อ"
        else:
            reason_th = REASON_TH.get(trend.get("reason"), "")
            if trend.get("should_buy"):
                sub = "ผ่านเกณฑ์ 2 ชม. — รอช่องว่างเพื่อเข้าซื้อ"
            elif reason_th:
                sub = reason_th
            else:
                sub = f"เกณฑ์: ชม.1 ≥ {MIN_CHANGE_1H_PERCENT}% และชม.2 ขึ้น, สุทธิ 2 ชม. ≥ {MIN_NET_2H_PERCENT}%"
        coin_parts.append(
            f'<article class="coin-card{holding_cls}">'
            f'<div class="coin-head"><div class="coin-sym">{_display_symbol(sym)}</div>'
            f'<div class="coin-price">{price_txt}</div></div>'
            f'<div class="coin-meta">{"".join(chips)}</div>'
            f'<div class="coin-sub">{sub}</div>'
            "</article>"
        )
    coin_parts.append("</div>")
    coin_cards_html = "".join(coin_parts)

    password_field_html = ""
    if os.environ.get("DASHBOARD_PASSWORD"):
        password_field_html = (
            '<input class="password-input" type="password" name="password" placeholder="รหัส" style="margin-top:8px;">'
        )

    watch_rows = ['<div class="watch-list">']
    for sym in watchlist:
        st = states_by_symbol.get(sym) or {}
        status = st.get("status", "IDLE")
        holding = status == "HOLDING"
        meta = "ถืออยู่ — ติ๊กเอาออกได้ บอทจะรอขายก่อนแล้วค่อยเลิกเฝ้า" if holding else "เฝ้าอยู่ รอสัญญาณขาขึ้น"
        watch_rows.append(
            '<label class="watch-row">'
            f'<div class="watch-row-main"><div class="watch-sym">{_display_symbol(sym)}</div>'
            f'<div class="watch-meta">{meta}</div></div>'
            f'<span class="watch-remove">เอาออก <input type="checkbox" name="remove_symbols" value="{sym}"></span>'
            "</label>"
        )
    if not watchlist:
        watch_rows.append('<div class="watch-meta">ยังว่าง — เพิ่มเหรียญด้านล่าง</div>')
    watch_rows.append("</div>")
    watchlist_rows_html = "".join(watch_rows)

    if control.get("paused"):
        pause_sub = "ตอนนี้: หยุดอยู่ (ไม่เปิดออเดอร์ใหม่)"
        paused_checked = "checked"
    else:
        pause_sub = "ตอนนี้: กำลังสแกนเหรียญในรายการ"
        paused_checked = ""

    last_updated = datetime.now().strftime("%H:%M:%S")
    suggested = max(1, int(round(100.0 / max_open)))
    size_hint_html = (
        "ระบบแบ่งเงินตามช่องว่างให้อัตโนมัติแล้ว จะถือกี่เหรียญพร้อมกันก็ปรับตรงนี้ได้เลย "
        "ไม่ต้องคำนวณอัตราเงินต่อไม้เองอีก"
    )

    unlock_button_html = ""
    if account_halted or halted_coins:
        if account_halted:
            unlock_label = "บอทถูกล็อกไม่ให้เปิดไม้ใหม่ (HALTED)"
            unlock_sub = reason or "Circuit breaker ทำงาน"
        else:
            names = ", ".join(_display_symbol(s) for s in halted_coins)
            unlock_label = "มีเหรียญค้างโดยไม่รู้ต้นทุน (HALTED)"
            unlock_sub = f"{names} — กดปลดล็อกเพื่อให้บอทถือต่อด้วยราคาตลาดปัจจุบัน แล้วขายได้"
        unlock_button_html = f'''<form class="control-card halted" method="POST" action="/control/unlock">
      <div class="control-label">{unlock_label}</div>
        <div class="control-sub">{unlock_sub}</div>
        <div class="field-row">
      {password_field_html}
      <button type="submit" class="btn btn-danger">ปลดล็อกตอนนี้</button>
      </div>
    </form>'''

    return DASHBOARD_TEMPLATE.safe_substitute(
        watch_count=str(len(watchlist)),
        status_class=status_class,
        status_label=status_label,
        hero_label=hero_label,
        hero_price=hero_price,
        hero_delta_html=hero_delta_html,
        freshness_html=freshness_html,
        banners_html=banners_html,
        progress_html=progress_html,
        cards_html=cards_html,
        coin_cards_html=coin_cards_html,
        unlock_button_html=unlock_button_html,
        pause_sub=pause_sub,
        paused_checked=paused_checked,
        watchlist_rows_html=watchlist_rows_html,
        max_open_positions=max_open,
        suggested_trade_size=suggested,
        size_hint_html=size_hint_html,
        max_daily_loss_percent=control.get("max_daily_loss_percent", InnovestXTradingBot.MAX_DAILY_LOSS_PERCENT),
        trade_size_percent=control.get("trade_size_percent", InnovestXTradingBot.DEFAULT_TRADE_SIZE_PERCENT),
        max_consecutive_losses=active_max_consecutive_losses,
        trailing_stop_percent=control.get("trailing_stop_percent", InnovestXTradingBot.DEFAULT_TRAILING_STOP_PERCENT),
        stop_loss_percent=control.get("stop_loss_percent", InnovestXTradingBot.DEFAULT_STOP_LOSS_PERCENT),
        password_field_html=password_field_html,
        last_updated=last_updated,
    )


# ==================== Web Service (หน้าเว็บสถานะ + ควบคุมบอท สำหรับ Render) ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        try:
            control = load_control()
            watchlist = list(control.get("watchlist") or [])
            # แสดงทั้งที่เฝ้าอยู่ และที่รอขายก่อนเอาออก
            visible = list(dict.fromkeys(watchlist + list(RUNNING_WATCHLIST.get("value") or [])))
            states = {}
            for sym in visible:
                try:
                    states[sym] = load_symbol_view(sym)
                except Exception:
                    states[sym] = {}
            shared_risk = load_shared_risk()
            html = render_dashboard(watchlist, states, control, shared_risk)
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
        except Exception as e:
            logger.error(f"Dashboard render error: {e}")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write("Bot is running! (dashboard error, see logs)".encode("utf-8"))

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length).decode("utf-8") if length else ""
            fields = urllib.parse.parse_qs(raw)

            required_password = os.environ.get("DASHBOARD_PASSWORD")
            given_password = fields.get("password", [""])[0]

            if required_password and given_password != required_password:
                self.send_response(403)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write("รหัสผ่านไม่ถูกต้อง".encode("utf-8"))
                return

            if self.path == "/control/settings":
                control = load_control()
                changes = []

                control["paused"] = "paused" in fields
                changes.append(f"paused={control['paused']}")

                raw_value = fields.get("trade_size_percent", [""])[0].strip()
                try:
                    value = float(raw_value)
                except ValueError:
                    value = None
                if value is not None and 1 <= value <= 100:
                    control["trade_size_percent"] = value
                    changes.append(f"trade_size_percent={value}%")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธค่าอัตราเงินที่ใช้เข้าซื้อ: '{raw_value}' (ต้องอยู่ระหว่าง 1-100)")

                raw_value = fields.get("max_open_positions", [""])[0].strip()
                try:
                    value = int(float(raw_value))
                except ValueError:
                    value = None
                if value is not None and 1 <= value <= 10:
                    control["max_open_positions"] = value
                    changes.append(f"max_open_positions={value}")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธจำนวนช่องถือ: '{raw_value}' (ต้องอยู่ระหว่าง 1-10)")

                raw_value = fields.get("max_consecutive_losses", [""])[0].strip()
                try:
                    value = int(float(raw_value))
                except ValueError:
                    value = None
                if value is not None and 1 <= value <= 20:
                    control["max_consecutive_losses"] = value
                    changes.append(f"max_consecutive_losses={value}")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธจำนวนไม้ขาดทุนติดกัน: '{raw_value}' (ต้องอยู่ระหว่าง 1-20)")

                raw_value = fields.get("max_daily_loss_percent", [""])[0].strip()
                try:
                    value = float(raw_value)
                except ValueError:
                    value = None
                if value is not None and 0.1 <= value <= 100:
                    control["max_daily_loss_percent"] = value
                    changes.append(f"max_daily_loss_percent={value}%")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธค่าขาดทุนสูงสุดต่อวัน: '{raw_value}' (ต้องอยู่ระหว่าง 0.1-100)")

                raw_value = fields.get("trailing_stop_percent", [""])[0].strip()
                try:
                    value = float(raw_value)
                except ValueError:
                    value = None
                if value is not None and 0.1 <= value <= 20:
                    control["trailing_stop_percent"] = value
                    changes.append(f"trailing_stop_percent={value}%")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธค่า Trailing Stop: '{raw_value}' (ต้องอยู่ระหว่าง 0.1-20)")

                raw_value = fields.get("stop_loss_percent", [""])[0].strip()
                try:
                    value = float(raw_value)
                except ValueError:
                    value = None
                if value is not None and 0.1 <= value <= 50:
                    control["stop_loss_percent"] = value
                    changes.append(f"stop_loss_percent={value}%")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธค่า Stop Loss: '{raw_value}' (ต้องอยู่ระหว่าง 0.1-50)")

                save_control(control)
                logger.info(f"[เว็บควบคุม] บันทึกการตั้งค่า: {', '.join(changes)}")

            elif self.path == "/control/watchlist":
                control = load_control()
                watchlist = list(control.get("watchlist") or [])

                remove_set = set()
                for item in fields.get("remove_symbols", []):
                    item = item.strip().upper()
                    if item:
                        remove_set.add(item)
                new_watchlist = [s for s in watchlist if s not in remove_set]

                raw_add = fields.get("add_symbols", [""])[0]
                added = []
                catalog = fetch_exchange_symbol_catalog()
                for item in re.split(r"[\s,]+", raw_add.strip()):
                    item = item.strip().upper()
                    if not item:
                        continue
                    sym, err = validate_watchlist_symbol(item, catalog=catalog)
                    if err:
                        logger.warning(f"[เว็บควบคุม] ปฏิเสธเหรียญ: {err}")
                        continue
                    if sym not in new_watchlist and sym not in added:
                        added.append(sym)
                new_watchlist.extend(added)

                control["watchlist"] = new_watchlist
                save_control(control)
                logger.info(
                    f"[เว็บควบคุม] บันทึกรายการเหรียญ: เอาออก {sorted(remove_set) or '-'} "
                    f"เพิ่ม {added or '-'} ตอนนี้เฝ้า {new_watchlist}"
                )

            elif self.path == "/control/unlock":
                control = load_control()
                control["unlock_requested"] = True
                save_control(control)
                logger.info("[เว็บควบคุม] ได้รับคำขอปลดล็อกบอท — จะมีผลในรอบ loop ถัดไป")

            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers()

        except Exception as e:
            logger.error(f"Dashboard control error: {e}")
            self.send_response(500)
            self.end_headers()


def start_dummy_health_check_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    server.serve_forever()


def _sync_bot_settings(bot, control):
    if bot.max_daily_loss_percent != control["max_daily_loss_percent"]:
        logger.info(
            f"[{bot.symbol}] ปรับเพดานขาดทุนต่อวันจาก {bot.max_daily_loss_percent}% "
            f"เป็น {control['max_daily_loss_percent']}% (สั่งจากหน้าเว็บ)"
        )
        bot.max_daily_loss_percent = control["max_daily_loss_percent"]
    if bot.trade_size_percent != control["trade_size_percent"]:
        logger.info(
            f"[{bot.symbol}] ปรับอัตราเงินที่ใช้เข้าซื้อจาก {bot.trade_size_percent}% "
            f"เป็น {control['trade_size_percent']}% (สั่งจากหน้าเว็บ)"
        )
        bot.trade_size_percent = control["trade_size_percent"]
    if bot.max_consecutive_losses != control["max_consecutive_losses"]:
        logger.info(
            f"[{bot.symbol}] ปรับจำนวนไม้ขาดทุนติดกันก่อนหยุดจาก {bot.max_consecutive_losses} "
            f"เป็น {control['max_consecutive_losses']} ไม้ (สั่งจากหน้าเว็บ)"
        )
        bot.max_consecutive_losses = control["max_consecutive_losses"]
    if bot.trailing_stop_percent != control["trailing_stop_percent"]:
        logger.info(
            f"[{bot.symbol}] ปรับ Trailing Stop จาก {bot.trailing_stop_percent}% "
            f"เป็น {control['trailing_stop_percent']}% (สั่งจากหน้าเว็บ)"
        )
        bot.trailing_stop_percent = control["trailing_stop_percent"]
    if bot.stop_loss_percent != control["stop_loss_percent"]:
        logger.info(
            f"[{bot.symbol}] ปรับ Stop Loss จาก {bot.stop_loss_percent}% "
            f"เป็น {control['stop_loss_percent']}% (สั่งจากหน้าเว็บ)"
        )
        bot.stop_loss_percent = control["stop_loss_percent"]


def maybe_reset_shared_daily(bots):
    """รีเซ็ตวงจรรายวันระดับบัญชีครั้งเดียว ใช้มูลค่าพอร์ตรวมทุกเหรียญที่ถืออยู่"""
    risk = load_shared_risk()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if risk.get("trade_date") == today:
        return risk

    sample = next(iter(bots.values()), None)
    thb_free = 0.0
    if sample is not None:
        thb_free, _, _ = sample.get_free_balance()

    total_equity = thb_free
    for bot in bots.values():
        if bot.state.get("status") == "HOLDING" and float(bot.state.get("quantity", 0) or 0) > 0:
            latest_price = bot.get_latest_price()
            if latest_price:
                total_equity += bot.state["quantity"] * latest_price
            else:
                total_equity += bot.state["quantity"] * float(bot.state.get("entry_price", 0) or 0)

    risk["trade_date"] = today
    risk["daily_start_balance"] = total_equity
    risk["daily_realized_pnl"] = 0.0
    risk["consecutive_losses"] = 0
    if risk.get("status") == "HALTED":
        logger.info("วันใหม่: ปลดล็อก Circuit Breaker อัตโนมัติ — เปิดไม้ใหม่ได้อีกครั้ง")
        risk["status"] = "IDLE"
    save_shared_risk(risk)

    for bot in bots.values():
        bot.state["trade_date"] = today
        bot.state["daily_start_balance"] = total_equity
        bot.state["daily_realized_pnl"] = 0.0
        bot.state["consecutive_losses"] = 0
        # ไม่ปลด per-coin HALTED จาก reconcile อัตโนมัติ — กันการซื้อซ้ำทับเหรียญที่ยังไม่รู้ต้นทุน
        bot.save_state()

    logger.info(f"เริ่มวันใหม่ ({today}) มูลค่าพอร์ตรวมตั้งต้น: {total_equity:.2f} THB")
    return risk


if __name__ == "__main__":
    api_key = os.environ.get("INVX_API_KEY")
    api_secret = os.environ.get("INVX_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit(
            "กรุณาตั้งค่า environment variable INVX_API_KEY และ INVX_API_SECRET ก่อนรัน "
            "(ห้าม hardcode API key/secret ลงในไฟล์โค้ดโดยเด็ดขาด)"
        )

    health_thread = threading.Thread(target=start_dummy_health_check_server, daemon=True)
    health_thread.start()

    POLL_INTERVAL_SEC = 15

    control = load_control()
    save_control(control)

    bots = LIVE_BOTS
    install_shutdown_handlers()

    def get_or_create_bot(symbol):
        if symbol not in bots:
            logger.info(f"เริ่มเฝ้าเหรียญ {symbol}")
            bot = InnovestXTradingBot(
                api_key=api_key, api_secret=api_secret, symbol=symbol,
                trailing_stop_percent=control["trailing_stop_percent"],
                stop_loss_percent=control["stop_loss_percent"],
            )
            bot.max_daily_loss_percent = control["max_daily_loss_percent"]
            bot.trade_size_percent = control["trade_size_percent"]
            bot.max_consecutive_losses = control["max_consecutive_losses"]
            bot.reconcile_state_on_startup()
            bots[symbol] = bot
        return bots[symbol]

    for sym in control["watchlist"]:
        get_or_create_bot(sym)

    RUNNING_WATCHLIST["value"] = list(bots.keys())
    RUNNING_SYMBOL["value"] = (control["watchlist"] or [DEFAULT_SYMBOL])[0]
    logger.info(
        f"เริ่มบอทเฝ้า {RUNNING_WATCHLIST['value']} "
        f"(ถือได้สูงสุด {control['max_open_positions']} เหรียญ, poll ทุก {POLL_INTERVAL_SEC} วิ)"
    )

    stop_all = False
    while not stop_all:
        control = load_control()
        wanted = list(control.get("watchlist") or [])

        for sym in wanted:
            get_or_create_bot(sym)

        for sym in list(bots.keys()):
            if sym in wanted:
                continue
            bot = bots[sym]
            if bot.state.get("status") == "HOLDING" or float(bot.state.get("quantity", 0) or 0) > 0:
                logger.info(f"มีคำขอเอา {sym} ออกจากรายการ แต่ยังถืออยู่ — รอขายก่อน แล้วค่อยเลิกเฝ้า")
            else:
                logger.info(f"เลิกเฝ้า {sym} ตามคำสั่งจากหน้าเว็บ")
                del bots[sym]

        RUNNING_WATCHLIST["value"] = list(bots.keys())
        if wanted:
            RUNNING_SYMBOL["value"] = wanted[0]
        elif bots:
            RUNNING_SYMBOL["value"] = next(iter(bots))

        for bot in bots.values():
            _sync_bot_settings(bot, control)

        if control["unlock_requested"]:
            risk = load_shared_risk()
            if risk.get("status") == "HALTED":
                logger.info("ปลดล็อกบอทตามคำขอจากหน้าเว็บ: HALTED -> IDLE (รีเซ็ตขาดทุนติดกันเป็น 0)")
                risk["status"] = "IDLE"
                risk["consecutive_losses"] = 0
                save_shared_risk(risk)
            resume_failed = False
            for bot in bots.values():
                if bot.state.get("status") == "HALTED":
                    if not bot.resume_from_halt():
                        resume_failed = True
            if not resume_failed:
                control["unlock_requested"] = False
                save_control(control)
            else:
                logger.warning("ปลด HALTED บางเหรียญไม่สำเร็จ — จะลองใหม่ในรอบถัดไป")

        if bots:
            maybe_reset_shared_daily(bots)

        if not bots:
            logger.info("รายการเฝ้าว่าง — รอเพิ่มเหรียญจากหน้าเว็บ")
        elif control["paused"]:
            logger.info("บอทหยุดชั่วคราว (สั่งโดยผู้ใช้ผ่านหน้าเว็บ) — ไม่เปิดออเดอร์ซื้อใหม่ แต่ยังดูแลไม้ที่ถืออยู่ (stop loss / trailing)")
            for bot in bots.values():
                try:
                    px = bot.poll_price()
                    if px is not None and bot.state.get("status") == "HOLDING":
                        bot.run_strategy(px)
                except Exception:
                    logger.exception(f"[{bot.symbol}] ดึงราคา/ดูแลโพซิชันตอนหยุดชั่วคราวล้มเหลว")
        else:
            prices = {}
            for bot in bots.values():
                try:
                    prices[bot.symbol] = bot.poll_price()
                except Exception:
                    logger.exception(f"[{bot.symbol}] ดึงราคาล้มเหลว")
                time.sleep(0.2)

            for bot in bots.values():
                px = prices.get(bot.symbol)
                if px is None:
                    continue
                if bot.state.get("status") == "HOLDING":
                    try:
                        bot.run_strategy(px)
                    except Exception:
                        logger.exception(f"[{bot.symbol}] ดูแลโพซิชันล้มเหลว")

            risk = load_shared_risk()
            holding_count = sum(1 for b in bots.values() if b.state.get("status") == "HOLDING")
            slots = int(control["max_open_positions"]) - holding_count

            if risk.get("status") == "HALTED":
                logger.warning("บัญชีถูก HALTED จาก Circuit Breaker — ไม่เปิดออเดอร์ซื้อใหม่ (โพซิชันที่ถืออยู่ยังถูกดูแล)")
            elif slots <= 0:
                logger.info(f"ถือครบ {holding_count}/{control['max_open_positions']} ช่อง — รอขายก่อนจึงมองหาเหรียญถัดไป")
            else:
                candidates = []
                for bot in bots.values():
                    if bot.state.get("status") != "IDLE":
                        continue
                    px = prices.get(bot.symbol)
                    if px is None:
                        continue
                    try:
                        signal = bot.analyze_trend(px)
                    except Exception:
                        logger.exception(f"[{bot.symbol}] วิเคราะห์เทรนด์ล้มเหลว")
                        continue
                    if signal.get("should_buy"):
                        candidates.append((bot, px, signal))

                candidates.sort(
                    key=lambda item: (
                        item[2]["confidence"],
                        item[2].get("net_2h") or 0,
                        item[2]["change_1h"],
                    ),
                    reverse=True,
                )

                for bot, px, signal in candidates:
                    holding_count = sum(1 for b in bots.values() if b.state.get("status") == "HOLDING")
                    remaining_slots = int(control["max_open_positions"]) - holding_count
                    if remaining_slots <= 0:
                        logger.info("ช่องถือเต็มแล้ว — เหรียญที่เหลือจะรอจนมีช่องว่างหลังขาย")
                        break
                    net_txt = f"{signal.get('net_2h'):+.2f}%" if signal.get("net_2h") is not None else "n/a"
                    logger.info(
                        f"[{bot.symbol}] สัญญาณขาขึ้น คะแนน {signal['confidence']}% "
                        f"(ชม.1 {signal['change_1h']:+.2f}%, สุทธิ 2ชม. {net_txt}) — พยายามเข้าซื้อ "
                        f"(แบ่งเงินจาก {remaining_slots} ช่องว่าง)"
                    )
                    try:
                        bot.try_enter_position(px, available_slots=remaining_slots)
                    except Exception:
                        logger.exception(f"[{bot.symbol}] เข้าซื้อล้มเหลว")
                    time.sleep(0.3)

        for _ in range(POLL_INTERVAL_SEC):
            if STOP_ALL["value"] or any(b._stop_requested for b in bots.values()):
                stop_all = True
                break
            time.sleep(1)

    for bot in bots.values():
        bot.save_state()
        bot.save_market(force_history=True)
    logger.info("บอทหยุดทำงานเรียบร้อย (state ถูกบันทึกแล้ว)")
