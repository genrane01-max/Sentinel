"""
InnovestX Automated Trading Bot — ซื้อตอนย่อ (v8)
(จำยอดที่ราคาขึ้น รอย่อ แล้วค่อยเข้าซื้อ — ไม่ไล่ตาม, ไม่ใช้หน้าต่าง 1/2/3 ชม.)

ของใหม่ในเวอร์ชันนี้:
1. สูตรเข้าซื้อ: เห็นราคาขึ้นพอ → จำจุดสูงสุดไว้ → รอย่อจากยอดนั้น → เด้งเล็กน้อยค่อยซื้อ
2. ไม่ใช้เกณฑ์ชม.1/ชม.2/ชม.3 และไม่รอสะสมประวัติ 2 ชั่วโมงก่อนซื้อ
3. ยอดที่จำไว้เก็บบน Firebase ต่อเหรียญ ปิดบอทแล้วยังจำได้ จนกว่าจะซื้อหรือโครงสร้างพัง
4. แยก Firebase: ฐานหลัก = สถานะเทรด/ตั้งค่า, ฐานที่ 2 = bid/offer + ประวัติราคา
5. ตัดสินใจซื้อเทียบ offer (ask), ขายเทียบ bid, กันสเปรดกว้าง
6. แดชบอร์ดใช้สูตรเดียวกับบอท
7. มีเหรียญแต่ยืนยันราคาจับคู่ไม่ได้ — ถือต่อทันทีด้วยต้นทุนประมาณ แล้วขาย/trailing ได้เลย ไม่รอปลดจากเว็บ
8. แจ้งเตือน Telegram + หยุดเฝ้าต่อเหรียญหลังตัดขาดทุน (กดเฝ้าต่อเองจากหน้าเว็บ)
9. สัญญาณกลับตัวล็อกกำไรเหนือจุดคุ้มทุน โดยไม่ต้องรอ trailing arm — แล้วคูลดาวน์ก่อนซื้อเหรียญนี้คืน

ของเดิมยังอยู่ครบ: แดชบอร์ด, หยุด/เริ่มเทรด, รหัสผ่าน, % ขาดทุนต่อวัน, อัตราเงินต่อไม้,
จำนวนไม้ขาดทุนติดกัน, price_history ต่อเหรียญ, ค่าธรรมเนียม, circuit breaker, Decimal, graceful shutdown
"""

import time
import uuid
import hmac
import hashlib
import base64
import json
import os
import re
import signal
import logging
import threading
import urllib.parse
from string import Template
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

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
_FIREBASE_READY = {"value": False}
FIREBASE_INIT_TIMEOUT_SEC = 15


def firebase_call(fn, timeout=12, what="Firebase"):
    """กันบอท/หน้าเว็บค้างเงียบถ้า Firebase ไม่ตอบ"""
    box = {"done": False, "value": None, "error": None}

    def _run():
        try:
            box["value"] = fn()
        except Exception as e:
            box["error"] = e
        finally:
            box["done"] = True

    worker = threading.Thread(target=_run, daemon=True, name=f"fb-{what}")
    worker.start()
    worker.join(timeout)
    if not box["done"]:
        raise TimeoutError(f"{what} ไม่ตอบใน {timeout} วินาที")
    if box["error"] is not None:
        raise box["error"]
    return box["value"]


def _init_firebase_apps():
    global _market_db_enabled
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
    return True


def init_firebase(timeout=FIREBASE_INIT_TIMEOUT_SEC):
    """เชื่อม Firebase — มี timeout เพื่อไม่ให้โปรเซสค้างก่อนเปิด /health"""
    global _market_db_enabled
    if firebase_admin._apps:
        try:
            firebase_admin.get_app(MARKET_APP_NAME)
            _market_db_enabled = True
        except (ValueError, KeyError):
            _market_db_enabled = False
        _FIREBASE_READY["value"] = True
        return True
    try:
        firebase_call(_init_firebase_apps, timeout=timeout, what="Firebase init")
        _FIREBASE_READY["value"] = True
        return True
    except Exception as e:
        logger.error(f"Firebase Init Error: {e}")
        _FIREBASE_READY["value"] = False
        return False


def _ensure_firebase():
    if firebase_admin._apps:
        return True
    return init_firebase()


class _TimedFirebaseRef:
    def __init__(self, ref, label):
        self._ref = ref
        self._label = label

    def get(self):
        return firebase_call(self._ref.get, what=f"{self._label}.get")

    def set(self, value):
        return firebase_call(lambda: self._ref.set(value), what=f"{self._label}.set")

    def update(self, value):
        return firebase_call(lambda: self._ref.update(value), what=f"{self._label}.update")


def primary_ref(path):
    """โหนดบนฐานหลัก (สถานะเทรด / ตั้งค่า)"""
    _ensure_firebase()
    return _TimedFirebaseRef(db.reference(path), f"DB1:{path}")


def market_ref(path):
    """โหนดบนฐานที่ 2 ถ้ามี ไม่งั้นใช้ฐานหลักที่ path เดิม"""
    _ensure_firebase()
    if _market_db_enabled:
        try:
            return _TimedFirebaseRef(
                db.reference(path, app=firebase_admin.get_app(MARKET_APP_NAME)),
                f"DB2:{path}",
            )
        except Exception as e:
            logger.warning(f"ใช้ DB2 ไม่ได้ ถอยไปฐานหลัก: {e}")
    return _TimedFirebaseRef(db.reference(path), f"DB1:{path}")


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
        state["price_history"] = trim_to_continuous_recent(market["price_history"])
    if market.get("quote"):
        state["quote"] = market["quote"]
    return state


# อย่าเชื่อม Firebase ตอน import — เปิด /health ก่อน แล้วค่อยเชื่อมใน __main__

# ==================== การควบคุมบอทจากหน้าเว็บ ====================
DEFAULT_SYMBOL = os.environ.get("SYMBOL", "BTCTHB").upper()
RUNNING_SYMBOL = {"value": DEFAULT_SYMBOL}
RUNNING_WATCHLIST = {"value": [DEFAULT_SYMBOL]}
LIVE_BOTS = {}
STOP_ALL = {"value": False}
SHARED_RISK_PATH = "bots/_shared/risk"
SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}$")
KNOWN_QUOTE_SUFFIXES = ("THB", "USDT", "USD")
# ซื้อตอนย่อ — จำยอดที่ขึ้น วัดย่อจากยอดนั้น ไม่ใช้หน้าต่างเวลา
PULLBACK_MIN_IMPULSE_PERCENT = 0.8   # ขึ้นจากจุดต่ำอย่างน้อยเท่านี้ จึงนับว่าพุ่ง แล้วจำยอด
PULLBACK_MIN_DIP_PERCENT = 0.5       # ย่อจากยอดที่จำไว้อย่างน้อยเท่านี้ จึงรอเด้งเข้าซื้อ
PULLBACK_MAX_DIP_PERCENT = 1.5       # ย่อลึกกว่านี้ = โครงสร้างพัง ล้างยอด เริ่มใหม่
PULLBACK_BOUNCE_PERCENT = 0.09       # เด้งจากจุดต่ำของย่ออย่างน้อยเท่านี้ถึงซื้อ (กันมีดตก)
MAX_SPREAD_PERCENT = 0.53            # ไม่ซื้อถ้า (ask-bid)/mid กว้างเกินนี้ (%)
SWING_RESET = {
    "swing_low": 0.0,      # จุดเริ่มพุ่ง (trough)
    "swing_high": 0.0,     # ยอดที่จำไว้
    "pullback_low": 0.0,   # จุดต่ำสุดหลังยอด (ระหว่างรอเด้ง)
    "entry_armed": False,  # เห็นพุ่งแล้ว รอว่าราคาย่อ
}
REVERSAL_WINDOW_MINUTES = 10         # ดูย้อนหลังกี่นาทีเพื่อจับราคาพลิกลง
REVERSAL_BUCKET_MINUTES = 3.3        # แบ่งหน้าต่างเป็นช่วงละกี่นาที (15/5 = 3 ช่วง)
REVERSAL_MIN_BUCKET_DOWN_PERCENT = -0.12  # นับเป็น "ลง" ถ้าช่วงนั้นลงอย่างน้อยเท่านี้
REVERSAL_MIN_DOWN_RATIO = 0.66       # ต้องมีช่วงที่ลงอย่างน้อยเท่านี้ของทั้งหมด (2 ใน 3)
REVERSAL_MIN_TOTAL_PERCENT = -0.20   # รวมทั้งหน้าต่างต้องลงอย่างน้อยเท่านี้
REVERSAL_COOLDOWN_MINUTES = 10       # ขายด้วยสัญญาณกลับตัวแล้ว ห้ามซื้อเหรียญนี้คืนกี่นาที
ORDER_SEND_PATH = "/api/v1/digital-asset/order/send"
RECENT_ORDER_LOOKBACK_SEC = 180
MARKET_HISTORY_FLUSH_SEC = 15
PRICE_TICK_MIN_INTERVAL_SEC = 10
PRICE_TICK_MIN_MOVE_PERCENT = 0.05
HISTORY_NEEDED_SEC = 7200          # ไม่ใช้กันซื้อแล้ว เก็บไว้แค่คำนวณแถบประวัติถ้ามี
HISTORY_KEEP_SEC = 10800           # เก็บย้อนหลัง 3 ชม. (ใช้ตอนขายด้วยสัญญาณกลับตัว)
HISTORY_GAP_BREAK_SEC = 600        # ขาดเกิน 10 นาที = เริ่มสะสมใหม่ (กันประวัติเก่าตอนย้ายเครื่อง)
PRICE_OFFSET_TOLERANCE_SEC = 300
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


def parse_market_quote(data):
    """
    แปลงข้อมูลตลาดจาก InnovestX ให้เป็น:
    bid, ask, last, mid, spread, spread_pct
    รองรับ side=0/1 และ insideBidPrice/insideAskPrice
    """
    result = {
        "bid": None,
        "ask": None,
        "last": None,
        "mid": None,
        "spread": None,
        "spread_pct": None,
    }
    if not isinstance(data, dict):
        return result

    rows = data.get("data")
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        rows = []

    bids = []
    asks = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            price = float(row.get("price"))
        except (TypeError, ValueError):
            price = None
        side = row.get("side")
        if price is not None:
            if side in (0, "0", "Buy", "BUY", "buy"):
                bids.append(price)
            elif side in (1, "1", "Sell", "SELL", "sell"):
                asks.append(price)

        # สำรองจาก Ticker (ถ้ามี)
        if result["bid"] is None:
            try:
                inside_bid = float(row.get("insideBidPrice"))
                if inside_bid > 0:
                    result["bid"] = inside_bid
            except (TypeError, ValueError):
                pass
        if result["ask"] is None:
            try:
                inside_ask = float(row.get("insideAskPrice"))
                if inside_ask > 0:
                    result["ask"] = inside_ask
            except (TypeError, ValueError):
                pass

    if bids:
        result["bid"] = max(bids)
    if asks:
        result["ask"] = min(asks)

    # lastTradePrice
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            last = float(row.get("lastTradePrice"))
            if last > 0:
                result["last"] = last
                break
        except (TypeError, ValueError):
            pass

    # คำนวณ mid, spread, spread_pct
    if result["bid"] is not None and result["ask"] is not None:
        result["mid"] = (result["bid"] + result["ask"]) / 2
        result["spread"] = result["ask"] - result["bid"]
        if result["mid"] > 0:
            result["spread_pct"] = (result["spread"] / result["mid"]) * 100

    return result


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


def apply_pullback_tick(price, swing=None):
    """จำยอดที่ราคาขึ้น แล้ววัดว่าย่อจากยอดนั้นเท่าไร — ไม่ใช้หน้าต่าง 1/2/3 ชม.

    ลำดับ:
      1) ราคาขึ้นจากจุดต่ำถึงเกณฑ์ → ติดอาวุธ จำยอด ยังไม่ซื้อ (ไม่ไล่ตาม)
      2) ถ้าราคาทำยอดใหม่ → เลื่อนยอดที่จำไว้
      3) ย่อจากยอดในช่วงที่กำหนด → รอเด้ง
      4) เด้งจากจุดต่ำของย่อ → ซื้อ
      5) ย่อลึกเกิน / หลุดจุดเริ่มพุ่ง → ล้างยอด เริ่มใหม่
    """
    swing = swing or {}
    result = {
        "should_buy": False,
        "reason": "watching",
        "phase": "watch",
        "impulse_pct": None,
        "dip_pct": None,
        "bounce_pct": None,
        "swing_low": 0.0,
        "swing_high": 0.0,
        "pullback_low": 0.0,
        "entry_armed": False,
        "confidence": 0,
        "direction": None,
        "vetoed": False,
    }
    price = _safe_float(price, 0.0) or 0.0
    if price <= 0:
        result["reason"] = "no_price"
        return result

    armed = bool(swing.get("entry_armed"))
    trough = float(swing.get("swing_low") or 0.0)
    peak = float(swing.get("swing_high") or 0.0)
    pb_low = float(swing.get("pullback_low") or 0.0)

    if not armed:
        if trough <= 0 or price < trough:
            trough = price
        impulse = ((price - trough) / trough * 100.0) if trough > 0 else 0.0
        result["impulse_pct"] = impulse
        if impulse >= PULLBACK_MIN_IMPULSE_PERCENT:
            armed = True
            peak = price
            pb_low = price
            result["phase"] = "armed"
            result["reason"] = "armed_wait_dip"
            result["direction"] = "up"
        else:
            result["phase"] = "watch"
            result["reason"] = "waiting_impulse"
            if impulse > 0:
                result["direction"] = "up"
            elif trough > 0 and price < float(swing.get("swing_low") or trough):
                result["direction"] = "down"
    else:
        impulse = ((peak - trough) / trough * 100.0) if trough > 0 and peak > 0 else 0.0
        result["impulse_pct"] = impulse
        result["direction"] = "up"
        if price > peak:
            peak = price
            pb_low = price
            result["phase"] = "armed"
            result["reason"] = "armed_new_high"
            result["dip_pct"] = 0.0
            result["bounce_pct"] = 0.0
        else:
            if pb_low <= 0 or price < pb_low:
                pb_low = price
            dip = ((peak - price) / peak * 100.0) if peak > 0 else 0.0
            max_dip = ((peak - pb_low) / peak * 100.0) if peak > 0 else 0.0
            bounce = ((price - pb_low) / pb_low * 100.0) if pb_low > 0 else 0.0
            result["dip_pct"] = dip
            result["bounce_pct"] = bounce
            broke = max_dip > PULLBACK_MAX_DIP_PERCENT or (trough > 0 and price < trough)
            if broke:
                armed = False
                trough = price
                peak = 0.0
                pb_low = 0.0
                result["phase"] = "watch"
                result["reason"] = "structure_broke"
                result["direction"] = "down"
                result["impulse_pct"] = 0.0
            elif max_dip < PULLBACK_MIN_DIP_PERCENT:
                result["phase"] = "armed"
                result["reason"] = "armed_wait_dip"
            elif bounce >= PULLBACK_BOUNCE_PERCENT:
                result["phase"] = "bounce"
                result["reason"] = None
                result["should_buy"] = True
                result["confidence"] = min(100, int(60 + impulse * 8 + bounce * 40))
            else:
                result["phase"] = "pullback"
                result["reason"] = "waiting_bounce"

    result["swing_low"] = trough
    result["swing_high"] = peak
    result["pullback_low"] = pb_low
    result["entry_armed"] = armed
    if result["should_buy"] and not result["confidence"]:
        result["confidence"] = 80
    return result


def swing_fields_from_signal(signal):
    return {
        "swing_low": float(signal.get("swing_low") or 0.0),
        "swing_high": float(signal.get("swing_high") or 0.0),
        "pullback_low": float(signal.get("pullback_low") or 0.0),
        "entry_armed": bool(signal.get("entry_armed")),
    }


def detect_momentum_reversal(history, now=None,
                              window_minutes=REVERSAL_WINDOW_MINUTES,
                              bucket_minutes=REVERSAL_BUCKET_MINUTES):
    """หาสัญญาณกลับตัวระยะสั้น ใช้ล็อกกำไรก่อน trailing % แบบเดิมจะทันตัด

    แบ่งช่วงย้อนหลังสั้นๆ ถ้าส่วนใหญ่ลง และรวมลงพอ
    ถ้าส่วนใหญ่ลง และรวมลงพอ — ขายเหนือจุดคุ้มทุนได้เลย ไม่ต้องรอ trailing arm
    """
    result = {"is_reversal": False, "reason": "no_data", "total_change": None, "buckets": []}
    now = time.time() if now is None else now
    history = trim_to_continuous_recent(history or [], now)
    if not history:
        return result
    window_sec = window_minutes * 60
    bucket_sec = bucket_minutes * 60
    n_buckets = int(window_minutes // bucket_minutes)
    if n_buckets < 2 or now - history[0][0] < window_sec:
        result["reason"] = "waiting_history"
        return result

    def price_at(seconds_ago, tolerance=90):
        target = now - seconds_ago
        closest = min(history, key=lambda t: abs(t[0] - target))
        return closest[1] if abs(closest[0] - target) <= tolerance else None

    marks = []
    for i in range(n_buckets + 1):
        px = price_at(window_sec - i * bucket_sec)
        if px is None:
            result["reason"] = "gap"
            return result
        marks.append(px)

    buckets, declining = [], 0
    for i in range(n_buckets):
        pct = _pct_change(marks[i + 1], marks[i])
        if pct is None:
            result["reason"] = "gap"
            return result
        buckets.append(pct)
        if pct <= REVERSAL_MIN_BUCKET_DOWN_PERCENT:
            declining += 1
    result["buckets"] = buckets

    total_change = _pct_change(marks[-1], marks[0])
    result["total_change"] = total_change

    if declining / n_buckets < REVERSAL_MIN_DOWN_RATIO:
        result["reason"] = "not_enough_down_buckets"
        return result
    if total_change is None or total_change > REVERSAL_MIN_TOTAL_PERCENT:
        result["reason"] = "not_enough_total_drop"
        return result

    result["is_reversal"] = True
    result["reason"] = None
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


def parse_order_timestamp(order):
    """
    แปลงเวลาสร้างออเดอร์เป็น epoch วินาที
    รองรับ receiveDateTime, transactTime, ฯลฯ
    """
    if not isinstance(order, dict):
        return None

    # ลำดับความสำคัญ: receiveDateTime มาก่อน
    time_fields = [
        "receiveDateTime",
        "transactTime",
        "createdTime",
        "createTime",
        "createdAt",
        "timestamp",
        "time",
        "orderTime",
        "updatedTime",
        "updateTime",
    ]

    for key in time_fields:
        raw = order.get(key)
        if raw is None or raw == "":
            continue
        try:
            val = float(raw)
            if val > 1e12:
                return val / 1000.0
            if val > 1e9:
                return val
        except (TypeError, ValueError):
            pass

        try:
            text = str(raw).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except (TypeError, ValueError):
            continue

    return None


def normalize_order_side(value):
    """
    แปลง side ของ InnovestX ให้เป็น:
    0 = Buy
    1 = Sell
    รองรับทั้งตัวเลขและข้อความ
    """
    if value in (0, "0", "Buy", "BUY", "buy"):
        return 0
    if value in (1, "1", "Sell", "SELL", "sell"):
        return 1
    return None


def order_is_recent_match(order, symbol, side, since_ts, allow_open_without_time=False):
    """
    ตรวจสอบว่าออเดอร์นี้ตรงกับเหรียญ/ฝั่ง และเกิดขึ้นหลัง since_ts หรือไม่
    ใช้ normalize_order_side เพื่อแปลง side ให้เป็น 0/1
    """
    if not isinstance(order, dict):
        return False

    order_symbol = str(order.get("symbol") or "").upper()
    if order_symbol and order_symbol != str(symbol or "").upper():
        return False

    order_side = normalize_order_side(order.get("side"))
    if order_side is None:
        return False
    if order_side != int(side):
        return False

    ts = parse_order_timestamp(order)
    if ts is None:
        if not allow_open_without_time:
            return False
        state = str(order.get("orderState") or order.get("status") or "").lower()
        return state in ("working", "new", "open", "pending", "partial", "partiallyfilled", "")

    return ts >= (float(since_ts) - 5.0)


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


def trim_to_continuous_recent(history, now=None, max_gap_sec=HISTORY_GAP_BREAK_SEC):
    """เก็บเฉพาะช่วงราคาต่อเนื่องล่าสุด ทิ้งข้อมูลเก่าก่อนรูที่เกิดตอนบอทดับ/ย้ายเครื่อง"""
    if not history:
        return []
    now = time.time() if now is None else now
    cleaned = []
    for item in history:
        try:
            ts = float(item[0])
            px = float(item[1])
        except (TypeError, ValueError, IndexError):
            continue
        cleaned.append([ts, px])
    if not cleaned:
        return []
    cleaned.sort(key=lambda t: t[0])
    if now - cleaned[-1][0] > max_gap_sec:
        return []
    cut = 0
    for i in range(len(cleaned) - 1, 0, -1):
        if cleaned[i][0] - cleaned[i - 1][0] > max_gap_sec:
            cut = i
            break
    return cleaned[cut:]


def history_elapsed_sec(history, now=None, max_gap_sec=HISTORY_GAP_BREAK_SEC):
    hist = trim_to_continuous_recent(history, now=now, max_gap_sec=max_gap_sec)
    if not hist:
        return 0.0
    now = time.time() if now is None else now
    return max(0.0, now - hist[0][0])


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
    bot = next(iter(list(LIVE_BOTS.values())), None)
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
    "watching": "กำลังดูราคา เพื่อจำยอดตอนขึ้น",
    "waiting_impulse": f"รอราคาขึ้นจากจุดต่ำอย่างน้อย {PULLBACK_MIN_IMPULSE_PERCENT}% แล้วจะจำยอด",
    "armed_wait_dip": f"จำยอดแล้ว — ไม่ไล่ซื้อ รอว่าราคาย่อ {PULLBACK_MIN_DIP_PERCENT}–{PULLBACK_MAX_DIP_PERCENT}%",
    "armed_new_high": "ราคาทำยอดใหม่ — เลื่อนยอดที่จำไว้ ยังไม่ซื้อ",
    "waiting_bounce": "ย่อมาแล้ว รอเด้งเล็กน้อยแล้วค่อยซื้อ (กันมีดตก)",
    "structure_broke": f"ย่อลึกเกิน {PULLBACK_MAX_DIP_PERCENT}% หรือหลุดจุดเริ่มพุ่ง — ล้างยอด เริ่มใหม่",
    "wide_spread": f"สเปรดกว้างเกิน {MAX_SPREAD_PERCENT}% — ไม่ซื้อ",
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


_CONTROL_CACHE = {"value": None, "fresh": False}


def _build_control(data):
    data = data or {}
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

    paused_symbols = []
    for item in data.get("paused_symbols") or []:
        sym = _normalize_symbol(item)
        if looks_like_pair(sym) and sym in watchlist and sym not in paused_symbols:
            paused_symbols.append(sym)
    pause_reasons = {}
    raw_reasons = data.get("pause_reasons") or {}
    if isinstance(raw_reasons, dict):
        for key, value in raw_reasons.items():
            sym = _normalize_symbol(key)
            if sym in paused_symbols:
                pause_reasons[sym] = str(value or "user")

    return {
        "active_symbol": watchlist[0] if watchlist else fallback_symbol,
        "watchlist": watchlist,
        "paused": bool(data.get("paused", False)),
        "paused_symbols": paused_symbols,
        "pause_reasons": pause_reasons,
        "max_daily_loss_percent": max_daily_loss_percent,
        "trade_size_percent": trade_size_percent,
        "max_consecutive_losses": max_consecutive_losses,
        "max_open_positions": max_open_positions,
        "trailing_stop_percent": trailing_stop_percent,
        "stop_loss_percent": stop_loss_percent,
        "unlock_requested": bool(data.get("unlock_requested", False)),
    }


def _uninitialized_control():
    """ค่าปลอดภัยตอนยังอ่าน Firebase ไม่ได้ — รายการว่าง ไม่ซื้อ และห้ามเขียนทับของจริง"""
    result = _build_control({})
    result["watchlist"] = []
    result["paused_symbols"] = []
    result["pause_reasons"] = {}
    result["_uninitialized"] = True
    return result


def load_control():
    """อ่านคำสั่งควบคุมล่าสุดจากหน้าเว็บ (รายการเหรียญที่เฝ้า, หยุดชั่วคราว, ความเสี่ยง, จำนวนช่องถือพร้อมกัน)"""
    try:
        data = primary_ref("bot_control").get() or {}
    except Exception as e:
        cached = _CONTROL_CACHE.get("value")
        if cached is not None:
            logger.warning(f"อ่าน bot_control จาก Firebase ไม่ได้ ใช้ค่าล่าสุดที่อ่านได้: {e}")
            return dict(cached)
        logger.warning(
            f"อ่าน bot_control จาก Firebase ไม่ได้ — ยังไม่ใช้ค่าเริ่มต้นและจะไม่เขียนทับ watchlist: {e}"
        )
        return _uninitialized_control()

    result = _build_control(data)
    _CONTROL_CACHE["value"] = result
    _CONTROL_CACHE["fresh"] = True
    return result


def save_control(control):
    if not isinstance(control, dict) or control.get("_uninitialized"):
        logger.warning("ข้ามการบันทึก bot_control — ยังอ่านของจริงจาก Firebase ไม่ได้ กันเขียนทับ watchlist")
        return False
    try:
        payload = {k: v for k, v in control.items() if k != "_uninitialized"}
        if payload.get("watchlist"):
            payload["active_symbol"] = payload["watchlist"][0]
            control["active_symbol"] = payload["active_symbol"]
        elif not payload.get("active_symbol"):
            payload["active_symbol"] = DEFAULT_SYMBOL
            control["active_symbol"] = DEFAULT_SYMBOL
        primary_ref("bot_control").set(payload)
        _CONTROL_CACHE["value"] = dict(payload)
        _CONTROL_CACHE["fresh"] = True
        return True
    except Exception as e:
        logger.error(f"บันทึก bot_control ไป Firebase ไม่สำเร็จ: {e}")
        return False


def wait_for_fresh_control(attempts=8, base_sleep=2):
    """รออ่าน bot_control จริงจาก Firebase — ไม่เขียนค่าเริ่มต้นทับถ้ายังอ่านไม่ได้"""
    control = None
    for attempt in range(1, attempts + 1):
        init_firebase()
        control = load_control()
        if _CONTROL_CACHE.get("fresh") and not control.get("_uninitialized"):
            return control
        logger.warning(
            f"อ่าน bot_control จาก Firebase ไม่ได้ครั้งที่ {attempt}/{attempts} "
            f"— ยังไม่เขียนทับรายการเหรียญ"
        )
        if attempt < attempts:
            time.sleep(min(base_sleep * attempt, 10))
    return control if control is not None else _uninitialized_control()


def notify_telegram(text):
    """ส่งข้อความสั้นๆ เข้ามือถือ — ไม่มี token/chat แล้วข้ามเงียบๆ ไม่ให้การเทรดพัง"""
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (os.environ.get("TELEGRAM_CHAT_ID") or "").strip()
    if not token or not chat_id or not str(text or "").strip():
        return False
    try:
        res = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": str(text).strip(),
                "disable_web_page_preview": True,
            },
            timeout=8,
        )
        status = getattr(res, "status_code", 200)
        if status >= 400:
            logger.warning(f"ส่ง Telegram ไม่สำเร็จ HTTP {status}")
            return False
        return True
    except Exception as e:
        logger.warning(f"ส่ง Telegram ไม่สำเร็จ: {e}")
        return False


def is_watch_paused(symbol, control=None):
    if control is None:
        control = load_control()
    paused = {_normalize_symbol(s) for s in (control.get("paused_symbols") or [])}
    return _normalize_symbol(symbol) in paused


def apply_watch_pause(control, symbol, paused, reason="user"):
    """หยุด/เฝ้าต่อรายเหรียญ — ยังอยู่ในรายการ แต่ไม่เปิดไม้ซื้อ"""
    control = dict(control or {})
    symbol = _normalize_symbol(symbol)
    paused_list = [s for s in (control.get("paused_symbols") or []) if _normalize_symbol(s) != symbol]
    reasons = dict(control.get("pause_reasons") or {})
    if paused and looks_like_pair(symbol):
        paused_list.append(symbol)
        reasons[symbol] = str(reason or "user")
    else:
        reasons.pop(symbol, None)
    control["paused_symbols"] = paused_list
    control["pause_reasons"] = reasons
    return control


def set_watch_paused(symbol, paused, reason="user"):
    control = apply_watch_pause(load_control(), symbol, paused, reason=reason)
    save_control(control)
    return control


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
    STALE_PRICE_ALERT_SEC = 300           # ราคานิ่งนานเท่านี้วิ ระหว่างถือของ ให้แจ้งเตือน
    STALE_PRICE_ALERT_COOLDOWN_SEC = 900  # กันสแปม ห่างกันอย่างน้อยเท่านี้ค่อยแจ้งซ้ำ

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
        self._last_stale_alert_ts = 0.0

        self.state = self.load_state()
        LIVE_BOTS[self.symbol] = self

    # ==================== State management ====================
    def load_state(self):
        default_state = {
            "status": "IDLE",  # IDLE / HOLDING / HALTED
            "entry_price": 0.0,
            "highest_price": 0.0,
            "quantity": 0.0,
            "dust_quantity": 0.0,  # เศษที่ขายไม่หมดเพราะขั้นต่ำของตลาด — รวมเข้าไม้ถัดไป
            "roundtrip_fee_percent": self.DEFAULT_ROUNDTRIP_FEE_PERCENT,
            "price_history": [],  # [[timestamp_sec, price], ...] เก็บย้อนหลัง 3 ชม. อยู่บน DB2
            "quote": {},          # bid/ask/last/spread ล่าสุด อยู่บน DB2
            "trade_date": None,   # "YYYY-MM-DD" (UTC) สำหรับรีเซ็ต circuit breaker รายวัน
            "daily_start_balance": 0.0,
            "daily_realized_pnl": 0.0,
            "consecutive_losses": 0,
            "pending_order": None,  # คำสั่งที่ timeout แล้วยังไม่รู้ว่าเข้าหรือไม่ — ห้ามยิงซ้ำ
            "halt_cleared_by_user": False,
            "entry_price_estimated": False,  # True = ถือด้วยต้นทุนประมาณ เพราะยืนยัน fill ไม่ได้
            "reversal_cooldown_until": 0.0,  # epoch วินาที — ห้ามเปิดไม้ใหม่ก่อนถึงเวลานี้
            "swing_low": 0.0,        # จุดเริ่มพุ่ง ที่ใช้วัดว่าขึ้นพอจะจำยอด
            "swing_high": 0.0,       # ยอดที่จำไว้ เพื่อเปรียบเทียบย่อ
            "pullback_low": 0.0,     # จุดต่ำสุดหลังยอด (รอเด้ง)
            "entry_armed": False,    # True = จำยอดแล้ว รอว่าราคาย่อ
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
        raw_len = len(hist) if hist else 0
        hist = trim_to_continuous_recent(hist)
        if raw_len and len(hist) < raw_len:
            logger.info(
                f"[{self.symbol}] ทิ้งประวัติราคาที่ขาดช่วง {raw_len - len(hist)} จุด "
                f"— เริ่มสะสมใหม่ (แถบรอ 2 ชม. จะกลับมา)"
            )
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
            logger.warning(
                f"[{self.symbol}] state บอก IDLE แต่มีเหรียญค้างอยู่ {coin_free} "
                f"— ไม่ซื้อทับ จะถือต่อทันทีด้วยราคาประมาณ เพื่อให้ขายได้ (ไม่รอปลดจากเว็บ)"
            )
            if not self._adopt_unknown_buy_cost(coin_free):
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
        hist = trim_to_continuous_recent(self.state.get("price_history") or [], now)
        if should_append_price_tick(hist, now, price):
            hist.append([now, price])
            cutoff = now - HISTORY_KEEP_SEC
            self.state["price_history"] = [t for t in hist if t[0] >= cutoff]
            self.save_market(force_history=False)
        else:
            # ราคาไม่ขยับพอ — อัปเดตแค่ quote ไม่เขียนประวัติทั้งก้อน
            self.state["price_history"] = hist
            self.save_market(force_history=False)

    def _price_at_offset(self, seconds_ago, tolerance_sec=PRICE_OFFSET_TOLERANCE_SEC):
        history = self.state["price_history"]
        if not history:
            return None
        target = time.time() - seconds_ago
        closest = min(history, key=lambda t: abs(t[0] - target))
        if abs(closest[0] - target) > tolerance_sec:
            return None  # ข้อมูล ณ ช่วงเวลานั้นขาดหายไปเกินไป ถือว่าไม่พอ
        return closest[1]

    def _swing_from_state(self):
        return {
            "swing_low": float(self.state.get("swing_low") or 0.0),
            "swing_high": float(self.state.get("swing_high") or 0.0),
            "pullback_low": float(self.state.get("pullback_low") or 0.0),
            "entry_armed": bool(self.state.get("entry_armed")),
        }

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
    def _dust_threshold(self):
        rules = self.get_symbol_rules()
        try:
            return float(rules["quantity_increment"])
        except (TypeError, ValueError, KeyError):
            return 0.0

    def _absorb_dust_into_qty(self, qty):
        """รวมเศษที่ขายไม่หมดรอบก่อนเข้าจำนวนไม้ใหม่ แล้วล้าง dust_quantity"""
        try:
            dust = Decimal(str(self.state.get("dust_quantity") or 0))
            qty = Decimal(str(qty or 0)) + dust
            qty_f = float(qty)
            dust_f = float(dust)
        except (InvalidOperation, ValueError, TypeError):
            dust_f = float(self.state.get("dust_quantity") or 0.0)
            qty_f = float(qty or 0.0) + dust_f
        if dust_f > 0:
            logger.info(f"[{self.symbol}] รวมเศษค้าง {dust_f} เข้าจำนวนที่ถือ — รวม {qty_f}")
        return qty_f

    def _stash_unsellable_dust(self, coin_free, sell_qty):
        """เก็บเศษที่ปัดทิ้งเพราะขั้นต่ำของตลาด ไว้รวมกับไม้ถัดไป — ใช้ยอดในกระเป๋าจริง ไม่ใช่จำนวนใน state ที่ถูกปัดแล้ว"""
        try:
            leftover = max(Decimal("0"), Decimal(str(coin_free or 0)) - Decimal(str(sell_qty or 0)))
            leftover = float(leftover)
        except (InvalidOperation, ValueError, TypeError):
            leftover = max(0.0, float(coin_free or 0.0) - float(sell_qty or 0.0))
        prev = float(self.state.get("dust_quantity") or 0.0)
        self.state["dust_quantity"] = prev + leftover
        if leftover > 0:
            logger.info(
                f"[{self.symbol}] ขายได้ {sell_qty} เหลือเศษ {leftover} "
                f"(ต่ำกว่าขั้นต่ำของตลาด ขายต่อไม่ได้) เก็บไว้รวมกับไม้ถัดไป"
            )
        return leftover

    def _has_unresolved_order(self):
        """ล็อก pending ยังอยู่หรือไม่ — ไม่ปลดแค่เพราะหมดเวลา ต้องเคลียร์จากพอร์ต/ออเดอร์จริง"""
        pending = self.state.get("pending_order") or {}
        return bool(pending)

    def _pending_since_ts(self):
        pending = self.state.get("pending_order") or {}
        try:
            ts = float(pending.get("ts") or 0)
        except (TypeError, ValueError):
            ts = 0.0
        if ts <= 0:
            return time.time() - RECENT_ORDER_LOOKBACK_SEC
        return ts

    def _recover_order_after_timeout(self, side, max_attempts=5, delay_sec=1.5):
        """
        หลัง timeout: หาออเดอร์ล่าสุดในระบบ (ใช้ orderId ที่เก็บไว้ ถ้ามี)
        ห้ามยิงซ้ำ
        """
        pending = self.state.get("pending_order") or {}
        pending_order_id = pending.get("order_id")

        # ถ้ามี pending_order_id ให้ค้นหาตามนั้นก่อน
        if pending_order_id:
            for attempt in range(1, max_attempts + 1):
                hist_res = self.send_request(
                    "POST", "/api/v1/digital-asset/order/history/inquiry",
                    body={"symbol": self.symbol, "orderId": pending_order_id}
                )
                if hist_res and hist_res.get("code") == "0000":
                    orders = hist_res.get("data") or []
                    if orders:
                        order = orders[0]
                        logger.info(f"[{self.symbol}] พบออเดอร์ตาม orderId {pending_order_id}")
                        return {"code": "0000", "data": order}
                time.sleep(delay_sec)

        # ถ้าไม่มี orderId หรือหาไม่เจอ ให้ค้นหาจาก Open Orders / History ทั่วไป
        since_ts = self._pending_since_ts()
        for attempt in range(1, max_attempts + 1):
            open_res = self.send_request("GET", "/api/v1/digital-asset/order/open/inquiry")
            if open_res and open_res.get("code") == "0000":
                for order in open_res.get("data") or []:
                    if order_is_recent_match(order, self.symbol, side, since_ts, allow_open_without_time=True):
                        logger.info(f"[{self.symbol}] พบออเดอร์ค้างในระบบหลัง timeout: {order.get('orderId')}")
                        return {"code": "0000", "data": order}

            hist_res = self.send_request(
                "POST", "/api/v1/digital-asset/order/history/inquiry",
                body={"symbol": self.symbol}
            )
            if hist_res and hist_res.get("code") == "0000":
                for order in hist_res.get("data") or []:
                    if order_is_recent_match(order, self.symbol, side, since_ts, allow_open_without_time=False):
                        logger.info(f"[{self.symbol}] พบออเดอร์ล่าสุดในประวัติหลัง timeout: {order.get('orderId')}")
                        return {"code": "0000", "data": order}

            logger.info(f"[{self.symbol}] ยังไม่เจอออเดอร์ล่าสุดหลัง timeout (ครั้งที่ {attempt}/{max_attempts})")
            time.sleep(delay_sec)

        logger.error(
            f"[{self.symbol}] timeout แล้วยังไม่เจอออเดอร์ล่าสุดในระบบ "
            f"— จะไม่ยิงซ้ำ (จะเช็คยอดเหรียญในพอร์ตก่อนตัดสิน)"
        )
        return None

    def execute_market_order(self, side, value=None, quantity=None):
        if side == 0 and value is not None and value < self.MIN_ORDER_THB:
            logger.warning(f"ยกเลิกคำสั่งซื้อ: มูลค่า {value} THB ต่ำกว่าขั้นต่ำ {self.MIN_ORDER_THB} THB")
            return None

        if self._has_unresolved_order():
            pending_side = (self.state.get("pending_order") or {}).get("side", side)
            logger.warning(
                f"[{self.symbol}] มีคำสั่งที่ timeout ค้างอยู่ "
                f"— ไม่ยิงซ้ำ กำลังตรวจสถานะในระบบ"
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

        # ถ้า timeout หรือ indeterminate → เก็บ pending แล้วหา recovery
        if res and res.get("code") == "TIMEOUT_INDETERMINATE":
            self.state["pending_order"] = {
                "side": side,
                "value": value,
                "quantity": quantity,
                "ts": time.time(),
                # ไม่มี order_id เพราะ timeout ยังไม่รู้
            }
            self.save_state()
            recovered = self._recover_order_after_timeout(side)
            return recovered

        # ถ้าสำเร็จ ให้เก็บ orderId ทันที
        if res and res.get("code") == "0000":
            order_id = (res.get("data") or {}).get("orderId")
            if order_id:
                self.state["pending_order"] = {
                    "side": side,
                    "value": value,
                    "quantity": quantity,
                    "ts": time.time(),
                    "order_id": order_id,   # เก็บ orderId ไว้ใช้ตอนกู้คืน
                }
                self.save_state()
            else:
                self.state["pending_order"] = None
            return res

        # กรณีอื่น ๆ ที่ไม่สำเร็จ
        self.state["pending_order"] = None
        return res

    def _wait_for_coin_balance(self, dust_threshold, max_attempts=6, delay_sec=1.0):
        last = (0.0, 0.0, False)
        for _ in range(max_attempts):
            last = self.get_free_balance()
            _, coin_free, has_pending = last
            if float(coin_free or 0) > dust_threshold:
                return last
            time.sleep(delay_sec)
            if has_pending:
                continue
        return last

    def _adopt_filled_buy(self, avg_price, qty, fee_pct):
        qty = self._absorb_dust_into_qty(qty)
        self.state.update({
            "status": "HOLDING",
            "entry_price": avg_price,
            "highest_price": avg_price,
            "quantity": qty,
            "dust_quantity": 0.0,
            "roundtrip_fee_percent": fee_pct,
            "pending_order": None,
            "entry_price_estimated": False,
            "halt_cleared_by_user": False,
        })
        self.state.update(dict(SWING_RESET))
        self.save_state()
        name = _display_symbol(self.symbol)
        notify_telegram(f"Sentinel ซื้อ {name}\nต้นทุน {avg_price} จำนวน {qty}")

    def _estimate_buy_cost(self, preferred=None):
        """ต้นทุนสำรองเมื่อยืนยัน fill ไม่ได้ — ใช้ offer ตอนซื้อก่อน แล้วค่อยราคาตลาด"""
        quote = self.state.get("quote") or {}
        for candidate in (preferred, quote_mark_price(quote, "buy"), quote_mark_price(quote, "last")):
            px = _safe_float(candidate, 0.0) or 0.0
            if px > 0:
                return px
        return _safe_float(self.get_latest_price(), 0.0) or 0.0

    def _halt_unknown_buy_cost(self, qty):
        """สำรองสุดท้ายเมื่อมีเหรียญแต่ยังไม่มีราคาเลย — รอบถัดไปจะถือต่อเองเมื่อมีราคา"""
        self.state.update({
            "status": "HALTED",
            "entry_price": 0.0,
            "highest_price": 0.0,
            "quantity": qty,
            "pending_order": None,
            "entry_price_estimated": False,
            "halt_cleared_by_user": False,
        })
        self.save_state()

    def _adopt_unknown_buy_cost(self, qty, estimated_price=None):
        """มีเหรียญในพอร์ตแต่ไม่รู้ fill จริง — ถือต่อทันทีด้วยราคาประมาณ ให้ trailing/ขายทำงาน ไม่รอปลดจากเว็บ"""
        px = self._estimate_buy_cost(estimated_price)
        if px <= 0:
            logger.error(
                f"[{self.symbol}] มีเหรียญเข้าพอร์ต {qty} แต่ยังไม่มีราคาตลาด "
                f"— HALTED ชั่วคราว จะถือต่อเองในรอบถัดไปเมื่อมีราคา (ไม่ต้องปลดจากเว็บ)"
            )
            self._halt_unknown_buy_cost(qty)
            notify_telegram(
                f"Sentinel { _display_symbol(self.symbol) } มีเหรียญเข้าพอร์ตแต่ยังไม่มีราคาตลาด — จะถือต่อเองเมื่อมีราคา"
            )
            return False
        fee_pct = self.estimate_roundtrip_fee_percent()
        latest = _safe_float(quote_mark_price(self.state.get("quote") or {}, "last"), 0.0) or 0.0
        highest = max(px, latest, float(self.state.get("highest_price") or 0.0))
        qty = self._absorb_dust_into_qty(qty)
        self.state.update({
            "status": "HOLDING",
            "entry_price": px,
            "highest_price": highest,
            "quantity": qty,
            "dust_quantity": 0.0,
            "roundtrip_fee_percent": fee_pct,
            "pending_order": None,
            "entry_price_estimated": True,
            "halt_cleared_by_user": False,
        })
        self.state.update(dict(SWING_RESET))
        self.save_state()
        logger.warning(
            f"[{self.symbol}] มีเหรียญ {qty} แต่ยืนยันราคาจับคู่ไม่ได้ "
            f"— ถือต่อทันทีด้วยต้นทุนประมาณ {px} (สูงสุดที่เห็น {highest}) "
            f"ไม่รอปลดจากเว็บ เพื่อไม่พลาดการขายตอนเหรียญวิ่ง"
        )
        notify_telegram(
            f"Sentinel ถือ {_display_symbol(self.symbol)} ต่อ\n"
            f"ต้นทุนประมาณ {px} (ยืนยันราคาจับคู่ไม่ได้) จำนวน {qty}"
        )
        return True

    def _finalize_buy(self, order_id=None, expected_buy=None, buy_value=None):
        """
        จบขั้นตอนซื้อด้วยของจริงในพอร์ต
        ถ้า order_id ไม่มี จะพยายามดึงจาก pending_order
        """
        # ถ้าไม่ได้รับ order_id ให้ลองดึงจาก pending
        if not order_id:
            pending = self.state.get("pending_order") or {}
            order_id = pending.get("order_id")

        dust = self._dust_threshold()
        avg_price = 0.0

        if order_id:
            avg_price = self.confirm_fill_price(order_id) or 0.0

        _, coin_free, still_pending = self._wait_for_coin_balance(dust)
        rules = self.get_symbol_rules()

        if float(coin_free or 0) > dust:
            qty = self._floor_to_increment(coin_free, rules["quantity_increment"])
            if avg_price > 0:
                if expected_buy:
                    self._check_slippage(expected_buy, avg_price, "ซื้อ")
                fee_pct = self.estimate_roundtrip_fee_percent()
                self._adopt_filled_buy(avg_price, qty, fee_pct)
                logger.info(
                    f"[{self.symbol}] ซื้อสำเร็จ ต้นทุนจริง {avg_price} THB จำนวนจริงในพอร์ต {qty}"
                    + (f" (ส่งคำสั่งมูลค่า {buy_value:.2f} THB)" if buy_value else "")
                    + f", ค่าธรรมเนียม round-trip โดยประมาณ {fee_pct:.3f}%"
                )
                return True

            # มีเหรียญแต่ยังไม่รู้ราคาจริง → ถือต่อด้วยราคาประมาณ
            logger.warning(
                f"[{self.symbol}] มีเหรียญเข้าพอร์ต {qty} แต่ยืนยันราคาจับคู่ไม่ได้ "
                f"— จะถือต่อด้วยต้นทุนประมาณทันที ไม่รอปลดจากเว็บ"
            )
            return self._adopt_unknown_buy_cost(qty, expected_buy)

        if still_pending:
            logger.warning(
                f"[{self.symbol}] ออเดอร์ยัง matching และยังไม่มีเหรียญในพอร์ต "
                f"— คง pending ไว้ ไม่ยิงซ้ำ"
            )
            return False

        self.state["pending_order"] = None
        self.save_state()
        logger.warning(f"[{self.symbol}] ซื้อไม่สำเร็จ: ไม่มีเหรียญเข้าพอร์ต")
        return False

    def _settle_pending_buy(self):
        pending = self.state.get("pending_order") or {}
        try:
            pending_side = int(pending.get("side", 0))
        except (TypeError, ValueError):
            pending_side = 0
        if pending_side == 1:
            logger.warning(f"[{self.symbol}] มี pending ขายค้าง — ไม่เปิดไม้ซื้อทับ")
            return False
        recovered = self._recover_order_after_timeout(0)
        order_id = None
        if recovered and recovered.get("code") == "0000":
            order_id = (recovered.get("data") or {}).get("orderId")
        return self._finalize_buy(
            order_id=order_id,
            expected_buy=None,
            buy_value=pending.get("value"),
        )

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
            notify_telegram(f"Sentinel หยุดเปิดไม้ใหม่\n{halt_reason}")
        save_shared_risk(risk)
        self.save_state()

    # ==================== Strategy ====================
    def analyze_trend(self, current_price):
        """จำยอดที่ขึ้น รอย่อ เด้งค่อยซื้อ — ไม่ไล่ตามและไม่ใช้หน้าต่างเวลา"""
        empty = {
            "ready": False, "should_buy": False, "direction": None, "confidence": 0,
            "impulse_pct": None, "dip_pct": None, "bounce_pct": None,
            "vetoed": False, "reason": "unknown", "elapsed": 0.0,
            "phase": "watch", "swing_high": 0.0, "swing_low": 0.0, "pullback_low": 0.0,
            "entry_armed": False,
        }
        if current_price is None:
            empty["reason"] = "no_price"
            return empty

        signal = apply_pullback_tick(current_price, self._swing_from_state())
        new_swing = swing_fields_from_signal(signal)
        changed = any(self.state.get(k) != v for k, v in new_swing.items())
        self.state.update(new_swing)
        if changed:
            self.save_state()

        quote = self.state.get("quote") or {}
        spread = quote.get("spread_pct")
        should_buy = bool(signal["should_buy"])
        reason = signal.get("reason")
        if should_buy and spread is not None and spread > MAX_SPREAD_PERCENT:
            should_buy = False
            reason = "wide_spread"
            logger.info(
                f"[{self.symbol}] สเปรด {spread:.3f}% กว้างเกิน {MAX_SPREAD_PERCENT}% "
                f"(bid {quote.get('bid')} / offer {quote.get('ask')}) — ไม่ซื้อ"
            )

        peak = signal.get("swing_high") or 0.0
        dip = signal.get("dip_pct")
        bounce = signal.get("bounce_pct")
        impulse = signal.get("impulse_pct")
        dip_txt = f"{dip:.2f}%" if dip is not None else "—"
        bounce_txt = f"{bounce:.2f}%" if bounce is not None else "—"
        impulse_txt = f"{impulse:+.2f}%" if impulse is not None else "—"
        logger.info(
            f"[{self.symbol}] ยอดที่จำ {peak or '—'} ขึ้นจากจุดต่ำ {impulse_txt} "
            f"ย่อ {dip_txt} เด้ง {bounce_txt} เฟส {signal.get('phase')}"
        )
        if should_buy:
            logger.info(
                f"[{self.symbol}] ผ่านเกณฑ์ซื้อตอนย่อ — ยอด {peak} ย่อ {dip_txt} เด้ง {bounce_txt}"
            )
        else:
            logger.info(f"[{self.symbol}] ไม่เข้าซื้อ — {REASON_TH.get(reason, reason or 'พร้อมซื้อ')}")

        return {
            "ready": True,
            "should_buy": should_buy,
            "direction": signal.get("direction"),
            "confidence": signal.get("confidence") or 0,
            "impulse_pct": impulse,
            "dip_pct": dip,
            "bounce_pct": bounce,
            "vetoed": False,
            "reason": None if should_buy else reason,
            "elapsed": history_elapsed_sec(self.state.get("price_history") or []),
            "phase": signal.get("phase"),
            "swing_high": peak,
            "swing_low": signal.get("swing_low") or 0.0,
            "pullback_low": signal.get("pullback_low") or 0.0,
            "entry_armed": bool(signal.get("entry_armed")),
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
        if is_watch_paused(self.symbol):
            logger.info(f"[{self.symbol}] หยุดเฝ้าอยู่ — ไม่เปิดไม้ซื้อ (กดเฝ้าต่อที่หน้าเว็บถ้าต้องการ)")
            return False
        cooldown_until = float(self.state.get("reversal_cooldown_until") or 0.0)
        if time.time() < cooldown_until:
            remaining_min = int((cooldown_until - time.time()) / 60) + 1
            logger.info(
                f"[{self.symbol}] เพิ่งขายด้วยสัญญาณกลับตัว — คูลดาวน์อีก ~{remaining_min} "
                f"นาที ไม่เปิดไม้ใหม่"
            )
            return False

        if self._has_unresolved_order():
            logger.warning(f"[{self.symbol}] มี pending หลัง timeout — เคลียร์จากพอร์ต ไม่ยิงคำสั่งใหม่")
            return self._settle_pending_buy()

        quote = self.state.get("quote") or {}
        spread = quote.get("spread_pct")
        if spread is not None and spread > MAX_SPREAD_PERCENT:
            logger.info(
                f"[{self.symbol}] ข้ามการซื้อ: สเปรด {spread:.3f}% กว้างเกิน {MAX_SPREAD_PERCENT}%"
            )
            return False
        expected_buy = quote_mark_price(quote, "buy") or current_price

        thb_free, coin_free, has_pending = self.get_free_balance()
        dust = self._dust_threshold()
        if has_pending:
            logger.info(f"[{self.symbol}] ข้ามการซื้อ: มีออเดอร์ค้างอยู่ในระบบ")
            return False
        if float(coin_free or 0) > dust:
            logger.error(
                f"[{self.symbol}] state บอก IDLE แต่มีเหรียญในพอร์ต {coin_free} "
                f"— ไม่ซื้อทับ ถือต่อด้วยต้นทุนประมาณทันที เพื่อให้ขายได้"
            )
            self._adopt_unknown_buy_cost(coin_free, current_price)
            return False
        if thb_free <= self.MIN_ORDER_THB:
            logger.info(f"[{self.symbol}] เงินว่างไม่พอสำหรับซื้อขั้นต่ำ (คงเหลือ {thb_free:.2f} THB)")
            return False

        slots = max(1, int(available_slots or 1))
        while slots > 1 and round((thb_free / slots) * (self.trade_size_percent / 100.0), 2) < self.MIN_ORDER_THB:
            slots -= 1
        buy_value = round((thb_free / slots) * (self.trade_size_percent / 100.0), 2)
        if buy_value < self.MIN_ORDER_THB:
            logger.info(
                f"[{self.symbol}] เงินที่แบ่งได้ {buy_value:.2f} THB ต่ำกว่าขั้นต่ำ {self.MIN_ORDER_THB} THB — ข้าม"
            )
            return False
        order_res = self.execute_market_order(side=0, value=buy_value)

        order_id = None
        if order_res and order_res.get("code") == "0000":
            order_id = (order_res.get("data") or {}).get("orderId")
            if order_id:
                logger.info(f"[{self.symbol}] ✔ ส่งคำสั่งซื้อสำเร็จ Order ID: {order_id} กำลังยืนยันจากพอร์ตจริง...")
            else:
                logger.warning(f"[{self.symbol}] ได้ code 0000 แต่ไม่มี orderId — จะเช็คยอดเหรียญในพอร์ต")
        else:
            logger.warning(
                f"[{self.symbol}] ยิงออเดอร์ซื้อไม่ยืนยันสำเร็จ: {order_res} "
                f"— จะเช็คยอดเหรียญในพอร์ตก่อน ห้ามยิงซ้ำถ้าของเข้าแล้ว"
            )

        return self._finalize_buy(order_id=order_id, expected_buy=expected_buy, buy_value=buy_value)

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

            # Trailing ใช้ล็อกกำไรหลังราคาขึ้นพอแล้ว ไม่ใช่ตัดไม้ที่ขึ้นแค่ติ๊ก
            # ต้องขึ้นสูงกว่าต้นทุนอย่างน้อย (ค่าธรรมเนียมไป-กลับ + % trailing)
            # ไม่งั้น default trail 1% จะแย่งตัดก่อน stop loss 3% ทุกครั้งที่ราคาย่อเล็กน้อย
            trailing_threshold = highest_price * (1 - self.trailing_stop_percent / 100)
            stop_loss_threshold = entry_price * (1 - self.stop_loss_percent / 100)
            breakeven_price = entry_price * (1 + fee_pct / 100)
            min_peak_price = entry_price * (1 + (fee_pct + self.trailing_stop_percent) / 100)
            trailing_armed = highest_price >= min_peak_price
            reversal = detect_momentum_reversal(self.state.get("price_history") or [])

            if sell_mark <= stop_loss_threshold:
                logger.warning("🚨 ถึงจุด Hard Stop Loss ขายทันทีเพื่อจำกัดความเสียหาย")
                self.sell_position(qty, sell_mark, reason="stop_loss")
            elif reversal.get("is_reversal") and sell_mark > breakeven_price:
                logger.info(
                    f"⚠️ [{self.symbol}] สัญญาณกลับตัว ({reversal['total_change']:+.2f}% ใน "
                    f"{REVERSAL_WINDOW_MINUTES} นาที) เหนือจุดคุ้มทุน — ขายล็อกกำไรทันที ไม่รอ trailing"
                )
                self.sell_position(qty, sell_mark, reason="momentum_reversal")
            elif sell_mark <= trailing_threshold:
                if not trailing_armed:
                    logger.info(
                        f"[{self.symbol}] ข้าม Trailing Stop: จุดสูงสุด {highest_price} "
                        f"ยังขึ้นไม่พอสำหรับล็อกกำไร (ต้องถึง {min_peak_price:.2f}) — ถือต่อ ให้ Stop Loss ดูแล"
                    )
                elif sell_mark > breakeven_price:
                    logger.info(
                        f"💰 ถึงจุด Trailing Stop ({self.trailing_stop_percent}%) และคุ้มค่าธรรมเนียม "
                        f"(breakeven {breakeven_price:.2f}) ขายล็อกกำไร"
                    )
                    self.sell_position(qty, sell_mark, reason="trailing")
                else:
                    logger.info(
                        f"[{self.symbol}] ถึง Trailing Stop แต่ราคา {sell_mark:.2f} "
                        f"ยังต่ำกว่า breakeven {breakeven_price:.2f} — ถือต่อ ไม่ขายทิ้งแบบไม่ได้กำไร"
                    )

    def sell_position(self, qty, current_price=None, reason=""):
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
            self.state.update(dict(SWING_RESET))
            self.save_state()
            return

        pnl_thb = net_pnl_thb(entry_price, sell_avg_price, sell_qty, fee_pct)
        logger.info(
            f"✔ ขายสำเร็จ ราคาเฉลี่ย {sell_avg_price} PnL สุทธิหลังหักค่าธรรมเนียม "
            f"{fee_pct:.3f}% = {pnl_thb:.2f} THB"
        )

        self._stash_unsellable_dust(coin_free, sell_qty)
        self.state.update({
            "status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0,
            "dust_quantity": float(self.state.get("dust_quantity") or 0.0),
            "reversal_cooldown_until": (
                time.time() + REVERSAL_COOLDOWN_MINUTES * 60
                if reason == "momentum_reversal" else 0.0
            ),
            "pending_order": None, "entry_price_estimated": False,
        })
        self.state.update(dict(SWING_RESET))
        self._register_trade_result(pnl_thb)
        self._after_successful_sell(pnl_thb, reason)

    def _after_successful_sell(self, pnl_thb, reason=""):
        name = _display_symbol(self.symbol)
        pnl_txt = f"{pnl_thb:+.2f} ฿"
        if reason == "stop_loss":
            set_watch_paused(self.symbol, True, reason="stop_loss")
            logger.warning(
                f"[{self.symbol}] ตัดขาดทุนแล้ว — หยุดเฝ้า ไม่ซื้อคืนจนกว่าจะกดเฝ้าต่อที่หน้าเว็บ"
            )
            notify_telegram(
                f"Sentinel ตัดขาดทุน {name} {pnl_txt}\n"
                f"หยุดเฝ้าแล้ว จะไม่ซื้อคืน จนกว่าคุณจะกดเฝ้าต่อที่หน้าเว็บ"
            )
            return
        if reason == "trailing":
            notify_telegram(f"Sentinel ขาย {name} (trailing) {pnl_txt}")
            return
        if reason == "momentum_reversal":
            notify_telegram(f"Sentinel ขาย {name} (สัญญาณกลับตัว) {pnl_txt}")
            return
        notify_telegram(f"Sentinel ขาย {name} {pnl_txt}")

    def resume_from_halt(self):
        """ปลด HALTED — ถ้ายังมีของค้าง ให้ถือเป็น HOLDING ด้วยราคาประมาณ แล้วขายได้ทันที"""
        _, coin_free, has_pending = self.get_free_balance()
        if has_pending:
            logger.warning(f"[{self.symbol}] ยังมีออเดอร์ค้าง — ยังไม่ปลด HALTED รอรอบถัดไป")
            return False
        rules = self.get_symbol_rules()
        dust_threshold = float(rules["quantity_increment"])
        if coin_free > dust_threshold:
            return self._adopt_unknown_buy_cost(coin_free)
        logger.info(f"[{self.symbol}] ปลด HALTED: ไม่มีเหรียญค้าง — กลับ IDLE")
        self.state.update({
            "status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0,
            "entry_price_estimated": False,
            "halt_cleared_by_user": True,
        })
        self.state.update(dict(SWING_RESET))
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

    def _maybe_alert_stale_price(self):
        if self.state.get("status") != "HOLDING":
            return
        hist = self.state.get("price_history") or []
        if not hist:
            return
        age = time.time() - hist[-1][0]
        if age < self.STALE_PRICE_ALERT_SEC:
            return
        now = time.time()
        if now - self._last_stale_alert_ts < self.STALE_PRICE_ALERT_COOLDOWN_SEC:
            return
        self._last_stale_alert_ts = now
        notify_telegram(
            f"⚠️ Sentinel {_display_symbol(self.symbol)} ราคาไม่อัปเดตมา "
            f"{int(age // 60)} นาที ขณะยังถือเหรียญอยู่ — เช็ค log/การเชื่อมต่อ API ด่วน"
        )

    def run_once(self):
        """รันหนึ่งรอบของ loop หลัก (ไม่ sleep) — เรียกจาก run() หรือจาก supervisor loop ใน __main__"""
        self.maybe_reconcile_periodically()
        self._maybe_reset_daily_counters()

        price = self.get_latest_price()
        if price is None:
            self._maybe_alert_stale_price()
            return
        self._record_price_tick(price)

        if self.state.get("status") == "HALTED":
            self.resume_from_halt()

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
  .coin-card.is-paused { border-color:#5A4A28; }
  .coin-head { display:flex; align-items:baseline; justify-content:space-between; gap:8px; }
  .coin-sym { font-size:15px; font-weight:700; letter-spacing:0.02em; }
  .coin-price { font-size:15px; font-weight:700; font-variant-numeric:tabular-nums; }
  .coin-meta { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
  .chip { font-size:11px; font-weight:600; padding:3px 8px; border-radius:999px; background:#33312A; color:var(--text-soft); }
  .chip.up { background:var(--green-soft); color:var(--green); }
  .chip.down { background:var(--red-soft); color:var(--red); }
  .chip.hold { background:var(--green-soft); color:var(--green); }
  .coin-sub { margin-top:8px; font-size:12px; color:var(--text-soft); line-height:1.45; }
  .coin-actions { display:flex; flex-wrap:wrap; align-items:center; gap:8px; margin-top:10px; }
  .coin-actions .password-input { flex:1; min-width:90px; margin:0; padding:8px 10px; font-size:12px; }

  .control { margin:28px 20px 0; }
  .control-stats { display:grid; grid-template-columns:1fr 1fr; gap:10px; margin-bottom:14px; }
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
          <div class="brand-sub">เฝ้า ${watch_count} เหรียญ · ซื้อตอนย่อ</div>
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
    ${coin_cards_html}
<section class="control">
<div class="control-stats">
${control_cards_html}
</div>
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
<div class="control-sub">ใช้เมื่อราคาเคยขึ้นพอที่จะล็อกกำไรได้ (สูงกว่าต้นทุนอย่างน้อยค่าธรรมเนียม + % นี้) ขึ้นแค่ติ๊กแล้วย่อ จะไม่ขาย ทิ้งให้ Stop Loss ตัดสิน</div>

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
  }, 15000);
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
    """สูตรเดียวกับบอท: ใช้ยอดที่จำไว้ใน state เทียบราคาล่าสุด"""
    empty = {
        "direction": None, "confidence": 0, "current": None,
        "impulse_pct": None, "dip_pct": None, "bounce_pct": None, "elapsed": 0.0,
        "should_buy": False, "vetoed": False, "reason": "unknown",
        "phase": "watch", "swing_high": 0.0, "swing_low": 0.0, "pullback_low": 0.0,
        "entry_armed": False,
    }
    history = trim_to_continuous_recent(state.get("price_history") or [])
    current = None
    if history:
        current = history[-1][1]
        empty["elapsed"] = history_elapsed_sec(history)
    empty["current"] = current
    empty["swing_low"] = float(state.get("swing_low") or 0.0)
    empty["swing_high"] = float(state.get("swing_high") or 0.0)
    empty["pullback_low"] = float(state.get("pullback_low") or 0.0)
    empty["entry_armed"] = bool(state.get("entry_armed"))
    if current is None:
        empty["reason"] = "no_price"
        if empty["entry_armed"] and empty["swing_high"]:
            empty["reason"] = "armed_wait_dip"
            empty["phase"] = "armed"
        return empty
    swing = {
        "swing_low": float(state.get("swing_low") or 0.0),
        "swing_high": float(state.get("swing_high") or 0.0),
        "pullback_low": float(state.get("pullback_low") or 0.0),
        "entry_armed": bool(state.get("entry_armed")),
    }
    signal = apply_pullback_tick(current, swing)
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
    waiting_impulse = 0
    armed_waiting = 0

    for sym in display_symbols:
        st = states_by_symbol.get(sym) or {}
        if st.get("status") == "HOLDING":
            holding_symbols.append(sym)
            hist = st.get("price_history") or []
            px = hist[-1][1] if hist else None
            entry = float(st.get("entry_price", 0.0) or 0.0)
            qty = float(st.get("quantity", 0.0) or 0.0)
            if px is not None and entry:
                fee_pct = float(
                    st.get("roundtrip_fee_percent")
                    or InnovestXTradingBot.DEFAULT_ROUNDTRIP_FEE_PERCENT
                )
                total_unrealized += net_pnl_thb(entry, px, qty, fee_pct)
        hist = st.get("price_history") or []
        if hist:
            ts = hist[-1][0]
            if newest_ts is None or ts > newest_ts:
                newest_ts = ts
        if st.get("status") != "HOLDING":
            tr = _trend_from_state(st)
            if tr.get("entry_armed") or tr.get("phase") in ("armed", "pullback", "bounce"):
                armed_waiting += 1
            else:
                waiting_impulse += 1

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
    if control.get("_uninitialized"):
        banners.append(
            '<div class="banner banner-warning">ยังอ่านรายการเหรียญจาก Firebase ไม่ได้ '
            "— ไม่ได้ลบของเดิม และบอทจะไม่เขียนทับ watchlist กำลังลองใหม่อัตโนมัติ</div>"
        )
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
            f"— บอทจะถือต่อเองเมื่อมีราคาตลาด แล้วขาย/trailing ได้เลย ไม่ต้องเฝ้าหน้าเว็บ "
            f"ถ้าค้างนานผิดปกติค่อยกดปลดล็อกด้านล่าง</div>"
        )

    paused_symbols = [s for s in (control.get("paused_symbols") or []) if s in display_symbols]
    pause_reasons = dict(control.get("pause_reasons") or {})
    if paused_symbols:
        names = ", ".join(_display_symbol(s) for s in paused_symbols)
        banners.append(
            f'<div class="banner banner-warning">หยุดเฝ้า: {names} '
            f"— บอทไม่ซื้อคืนเหรียญเหล่านี้ จนกว่าคุณจะกดเฝ้าต่อที่การ์ดเหรียญ</div>"
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
    if holding_count == 0 and (armed_waiting or waiting_impulse):
        best = None
        for sym in watchlist:
            st = states_by_symbol.get(sym) or {}
            if st.get("status") == "HOLDING":
                continue
            tr = _trend_from_state(st)
            if tr.get("entry_armed") or tr.get("phase") in ("armed", "pullback", "bounce"):
                best = (sym, tr)
                if tr.get("should_buy") or tr.get("phase") == "pullback":
                    break
        if best:
            sym, tr = best
            dip = tr.get("dip_pct") or 0.0
            peak = tr.get("swing_high") or 0.0
            pct = min(100, (dip / PULLBACK_MIN_DIP_PERCENT * 100) if PULLBACK_MIN_DIP_PERCENT else 0)
            if tr.get("should_buy"):
                label = f"ย่อจากยอด {_display_symbol(sym)} แล้วเด้ง — พร้อมซื้อเมื่อมีช่องว่าง"
                sub = f"ยอดที่จำ {_fmt_thb(peak)} ฿ · ย่อ {dip:.2f}%"
                pct = 100
            elif tr.get("phase") == "pullback":
                label = f"ย่อจากยอด {_display_symbol(sym)} แล้ว — รอเด้งเล็กน้อย"
                sub = f"ยอดที่จำ {_fmt_thb(peak)} ฿ · ย่อ {dip:.2f}%"
                pct = min(100, max(pct, 70))
            else:
                label = f"จำยอด {_display_symbol(sym)} แล้ว — ไม่ไล่ซื้อ รอว่าราคาย่อ"
                sub = f"ยอดที่จำ {_fmt_thb(peak)} ฿ · ย่อแล้ว {dip:.2f}% (เป้า {PULLBACK_MIN_DIP_PERCENT}%)"
            progress_html = (
                '<section class="progress-card">'
                f'<div class="progress-label">{label}</div>'
                f'<div class="progress-bar"><div class="progress-fill" style="width:{pct:.0f}%"></div></div>'
                f'<div class="progress-sub">{sub}</div>'
                "</section>"
            )
        elif waiting_impulse:
            progress_html = (
                '<section class="progress-card">'
                '<div class="progress-label">รอราคาขึ้นพอเพื่อจำยอด — ยังไม่ซื้อ</div>'
                '<div class="progress-bar"><div class="progress-fill" style="width:8%"></div></div>'
                f'<div class="progress-sub">ขึ้นจากจุดต่ำอย่างน้อย {PULLBACK_MIN_IMPULSE_PERCENT}% แล้วจะจำยอด รอย่อ ค่อยซื้อ</div>'
                "</section>"
            )

    control_cards_html = "".join([
        _render_card(
            "กำไรวันนี้ (รับรู้แล้ว)",
            f"{'+' if daily_pnl >= 0 else ''}{_fmt_thb(daily_pnl)} ฿",
            value_class="positive" if daily_pnl >= 0 else "negative",
        ),
        _render_card("ทุนเริ่มต้นวันนี้", f"{_fmt_thb(daily_start)} ฿"),
        _render_card("ขาดทุนติดกัน", f"{consecutive_losses} / {active_max_consecutive_losses} ไม้"),
        _render_card("เหรียญในรายการ", f"{len(watchlist)} ตัว", sub=f"ถือได้สูงสุด {max_open} ช่อง", value_class="accent"),
    ])

    password_field_html = ""
    password_field_inline = ""
    if os.environ.get("DASHBOARD_PASSWORD"):
        password_field_html = (
            '<input class="password-input" type="password" name="password" placeholder="รหัส" style="margin-top:8px;">'
        )
        password_field_inline = (
            '<input class="password-input" type="password" name="password" placeholder="รหัส">'
        )

    coin_parts = ['<div class="coin-list">']
    if not display_symbols:
        coin_parts.append(
            '<div class="coin-card"><div class="coin-sub">ยังไม่มีเหรียญในรายการ — เพิ่มด้านล่างได้เลย</div></div>'
        )
    paused_set = set(paused_symbols)
    for sym in display_symbols:
        st = states_by_symbol.get(sym) or {}
        status = st.get("status", "IDLE")
        trend = _trend_from_state(st)
        direction = trend.get("direction")
        confidence = trend.get("confidence") or 0
        current = trend.get("current")
        phase = trend.get("phase") or "watch"
        dip_pct = trend.get("dip_pct")
        impulse_pct = trend.get("impulse_pct")
        bounce_pct = trend.get("bounce_pct")
        swing_high = float(trend.get("swing_high") or 0.0)
        is_paused = sym in paused_set
        holding_cls = " is-holding" if status == "HOLDING" else ""
        if is_paused:
            holding_cls += " is-paused"
        price_txt = f"{_fmt_thb(current)} ฿" if current is not None else "—"
        chips = []
        if sym in pending_remove:
            chips.append('<span class="chip down">รอขายก่อนเอาออก</span>')
        if is_paused:
            if pause_reasons.get(sym) == "stop_loss":
                chips.append('<span class="chip down">ตัดขาดทุน — หยุดเฝ้า</span>')
            else:
                chips.append('<span class="chip down">หยุดเฝ้า</span>')
        if status == "HOLDING":
            chips.append('<span class="chip hold">ถืออยู่</span>')
            if st.get("entry_price_estimated"):
                chips.append('<span class="chip">ต้นทุนประมาณ</span>')
            entry = float(st.get("entry_price", 0.0) or 0.0)
            if current is not None and entry:
                d = (current - entry) / entry * 100
                chips.append(f'<span class="chip {"up" if d >= 0 else "down"}">{d:+.2f}% จากต้นทุน</span>')
        elif status == "HALTED":
            chips.append('<span class="chip down">HALTED — รอราคาตลาด บอทจะถือต่อเอง</span>')
        elif not is_paused:
            chips.append('<span class="chip" >เฝ้าอยู่</span>')
        if status != "HOLDING" and not is_paused:
            if trend.get("should_buy"):
                chips.append('<span class="chip up">พร้อมซื้อตอนย่อ</span>')
            elif phase == "pullback":
                chips.append('<span class="chip up">ย่อแล้ว รอเด้ง</span>')
            elif phase == "armed" or trend.get("entry_armed"):
                chips.append('<span class="chip">จำยอดแล้ว รอว่าราคาย่อ</span>')
            else:
                chips.append('<span class="chip">รอพุ่งเพื่อจำยอด</span>')
        if swing_high and status != "HOLDING":
            chips.append(f'<span class="chip">ยอดที่จำ {_fmt_thb(swing_high)}</span>')
        if impulse_pct:
            chips.append(f'<span class="chip {"up" if impulse_pct >= 0 else "down"}">ขึ้นมา {impulse_pct:+.2f}%</span>')
        if dip_pct:
            chips.append(f'<span class="chip {"down" if dip_pct > 0 else ""}">ย่อ {dip_pct:.2f}%</span>')
        if bounce_pct:
            chips.append(f'<span class="chip up">เด้ง {bounce_pct:+.2f}%</span>')
        if direction == "down" and phase == "watch":
            chips.append('<span class="chip down">ยังลงอยู่</span>')
        quote = st.get("quote") or {}
        bid, ask, spread = quote.get("bid"), quote.get("ask"), quote.get("spread_pct")
        if bid and ask:
            chips.append(f'<span class="chip">bid {_fmt_thb(bid)} / offer {_fmt_thb(ask)}</span>')
        if spread is not None:
            chips.append(
                f'<span class="chip {"down" if spread > MAX_SPREAD_PERCENT else ""}">สเปรด {spread:.2f}%</span>'
            )
        if is_paused and pause_reasons.get(sym) == "stop_loss":
            sub = "ตัดขาดทุนแล้ว — บอทไม่ซื้อคืน จนกว่าคุณจะกดเฝ้าต่อ"
        elif is_paused:
            sub = "หยุดเฝ้าอยู่ — บอทไม่เปิดไม้ซื้อ จนกว่าคุณจะกดเฝ้าต่อ"
        elif status == "HOLDING":
            entry_txt = _fmt_thb(float(st.get("entry_price", 0) or 0))
            if st.get("entry_price_estimated"):
                sub = f"ต้นทุนประมาณ {entry_txt} ฿ (ยืนยันจับคู่ไม่ได้) · ดูแลด้วย trailing / stop loss "
            else:
                sub = f"ต้นทุน {entry_txt} ฿ · ดูแลด้วย trailing / stop loss "
        elif status == "HALTED":
            sub = "ยังดึงราคาไม่ได้ — บอทจะถือต่อเองเมื่อมีราคาตลาด แล้วขายได้ ไม่ต้องรอปลดจากเว็บ"
        else:
            reason_th = REASON_TH.get(trend.get("reason"), "")
            if trend.get("should_buy"):
                sub = (
                    f"ย่อจากยอด {_fmt_thb(swing_high)} ฿ แล้วเด้ง — รอช่องว่างเพื่อเข้าซื้อ"
                    if swing_high else
                    "ผ่านเกณฑ์ซื้อตอนย่อ — รอช่องว่างเพื่อเข้าซื้อ"
                )
            elif reason_th:
                sub = reason_th
            else:
                sub = (
                    f"จำยอดแล้วรอย่อ {PULLBACK_MIN_DIP_PERCENT}–{PULLBACK_MAX_DIP_PERCENT}% "
                    f"จากยอดที่จำไว้ แล้วค่อยซื้อ"
                )
        if is_paused:
            watch_btn = '<button type="submit" name="action" value="resume" class="btn btn-sm btn-accent">เฝ้าต่อ</button>'
        else:
            watch_btn = '<button type="submit" name="action" value="pause" class="btn btn-sm btn-neutral">หยุดเฝ้า</button>'
        coin_parts.append(
            f'<article class="coin-card{holding_cls}">'
            f'<div class="coin-head"><div class="coin-sym">{_display_symbol(sym)}</div>'
            f'<div class="coin-price">{price_txt}</div></div>'
            f'<div class="coin-meta">{"".join(chips)}</div>'
            f'<div class="coin-sub">{sub}</div>'
            f'<form class="coin-actions" method="POST" action="/control/coin-watch">'
            f'<input type="hidden" name="symbol" value="{sym}">'
            f'{password_field_inline}{watch_btn}'
            f'</form>'
            "</article>"
        )
    coin_parts.append("</div>")
    coin_cards_html = "".join(coin_parts)

    watch_rows = ['<div class="watch-list">']
    for sym in watchlist:
        st = states_by_symbol.get(sym) or {}
        status = st.get("status", "IDLE")
        holding = status == "HOLDING"
        if sym in paused_set:
            meta = "หยุดเฝ้าอยู่ — กดเฝ้าต่อที่การ์ดด้านบนถ้าต้องการให้บอทซื้อคืน"
        elif holding:
            meta = "ถืออยู่ — ติ๊กเอาออกได้ บอทจะรอขายก่อนแล้วค่อยเลิกเฝ้า"
        else:
            meta = "เฝ้าอยู่ รอสัญญาณขาขึ้น"
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
            unlock_sub = f"{names} — ปกติบอทถือต่อเองเมื่อมีราคา ถ้าค้างอยู่ให้กดเพื่อถือต่อด้วยราคาตลาดแล้วขายได้"
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
        coin_cards_html=coin_cards_html,
        control_cards_html=control_cards_html,
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
AUTH_COOKIE_NAME = "sentinel_auth"
AUTH_COOKIE_MAX_AGE_SEC = 30 * 24 * 3600
AUTH_COOKIE_SALT = b"sentinel-dashboard-v1"


def _passwords_match(given, required):
    if not required:
        return True
    if given is None:
        return False
    try:
        return hmac.compare_digest(str(given).encode("utf-8"), str(required).encode("utf-8"))
    except Exception:
        return False


def _auth_cookie_token(password):
    return hmac.new(str(password).encode("utf-8"), AUTH_COOKIE_SALT, hashlib.sha256).hexdigest()


def _render_login_page(error=False):
    err = (
        '<div class="banner banner-danger">รหัสผ่านไม่ถูกต้อง ลองอีกครั้ง</div>'
        if error else ""
    )
    return f"""<!DOCTYPE html>
<html lang="th">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Sentinel · ใส่รหัสผ่าน</title>
<style>
  :root {{
    --bg: #1E1C18; --card: #2A2822; --border: #3D3A32;
    --text: #EDE9DD; --text-soft: #9C9585;
    --accent: #E08A65; --accent-soft: #3D2C22;
    --red: #E08277; --red-soft: #3A2523;
  }}
  * {{ box-sizing: border-box; }}
  body {{ margin:0; background:var(--bg); color:var(--text);
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
    -webkit-font-smoothing:antialiased; min-height:100vh;
    display:flex; justify-content:center; padding:0 20px; }}
  .app {{ width:100%; max-width:420px; padding:48px 0 40px; }}
  .brand {{ display:flex; align-items:center; gap:10px; margin-bottom:28px; }}
  .spark {{ width:22px; height:22px; flex-shrink:0; }}
  .spark path {{ fill: var(--accent); }}
  .brand-title {{ font-size:15px; font-weight:700; }}
  .brand-sub {{ font-size:12px; color:var(--text-soft); margin-top:1px; }}
  .card {{ background:var(--card); border:1px solid var(--border); border-radius:16px; padding:20px; }}
  h1 {{ font-size:18px; font-weight:700; margin:0 0 8px; }}
  .hint {{ font-size:13px; color:var(--text-soft); line-height:1.5; margin:0 0 16px; }}
  .banner {{ margin:0 0 14px; padding:12px 14px; border-radius:12px; font-size:13px;
    font-weight:600; line-height:1.5; }}
  .banner-danger {{ background:var(--red-soft); color:var(--red); }}
  label {{ display:block; font-size:12px; font-weight:600; margin-bottom:8px; }}
  input {{ width:100%; padding:14px 12px; border:1px solid var(--border); border-radius:10px;
    font-size:16px; background:var(--bg); color:var(--text); }}
  input:focus {{ outline:none; border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-soft); }}
  button {{ width:100%; margin-top:14px; border:none; border-radius:10px; padding:14px 16px;
    font-size:15px; font-weight:700; background:var(--accent); color:#fff; cursor:pointer; }}
</style>
</head>
<body>
  <div class="app">
    <div class="brand">
      <svg class="spark" viewBox="0 0 24 24"><path d="M12 0c.6 3.8 2.2 6.4 5 8.2 2.8 1.8 5.4 2 7 2-1.6 0-4.2.2-7 2C14.2 14 12.6 16.6 12 20.4c-.6-3.8-2.2-6.4-5-8.2-2.8-1.8-5.4-2-7-2 1.6 0 4.2-.2 7-2C9.8 6.4 11.4 3.8 12 0z"/></svg>
      <div>
        <div class="brand-title">Sentinel</div>
        <div class="brand-sub">หน้าควบคุมบอท</div>
      </div>
    </div>
    {err}
    <form class="card" method="POST" action="/login" autocomplete="on">
      <h1>ใส่รหัสผ่าน</h1>
      <p class="hint">มือถือมักไม่โชว์กล่องรหัสของเบราว์เซอร์ — พิมพ์ในช่องนี้ได้เลย รหัสเดียวกับที่ตั้งใน Render</p>
      <label for="password">รหัสผ่าน</label>
      <input id="password" name="password" type="password" autocomplete="current-password"
        autocapitalize="off" autocorrect="off" spellcheck="false" required autofocus>
      <button type="submit">เข้าสู่แดชบอร์ด</button>
    </form>
  </div>
</body>
</html>
"""


class HealthCheckHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()

    def _request_path(self):
        return (self.path or "/").split("?", 1)[0]

    def _parse_cookies(self):
        raw = self.headers.get("Cookie") or ""
        out = {}
        for part in raw.split(";"):
            name, _, value = part.strip().partition("=")
            if name:
                out[name] = urllib.parse.unquote(value)
        return out

    def _cookie_secure(self):
        proto = (self.headers.get("X-Forwarded-Proto") or "").split(",")[0].strip().lower()
        return proto == "https"

    def _auth_cookie_header(self, password):
        token = _auth_cookie_token(password)
        parts = [
            f"{AUTH_COOKIE_NAME}={token}",
            "Path=/",
            "HttpOnly",
            "SameSite=Lax",
            f"Max-Age={AUTH_COOKIE_MAX_AGE_SEC}",
        ]
        if self._cookie_secure():
            parts.append("Secure")
        return "; ".join(parts)

    def _cookie_matches(self, required):
        if not required:
            return True
        got = self._parse_cookies().get(AUTH_COOKIE_NAME, "")
        if not got:
            return False
        try:
            return hmac.compare_digest(got, _auth_cookie_token(required))
        except Exception:
            return False

    def _basic_auth_matches(self, required):
        auth_header = self.headers.get("Authorization", "")
        if not auth_header.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(auth_header[6:]).decode("utf-8")
            _, _, pw = decoded.partition(":")
        except Exception:
            return False
        return _passwords_match(pw, required)

    def _check_auth(self):
        required = os.environ.get("DASHBOARD_PASSWORD")
        if not required:
            return True
        return self._cookie_matches(required) or self._basic_auth_matches(required)

    def _send_login_page(self, error=False):
        html = _render_login_page(error=error)
        # 200 ไม่ใช่ 401 และไม่ส่ง WWW-Authenticate — มือถือ/in-app browser มักบล็อกกล่อง Basic Auth
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(html.encode("utf-8"))

    def _require_auth(self):
        if self._check_auth():
            return True
        self._send_login_page(error=False)
        return False

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        if not self._require_auth():
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
            path = self._request_path()

            required_password = os.environ.get("DASHBOARD_PASSWORD")
            given_password = fields.get("password", [""])[0]
            form_ok = bool(required_password) and _passwords_match(given_password, required_password)

            if path == "/login":
                if required_password and form_ok:
                    self.send_response(303)
                    self.send_header("Location", "/")
                    self.send_header("Set-Cookie", self._auth_cookie_header(required_password))
                    self.end_headers()
                    return
                self._send_login_page(error=True)
                return

            if required_password:
                cookie_ok = self._cookie_matches(required_password)
                basic_ok = self._basic_auth_matches(required_password)
                if not (cookie_ok or basic_ok or form_ok):
                    self._send_login_page(error=True)
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

            elif self.path == "/control/coin-watch":
                control = load_control()
                symbol = _normalize_symbol(fields.get("symbol", [""])[0])
                action = (fields.get("action", [""])[0] or "").strip().lower()
                watchlist = list(control.get("watchlist") or [])
                if not looks_like_pair(symbol) or symbol not in watchlist:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธหยุด/เฝ้าต่อ: เหรียญไม่ได้อยู่ในรายการ ({symbol})")
                elif action == "pause":
                    control = apply_watch_pause(control, symbol, True, reason="user")
                    save_control(control)
                    logger.info(f"[เว็บควบคุม] หยุดเฝ้า {symbol} — จะไม่เปิดไม้ซื้อจนกว่าจะกดเฝ้าต่อ")
                    notify_telegram(f"Sentinel หยุดเฝ้า {_display_symbol(symbol)} ตามคำสั่งจากหน้าเว็บ")
                elif action == "resume":
                    control = apply_watch_pause(control, symbol, False)
                    save_control(control)
                    logger.info(f"[เว็บควบคุม] เฝ้าต่อ {symbol}")
                    notify_telegram(f"Sentinel เฝ้าต่อ {_display_symbol(symbol)} — พร้อมซื้อเมื่อเข้าเกณฑ์")
                else:
                    logger.warning(f"[เว็บควบคุม] ไม่รู้จักคำสั่งเฝ้าเหรียญ: {action}")

            self.send_response(303)
            self.send_header("Location", "/")
            if required_password and form_ok:
                self.send_header("Set-Cookie", self._auth_cookie_header(required_password))
            self.end_headers()

        except Exception as e:
            logger.error(f"Dashboard control error: {e}")
            self.send_response(500)
            self.end_headers()


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def start_dummy_health_check_server():
    port = int(os.environ.get("PORT", 10000))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info(f"เปิดหน้าเว็บและ /health ที่พอร์ต {port} (แยกคิวจากแดชบอร์ด)")
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
    logger.info("เปิด /health แล้ว กำลังเชื่อม Firebase — ถ้าเชื่อมช้า Render จะไม่คิดว่าเครื่องตาย")

    POLL_INTERVAL_SEC = 10

    control = wait_for_fresh_control()
    if _CONTROL_CACHE.get("fresh") and not control.get("_uninitialized"):
        save_control(control)
    else:
        logger.error(
            "ยังอ่าน bot_control จาก Firebase ไม่ได้ — เริ่มลูปโดยไม่เขียนทับรายการเหรียญ "
            "จะลองใหม่ทุกม้วน"
        )

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
    n_cycle = 0
    while not stop_all:
        n_cycle += 1
        try:
            control = load_control()
            if control.get("_uninitialized"):
                logger.warning(
                    f"รอบที่ {n_cycle} ยังอ่าน bot_control ไม่ได้ — คงรายการเหรียญเดิม ไม่เพิ่ม/ลบ/เขียนทับ"
                )
                wanted = list(bots.keys())
            else:
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
                if not control.get("_uninitialized"):
                    _sync_bot_settings(bot, control)

            hist_bits = []
            for b in bots.values():
                elapsed = history_elapsed_sec(b.state.get("price_history") or [])
                mins = int(elapsed // 60)
                hist_bits.append(
                    f"{b.symbol} {b.state.get('status')} {mins}/120น."
                )
            logger.info(
                f"รอบที่ {n_cycle} เฝ้า {list(bots.keys()) or '-'} "
                f"paused={bool(control.get('paused'))} | " + (" · ".join(hist_bits) or "ยังไม่มีเหรียญ")
            )

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
                        if px is None:
                            bot._maybe_alert_stale_price()
                            continue
                        if bot.state.get("status") == "HALTED":
                            bot.resume_from_halt()
                        if bot.state.get("status") == "HOLDING":
                            bot.run_strategy(px)
                    except Exception:
                        logger.exception(f"[{bot.symbol}] ดึงราคา/ดูแลโพซิชันตอนหยุดชั่วคราวล้มเหลว")
            else:
                prices = {}
                for bot in bots.values():
                    try:
                        prices[bot.symbol] = bot.poll_price()
                        if prices[bot.symbol] is None:
                            bot._maybe_alert_stale_price()
                    except Exception:
                        logger.exception(f"[{bot.symbol}] ดึงราคาล้มเหลว")
                    time.sleep(0.2)

                for bot in bots.values():
                    px = prices.get(bot.symbol)
                    if px is None:
                        continue
                    if bot.state.get("status") == "HALTED":
                        try:
                            bot.resume_from_halt()
                        except Exception:
                            logger.exception(f"[{bot.symbol}] ปลด HALTED อัตโนมัติล้มเหลว")
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
                            if is_watch_paused(bot.symbol, control):
                                logger.info(
                                    f"[{bot.symbol}] สัญญาณขาขึ้นแต่หยุดเฝ้าอยู่ — ไม่ซื้อ "
                                    f"(กดเฝ้าต่อที่หน้าเว็บถ้าต้องการ)"
                                )
                                continue
                            candidates.append((bot, px, signal))

                    candidates.sort(
                        key=lambda item: (
                            item[2].get("confidence") or 0,
                            item[2].get("impulse_pct") or 0,
                            item[2].get("bounce_pct") or 0,
                        ),
                        reverse=True,
                    )

                    for bot, px, signal in candidates:
                        holding_count = sum(1 for b in bots.values() if b.state.get("status") == "HOLDING")
                        remaining_slots = int(control["max_open_positions"]) - holding_count
                        if remaining_slots <= 0:
                            logger.info("ช่องถือเต็มแล้ว — เหรียญที่เหลือจะรอจนมีช่องว่างหลังขาย")
                            break
                        dip_txt = f"{signal.get('dip_pct'):.2f}%" if signal.get("dip_pct") is not None else "n/a"
                        peak = signal.get("swing_high") or 0.0
                        logger.info(
                            f"[{bot.symbol}] ซื้อตอนย่อ คะแนน {signal.get('confidence') or 0}% "
                            f"(ยอด {peak} ย่อ {dip_txt}) — พยายามเข้าซื้อ "
                            f"(แบ่งเงินจาก {remaining_slots} ช่องว่าง)"
                        )
                        try:
                            bot.try_enter_position(px, available_slots=remaining_slots)
                        except Exception:
                            logger.exception(f"[{bot.symbol}] เข้าซื้อล้มเหลว")
                        time.sleep(0.3)
        except Exception:
            logger.exception(f"รอบที่ {n_cycle} ล้มเหลว — จะลองใหม่ในรอบถัดไป")

        for _ in range(POLL_INTERVAL_SEC):
            if STOP_ALL["value"] or any(b._stop_requested for b in bots.values()):
                stop_all = True
                break
            time.sleep(1)

    for bot in bots.values():
        bot.save_state()
        bot.save_market(force_history=True)
    logger.info("บอทหยุดทำงานเรียบร้อย (state ถูกบันทึกแล้ว)")
