"""
InnovestX Automated Trading Bot — Price Action 3H Strategy (v4, dashboard + control + ปรับอัตราเงิน/จำนวนไม้ขาดทุนได้จากหน้าเว็บ)

สืบเนื่องจาก v3 (dashboard + control) เดิม เพิ่มเติมในเวอร์ชันนี้:

1. ปรับ "อัตราเงินที่ใช้เข้าซื้อต่อไม้" ได้จากหน้าเว็บ (% ของเงินบาทว่าง) แทนที่จะ fix ไว้ที่ 95% ตายตัว
2. ปรับ "จำนวนไม้ขาดทุนติดกันก่อนหยุดเทรด (Circuit Breaker)" ได้จากหน้าเว็บ แทนที่จะ fix ไว้ที่ 3 ไม้ตายตัว
3. ปุ่ม "ปลดล็อกตอนนี้" (ไม่ต้องรอข้ามวัน) มีอยู่แล้วตั้งแต่ v3 — ไม่ได้แก้เพิ่มในเวอร์ชันนี้

ของเดิมจาก v3 (ยังอยู่ครบ ไม่ได้ตัดออก): หน้าแดชบอร์ด, เลือกเหรียญจากหน้าเว็บ, ปุ่มหยุด/เริ่มเทรดต่อ,
รหัสผ่านป้องกันหน้าควบคุม (DASHBOARD_PASSWORD), ปรับ % ขาดทุนสูงสุดต่อวันจากหน้าเว็บ,
price_history สะสมจริงจาก Firebase, คำนวณค่าธรรมเนียมเข้าไปในจุดตัดสินใจขาย, จัดการ error code เฉพาะทาง,
Circuit breaker, log ทุก request-uid, ใช้ Decimal ปัดจำนวนเหรียญ, เช็ค minimum notional, graceful shutdown

⚠️ สมมติฐานที่ต้องตรวจสอบกับเอกสาร API ฉบับเต็มก่อนรันจริง (เหมือนเดิมจาก v3):
- FEE_ESTIMATE_PATH ด้านล่าง: คู่มือพูดถึงฟีเจอร์ "Get Estimate Fee" แต่ไม่ได้ให้ path ตรงๆ
  ตั้งตาม naming convention ของ endpoint อื่นที่มีอยู่แล้ว — ต้องยืนยันก่อนใช้จริง
- get_latest_price(): คู่มือเรียก ticker ว่า "Subscribe" และบอกว่าส่งข้อมูลทุก 1 นาที
  ฟังดูเหมือน WebSocket push มากกว่า REST request/response ปกติ — ถ้า endpoint นี้ใช้แบบ REST จริงไม่ได้
  ให้เปลี่ยนมาใช้ WebSocket client แทนการ poll ในฟังก์ชันนี้
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
logger = logging.getLogger("InnovestXBot")

# ==================== Firebase (state persistence) ====================
def init_firebase():
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate("/etc/secrets/firebase.json")
            db_url = os.getenv("FIREBASE_DATABASE_URL")
            firebase_admin.initialize_app(cred, {"databaseURL": db_url})
            logger.info("Firebase Realtime Database เชื่อมต่อสำเร็จ")
    except Exception as e:
        logger.error(f"Firebase Init Error: {e}")

init_firebase()

# ==================== การควบคุมบอทจากหน้าเว็บ ====================
DEFAULT_SYMBOL = os.environ.get("SYMBOL", "BTCTHB").upper()
RUNNING_SYMBOL = {"value": DEFAULT_SYMBOL}


def load_control():
    """อ่านคำสั่งควบคุมล่าสุดจากหน้าเว็บ (เหรียญที่ต้องการ, หยุดชั่วคราวหรือไม่, % ขาดทุนสูงสุดต่อวัน,
    อัตราเงินที่ใช้เข้าซื้อต่อไม้, จำนวนไม้ขาดทุนติดกันก่อนหยุด)"""
    try:
        data = db.reference("bot_control").get() or {}
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

    return {
        "active_symbol": (data.get("active_symbol") or DEFAULT_SYMBOL).upper(),
        "paused": bool(data.get("paused", False)),
        "max_daily_loss_percent": max_daily_loss_percent,
        "trade_size_percent": trade_size_percent,
        "max_consecutive_losses": max_consecutive_losses,
        "unlock_requested": bool(data.get("unlock_requested", False)),
    }


def save_control(control):
    try:
        db.reference("bot_control").set(control)
    except Exception as e:
        logger.error(f"บันทึก bot_control ไป Firebase ไม่สำเร็จ: {e}")


class InnovestXTradingBot:
    # ---- ค่าคงที่ที่ปรับได้ ----
    MAX_DAILY_LOSS_PERCENT = 5.0        # หยุดเทรดถ้าขาดทุนสะสมวันนี้เกิน % ของทุนเริ่มวัน (ปรับได้จากหน้าเว็บ)
    MAX_CONSECUTIVE_LOSSES = 3          # หยุดเทรดถ้าขาดทุนติดกันกี่ไม้ (ปรับได้จากหน้าเว็บ)
    DEFAULT_TRADE_SIZE_PERCENT = 95.0   # % ของเงินบาทว่างที่ใช้เข้าซื้อต่อไม้ (ปรับได้จากหน้าเว็บ)
    DEFAULT_ROUNDTRIP_FEE_PERCENT = 0.50  # fallback ถ้าดึงค่าธรรมเนียมจริงไม่ได้ (ปรับให้ตรงจริง!)
    MIN_ORDER_THB = 100.0
    MAX_ACCEPTABLE_SLIPPAGE_PERCENT = 1.0  # ถ้าราคาจริงเพี้ยนจากที่คาดเกิน % นี้จะแจ้งเตือน
    REQUEST_TIMEOUT_SEC = 10
    MAX_RETRIES = 3
    FEE_ESTIMATE_PATH = "/api/v1/digital-asset/order/fee/inquiry"

    def __init__(self, api_key, api_secret, symbol="BTCTHB", base_currency="THB",
                 target_currency=None, trailing_stop_percent=2.0, stop_loss_percent=1.5):
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

        self.trailing_stop_percent = trailing_stop_percent
        self.stop_loss_percent = stop_loss_percent

        # ปรับได้จากหน้าเว็บระหว่างรัน (ไม่ต้อง restart) — ค่าเริ่มต้นใช้ค่าคงที่ของคลาสไปก่อน
        # ตัวแปรพวกนี้จะถูกอัปเดตจริงทุกรอบ loop ใน __main__ จากค่าที่เก็บบน Firebase (bot_control)
        self.max_daily_loss_percent = self.MAX_DAILY_LOSS_PERCENT
        self.max_consecutive_losses = self.MAX_CONSECUTIVE_LOSSES
        self.trade_size_percent = self.DEFAULT_TRADE_SIZE_PERCENT

        self._stop_requested = False
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        self.state = self.load_state()

    # ==================== State management ====================
    def load_state(self):
        default_state = {
            "status": "IDLE",  # IDLE / HOLDING / HALTED
            "entry_price": 0.0,
            "highest_price": 0.0,
            "quantity": 0.0,
            "roundtrip_fee_percent": self.DEFAULT_ROUNDTRIP_FEE_PERCENT,
            "price_history": [],  # [[timestamp_sec, price], ...] เก็บย้อนหลัง 3 ชม.
            "trade_date": None,   # "YYYY-MM-DD" (UTC) สำหรับรีเซ็ต circuit breaker รายวัน
            "daily_start_balance": 0.0,
            "daily_realized_pnl": 0.0,
            "consecutive_losses": 0,
        }
        try:
            saved = db.reference(self.state_path).get()
            if saved:
                default_state.update(saved)
                logger.info(f"โหลดสถานะบอทจาก Firebase สำเร็จ: status={default_state['status']}")
            else:
                logger.info("ยังไม่มีสถานะเดิมบน Firebase (path ว่าง) — เริ่มจากค่าเริ่มต้น")
        except Exception as e:
            logger.warning(f"อ่านสถานะจาก Firebase ไม่ได้ กำลังใช้ค่าเริ่มต้น: {e}")
        return default_state

    def save_state(self):
        try:
            db.reference(self.state_path).set(self.state)
        except Exception as e:
            logger.error(f"บันทึกสถานะไป Firebase ไม่สำเร็จ: {e}")

    def _handle_shutdown(self, signum, frame):
        logger.info(f"ได้รับสัญญาณหยุด ({signum}) กำลังบันทึกสถานะก่อนปิดบอท...")
        self._stop_requested = True
        self.save_state()

    def reconcile_state_on_startup(self):
        """เช็คว่า state ตรงกับยอดจริงในพอร์ตหรือไม่ ก่อนเริ่ม loop"""
        logger.info(f"[{self.symbol}] กำลังตรวจสอบสถานะกับยอดจริงในพอร์ต (Reconcile)...")
        _, coin_free, _ = self.get_free_balance()
        rules = self.get_symbol_rules()
        dust_threshold = float(rules["quantity_increment"])

        if self.state["status"] == "HOLDING" and coin_free <= dust_threshold:
            logger.error(f"⚠️ RECONCILE MISMATCH: state บอก HOLDING แต่ในพอร์ตมีแค่ {coin_free} "
                         f"(อาจขายไปแล้วตอนบอทออฟไลน์) รีเซ็ตเป็น IDLE เพื่อความปลอดภัย")
            self.state.update({"status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0})
            self.save_state()
        elif self.state["status"] == "IDLE" and coin_free > dust_threshold:
            logger.error(f"⚠️ RECONCILE MISMATCH: state บอก IDLE แต่มีเหรียญค้างอยู่ {coin_free} "
                         f"(ไม่รู้ต้นทุนจริง) บอทจะไม่เทรดอัตโนมัติจนกว่าจะตรวจสอบด้วยมือ")
            self.state["status"] = "HALTED"
            self.save_state()
        else:
            logger.info(f"Reconcile ผ่าน: state={self.state['status']} ตรงกับพอร์ตจริง "
                        f"({coin_free} {self.target_currency})")

    # ==================== HTTP / signing ====================
    def send_request(self, method, path, query="", body=None, _retry_count=0):
        """ส่ง request พร้อม HMAC-SHA256 signature, timeout, retry/backoff และ audit log ต่อ request-uid"""
        url = self.base_url + path + query
        body_str = json.dumps(body) if body else ""
        timestamp = str(int(time.time() * 1000))
        request_uid = str(uuid.uuid4())
        content_type = "application/json"

        content_to_sign = (
            self.api_key + method.upper() + self.host + path + query +
            content_type + request_uid + timestamp + body_str
        )
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            content_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "Content-Type": content_type,
            "X-INVX-REQUEST-UID": request_uid,
            "X-INVX-TIMESTAMP": timestamp,
            "X-INVX-SIGNATURE": signature,
            "X-INVX-APIKEY": self.api_key,
        }

        try:
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, timeout=self.REQUEST_TIMEOUT_SEC)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, data=body_str, timeout=self.REQUEST_TIMEOUT_SEC)
            else:
                raise ValueError(f"Method {method} ไม่รองรับ")
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            logger.warning(f"[UID {request_uid}] เชื่อมต่อไม่สำเร็จ ({e}) — retry {_retry_count + 1}/{self.MAX_RETRIES}")
            if _retry_count < self.MAX_RETRIES:
                time.sleep(2 ** _retry_count)
                return self.send_request(method, path, query, body, _retry_count + 1)
            logger.error(f"[UID {request_uid}] ยกเลิกหลัง retry ครบจำนวน: {e}")
            return None

        if response.status_code == 429 or response.status_code >= 500:
            logger.warning(f"[UID {request_uid}] HTTP {response.status_code} — retry {_retry_count + 1}/{self.MAX_RETRIES}")
            if _retry_count < self.MAX_RETRIES:
                time.sleep(2 ** _retry_count)
                return self.send_request(method, path, query, body, _retry_count + 1)
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
            logger.warning(f"[UID {request_uid}] 4042 Symbol not found — ตรวจสอบ symbol '{self.symbol}'")

        return data

    # ==================== Market data / price history ====================
    def get_latest_price(self):
        """ดึงราคาจับคู่ซื้อขายล่าสุดจริง (Last Trade Price) แบบเรียลไทม์จาก Level 2 Order Book"""
        path = "/api/v1/digital-asset/orderbook/lvl2"
        body = {"symbol": self.symbol, "depth": 1}
        res = self.send_request("POST", path, body=body)
        if res and res.get("code") == "0000":
            data = res.get("data")
            if isinstance(data, list) and data:
                first_record = data[0]
                if isinstance(first_record, dict) and "lastTradePrice" in first_record:
                    try:
                        return float(first_record["lastTradePrice"])
                    except (TypeError, ValueError):
                        pass
        logger.warning("ดึงราคาล่าสุดเรียลไทม์ (lastTradePrice) ล้มเหลว")
        return None

    def _record_price_tick(self, price):
        now = time.time()
        self.state["price_history"].append([now, price])
        cutoff = now - 10800  # เก็บย้อนหลังแค่ 3 ชั่วโมงพอสำหรับกลยุทธ์
        self.state["price_history"] = [t for t in self.state["price_history"] if t[0] >= cutoff]
        self.save_state()

    def _price_at_offset(self, seconds_ago, tolerance_sec=300):
        history = self.state["price_history"]
        if not history:
            return None
        target = time.time() - seconds_ago
        closest = min(history, key=lambda t: abs(t[0] - target))
        if abs(closest[0] - target) > tolerance_sec:
            return None  # ข้อมูล ณ ช่วงเวลานั้นขาดหายไปเกินไป ถือว่าไม่พอ
        return closest[1]

    def _price_open_current_hour(self):
        history = self.state["price_history"]
        if not history:
            return None
        now = time.time()
        hour_start = now - (now % 3600)
        candidates = [p for ts, p in history if ts >= hour_start]
        return candidates[0] if candidates else history[0][1]

    def _has_enough_history(self):
        history = self.state["price_history"]
        if not history:
            return False
        return (time.time() - history[0][0]) >= 7200  # อย่างน้อย 2 ชั่วโมง

    def _trend_confidence(self, current_price, price_1h_ago, price_2h_ago, price_3h_ago):
        """ทิศทางหลักดูจาก 1ชม.ล่าสุด แล้วให้คะแนนความมั่นใจเพิ่มจาก 2ชม./3ชม.ก่อนหน้า (50% ต่อชม.)"""
        if current_price is None or price_1h_ago is None or current_price == price_1h_ago:
            return None, 0

        direction = "up" if current_price > price_1h_ago else "down"
        confidence = 0

        if price_1h_ago is not None and price_2h_ago is not None:
            hour2_same_direction = (direction == "up" and price_1h_ago > price_2h_ago) or \
                                    (direction == "down" and price_1h_ago < price_2h_ago)
            if hour2_same_direction:
                confidence += 50

        if price_2h_ago is not None and price_3h_ago is not None:
            hour3_same_direction = (direction == "up" and price_2h_ago > price_3h_ago) or \
                                    (direction == "down" and price_2h_ago < price_3h_ago)
            if hour3_same_direction:
                confidence += 50

        return direction, confidence

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
                    return buy_fee_pct * 2  # คูณ 2 เพื่อประมาณการค่าฟีแบบไป-กลับ (ซื้อ + ขาย)
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
                total_amount = float(asset.get("amount", 0.0))
                hold_amount = float(asset.get("hold", 0.0))
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
    def execute_market_order(self, side, value=None, quantity=None):
        if side == 0 and value is not None and value < self.MIN_ORDER_THB:
            logger.warning(f"ยกเลิกคำสั่งซื้อ: มูลค่า {value} THB ต่ำกว่าขั้นต่ำ {self.MIN_ORDER_THB} THB")
            return None

        path = "/api/v1/digital-asset/order/send"
        body = {"symbol": self.symbol, "timeInForce": 1, "side": side, "orderType": 1}
        if side == 0 and value is not None:
            body["value"] = round(value, 2)
        elif side == 1 and quantity is not None:
            body["quantity"] = quantity

        logger.info(f"กำลังส่งคำสั่งเทรด: {'ซื้อ' if side == 0 else 'ขาย'} -> {body}")
        return self.send_request("POST", path, body=body)

    def confirm_fill_price(self, order_id, max_attempts=5, delay_sec=1.5):
        """Poll ยืนยันราคาเฉลี่ยที่ match จริง แทนการ sleep คงที่"""
        path = "/api/v1/digital-asset/order/history/inquiry"
        body = {"symbol": self.symbol, "orderId": order_id}

        for attempt in range(1, max_attempts + 1):
            res = self.send_request("POST", path, body=body)
            if res and res.get("code") == "0000":
                orders = res.get("data", [])
                if orders:
                    avg_price = float(orders[0].get("avgPrice", 0.0))
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
        if self.state.get("trade_date") != today:
            thb_free, _, _ = self.get_free_balance()
            self.state["trade_date"] = today
            self.state["daily_start_balance"] = thb_free
            self.state["daily_realized_pnl"] = 0.0
            self.state["consecutive_losses"] = 0
            if self.state["status"] == "HALTED":
                logger.info("วันใหม่: ปลดล็อก Circuit Breaker อัตโนมัติ กลับสู่สถานะ IDLE")
                self.state["status"] = "IDLE"
            self.save_state()
            logger.info(f"เริ่มวันใหม่ ({today}) ทุนตั้งต้น: {thb_free:.2f} THB")

    def _register_trade_result(self, pnl_thb):
        self.state["daily_realized_pnl"] += pnl_thb
        if pnl_thb < 0:
            self.state["consecutive_losses"] += 1
        else:
            self.state["consecutive_losses"] = 0

        daily_loss_percent = 0.0
        if self.state["daily_start_balance"] > 0:
            daily_loss_percent = -self.state["daily_realized_pnl"] / self.state["daily_start_balance"] * 100

        halt_reason = None
        if daily_loss_percent >= self.max_daily_loss_percent:
            halt_reason = f"ขาดทุนสะสมวันนี้ {daily_loss_percent:.2f}% เกินเพดาน {self.max_daily_loss_percent}%"
        elif self.state["consecutive_losses"] >= self.max_consecutive_losses:
            halt_reason = f"ขาดทุนติดกัน {self.state['consecutive_losses']} ไม้ ถึงเพดาน {self.max_consecutive_losses} ไม้"

        if halt_reason:
            self.state["status"] = "HALTED"
            logger.error(f"🛑 CIRCUIT BREAKER ทำงาน: {halt_reason} — บอทหยุดเทรด "
                         f"(ปลดล็อกอัตโนมัติวันถัดไป หรือแก้ status บน Firebase ที่ path "
                         f"'{self.state_path}' ด้วยมือ)")
        self.save_state()

    # ==================== Strategy ====================
    def run_strategy(self, current_price):
        if self.state["status"] == "HALTED":
            return

        if self.state["status"] == "IDLE":
            logger.info(f"สถานะ IDLE กำลังวิเคราะห์ราคาปัจจุบัน: {current_price} THB")

            if not self._has_enough_history():
                logger.info("ข้อมูลราคาย้อนหลังยังไม่ครบ 2 ชั่วโมง รอสะสมข้อมูลต่อ")
                return

            price_1h_ago = self._price_at_offset(3600)
            price_2h_ago = self._price_at_offset(7200)
            price_3h_ago = self._price_at_offset(10800)
            price_open_1h = self._price_open_current_hour()

            if price_1h_ago is None or price_open_1h is None:
                logger.info("ข้อมูลราคาบางช่วงขาดหาย (gap) รอรอบถัดไป")
                return

            direction, confidence = self._trend_confidence(current_price, price_1h_ago, price_2h_ago, price_3h_ago)
            rule_current_hour_green = current_price > price_open_1h

            logger.info(f"ทิศทาง 1ชม: {direction or 'flat'} (ยืนยัน {confidence}%) | ชม.นี้เขียว: {rule_current_hour_green}")

            if direction == "down":
                logger.info(f"⚠️ เห็นสัญญาณขาลง (ยืนยัน {confidence}%) — ข้ามรอบนี้ ไม่เข้าซื้อ")
                return

            MIN_CONFIDENCE_TO_BUY = 50  # ต้องมี 2ชม. หรือ 3ชม. ยืนยันทิศทางเดียวกันอย่างน้อย 1 ใน 2 (ปรับตัวเลขนี้ได้เลย)
            if direction != "up" or confidence < MIN_CONFIDENCE_TO_BUY or not rule_current_hour_green:
                return

            thb_free, _, has_pending = self.get_free_balance()
            if has_pending:
                logger.info("ข้ามการซื้อ: มีออเดอร์ค้างอยู่ในระบบ")
                return
            if thb_free <= self.MIN_ORDER_THB:
                logger.info(f"เงินว่างไม่พอสำหรับซื้อขั้นต่ำ (คงเหลือ {thb_free:.2f} THB)")
                return

            rules = self.get_symbol_rules()
            # อัตราเงินที่ใช้เข้าซื้อต่อไม้ ปรับได้จากหน้าเว็บ (self.trade_size_percent) ค่าเริ่มต้น 95%
            buy_value = round(thb_free * (self.trade_size_percent / 100.0), 2)
            order_res = self.execute_market_order(side=0, value=buy_value)

            if not (order_res and order_res.get("code") == "0000"):
                logger.warning(f"ยิงออเดอร์ซื้อล้มเหลว: {order_res}")
                return

            order_id = order_res["data"]["orderId"]
            logger.info(f"✔ ส่งคำสั่งซื้อสำเร็จ Order ID: {order_id} กำลังยืนยันราคาจับคู่จริง...")

            avg_price = self.confirm_fill_price(order_id)
            if avg_price == 0.0:
                avg_price = current_price
                logger.warning("ยืนยันราคาจับคู่จริงไม่ได้ ใช้ราคาตลาด ณ ขณะนั้นแทน (ควรตรวจสอบ order ด้วยมือ)")

            self._check_slippage(current_price, avg_price, "ซื้อ")

            estimated_qty = self._floor_to_increment(buy_value / avg_price, rules["quantity_increment"])
            fee_pct = self.estimate_roundtrip_fee_percent()

            self.state.update({
                "status": "HOLDING",
                "entry_price": avg_price,
                "highest_price": avg_price,
                "quantity": estimated_qty,
                "roundtrip_fee_percent": fee_pct,
            })
            self.save_state()

            logger.info(f"🎉 ซื้อสำเร็จ ต้นทุนเฉลี่ย {avg_price} THB จำนวน {estimated_qty} "
                        f"(ค่าธรรมเนียม round-trip โดยประมาณ {fee_pct:.3f}%)")

        elif self.state["status"] == "HOLDING":
            entry_price = self.state["entry_price"]
            highest_price = self.state["highest_price"]
            qty = self.state["quantity"]
            fee_pct = self.state.get("roundtrip_fee_percent", self.DEFAULT_ROUNDTRIP_FEE_PERCENT)

            logger.info(f"สถานะ HOLDING ต้นทุน {entry_price} THB ราคาปัจจุบัน {current_price} THB")

            if current_price > highest_price:
                self.state["highest_price"] = current_price
                self.save_state()
                highest_price = current_price
                logger.info(f"🚀 จุดสูงสุดใหม่: {current_price} THB")

            trailing_threshold = highest_price * (1 - self.trailing_stop_percent / 100)
            stop_loss_threshold = entry_price * (1 - self.stop_loss_percent / 100)
            breakeven_price = entry_price * (1 + fee_pct / 100)  # breakeven จริงหลังหักค่าธรรมเนียม

            if current_price <= stop_loss_threshold:
                logger.warning("🚨 ถึงจุด Hard Stop Loss ขายทันทีเพื่อจำกัดความเสียหาย")
                self.sell_position(qty, current_price)
            elif current_price <= trailing_threshold and current_price > breakeven_price:
                logger.info(f"💰 ถึงจุด Trailing Stop และยังคุ้มค่าธรรมเนียม (breakeven {breakeven_price:.2f}) ขายล็อกกำไร")
                self.sell_position(qty, current_price)
            elif current_price <= trailing_threshold:
                logger.info(f"⏸ ราคาย่อถึง trailing threshold แต่ยังไม่คุ้มค่าธรรมเนียม "
                            f"(breakeven {breakeven_price:.2f}) ถือต่อ")

    def sell_position(self, qty, current_price=None):
        _, coin_free, has_pending = self.get_free_balance()
        if has_pending:
            logger.info("ข้ามการขาย: มีออเดอร์ค้างอยู่ในระบบ")
            return

        sell_qty = min(qty, coin_free)
        rules = self.get_symbol_rules()
        sell_qty = self._floor_to_increment(sell_qty, rules["quantity_increment"])

        if sell_qty <= 0:
            logger.warning(f"ไม่มียอดเหรียญ {self.target_currency} พร้อมขาย (คงเหลือจริง {coin_free})")
            return

        order_res = self.execute_market_order(side=1, quantity=sell_qty)
        if not (order_res and order_res.get("code") == "0000"):
            logger.warning(f"สั่งขายล้มเหลว: {order_res}")
            return

        order_id = order_res["data"]["orderId"]
        sell_avg_price = self.confirm_fill_price(order_id)

        if sell_avg_price > 0 and current_price is not None:
            self._check_slippage(current_price, sell_avg_price, "ขาย")

        entry_price = self.state["entry_price"]
        pnl_thb = (sell_avg_price - entry_price) * sell_qty if sell_avg_price > 0 else 0.0

        if sell_avg_price == 0.0:
            logger.warning("ยืนยันราคาขายจริงไม่ได้ — ข้าม PnL tracking รอบนี้ (ตรวจสอบ order ด้วยมือ)")

        logger.info(f"✔ ขายสำเร็จ ราคาเฉลี่ย {sell_avg_price or 'N/A'} PnL รอบนี้ {pnl_thb:.2f} THB")

        self.state.update({"status": "IDLE", "entry_price": 0.0, "highest_price": 0.0, "quantity": 0.0})
        self._register_trade_result(pnl_thb)

    # ==================== Main loop ====================
    def run_once(self):
        """รันหนึ่งรอบของ loop หลัก (ไม่ sleep) — เรียกจาก run() หรือจาก supervisor loop ใน __main__"""
        self._maybe_reset_daily_counters()

        if self.state["status"] == "HALTED":
            logger.warning("⛔ บอทอยู่ในสถานะ HALTED จาก Circuit Breaker — เฝ้าดูอย่างเดียว ไม่ส่งคำสั่งใหม่")
            return

        price = self.get_latest_price()
        if price is not None:
            self._record_price_tick(price)
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
<meta http-equiv="refresh" content="30">
<title>${symbol_display} Bot Dashboard</title>
<style>
  :root {
    --bg: #1E1C18; --card: #2A2822; --border: #3D3A32;
    --text: #EDE9DD; --text-soft: #9C9585;
    --accent: #E08A65; --accent-soft: #3D2C22;
    --green: #85B893; --green-soft: #24332A;
    --red: #E08277; --red-soft: #3A2523;
  }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; -webkit-font-smoothing:antialiased; min-height:100vh; display:flex; justify-content:center; padding:0 0 40px; }
  .app { width:100%; max-width:480px; }
  /* โค้ดใหม่ (ธีมมืดมืดกลมกลืน) */
  .topbar { position:sticky; top:0; z-index:10; display:flex; align-items:center; justify-content:space-between; padding:18px 20px; background:rgba(42,40,34,0.95); backdrop-filter:blur(8px); border-bottom:1px solid var(--border); }
  .brand { display:flex; align-items:center; gap:10px; }
  .spark { width:22px; height:22px; flex-shrink:0; }
  .spark path { fill: var(--accent); }
  .brand-title { font-size:15px; font-weight:700; letter-spacing:-0.01em; color:var(--text); }
  .brand-sub { font-size:12px; color:var(--text-soft); margin-top:1px; }
  .status-pill { display:inline-flex; align-items:center; gap:6px; padding:6px 12px; border-radius:999px; font-size:12px; font-weight:600; letter-spacing:0.02em; }
  .status-holding { background:var(--green-soft); color:var(--green); }
  .status-idle { background:#33312A; color:var(--text-soft); }
  .status-halted { background:var(--red-soft); color:var(--red); }
  .dot { width:7px; height:7px; border-radius:50%; background:currentColor; }
  .status-holding .dot { animation:pulse 1.8s ease-in-out infinite; }
  @keyframes pulse { 0%,100%{opacity:1;transform:scale(1);} 50%{opacity:.45;transform:scale(.8);} }
  @media (prefers-reduced-motion:reduce){ .status-holding .dot{animation:none;} }
  .hero { padding:32px 24px 20px; text-align:center; }
  .hero-label { font-size:13px; color:var(--text-soft); font-weight:500; }
  .hero-price { font-size:clamp(34px,9vw,44px); font-weight:800; letter-spacing:-0.02em; font-variant-numeric:tabular-nums; margin-top:4px; }
  .hero-decimal { font-size:.55em; color:var(--text-soft); font-weight:700; }
  .hero-delta { display:inline-block; margin-top:10px; font-size:13px; font-weight:600; padding:4px 10px; border-radius:999px; }
  .hero-delta.positive { color:var(--green); background:var(--green-soft); }
  .hero-delta.negative { color:var(--red); background:var(--red-soft); }
  .hero-sub { margin-top:10px; font-size:13px; color:var(--text-soft); }
  .banner { margin:0 20px 12px; padding:12px 14px; border-radius:12px; font-size:13px; font-weight:600; line-height:1.5; }
  .banner-danger { background:var(--red-soft); color:var(--red); }
  .banner-info { background:var(--accent-soft); color:var(--accent); }
  .banner-warning { background:#3D3420; color:#E8C468; }
  .progress-card { margin:0 20px 16px; background:var(--card); border:1px solid var(--border); border-radius:16px; padding:14px 16px; }
  .progress-label { font-size:12px; color:var(--text-soft); font-weight:600; }
  .progress-bar { margin-top:8px; height:8px; border-radius:999px; background:#33312A; overflow:hidden; }
  .progress-fill { height:100%; background:var(--accent); border-radius:999px; transition:width .3s ease; }
  .progress-sub { margin-top:6px; font-size:12px; color:var(--text-soft); }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:10px; padding:0 20px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:16px; padding:14px 16px; box-shadow:0 1px 2px rgba(43,40,34,.03); }
  .card-label { font-size:12px; color:var(--text-soft); font-weight:500; }
  .card-value { font-size:17px; font-weight:700; margin-top:4px; font-variant-numeric:tabular-nums; letter-spacing:-0.01em; }
  .card-value.positive { color:var(--green); }
  .card-value.negative { color:var(--red); }
  .card-value.accent { color:var(--accent); }
  .card-sub { font-size:11px; color:var(--text-soft); margin-top:2px; }
  .control { margin:20px 20px 0; background:var(--card); border:1px solid var(--border); border-radius:16px; padding:16px; }
  .control-title { font-size:13px; font-weight:700; margin-bottom:12px; }
  .control-row { display:flex; align-items:flex-end; gap:10px; margin-bottom:14px; }
  .control-row:last-child { margin-bottom:0; }
  .control-info { flex:1; min-width:0; }
  .control-label { font-size:12px; font-weight:600; color:var(--text); }
  .control-sub { font-size:11px; color:var(--text-soft); margin-top:2px; }
  .symbol-input { width:100%; margin-top:8px; padding:9px 10px; border:1px solid var(--border); border-radius:10px; font-size:14px; font-weight:600; letter-spacing:.02em; text-transform:uppercase; background:var(--bg); color:var(--text); }
  .symbol-input:focus { outline:2px solid var(--accent); outline-offset:1px; }
  .quick-picks { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }
  .pick { font-size:11px; font-weight:600; color:var(--text-soft); background:var(--bg); border:1px solid var(--border); border-radius:999px; padding:4px 9px; cursor:pointer; }
  .password-input { width:100%; margin-top:8px; padding:9px 10px; border:1px solid var(--border); border-radius:10px; font-size:13px; background:var(--bg); color:var(--text); }
  .btn { border:none; border-radius:10px; padding:10px 16px; font-size:13px; font-weight:700; cursor:pointer; white-space:nowrap; }
  .btn-accent { background:var(--accent); color:#fff; }
  .btn-neutral { background:#33312A; color:var(--text); }
  .btn-danger { background:var(--red); color:#fff; }
  footer { display:flex; align-items:center; justify-content:center; gap:8px; margin-top:20px; font-size:12px; color:var(--text-soft); }
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
          <div class="brand-title">InnovestX Bot</div>
          <div class="brand-sub">${symbol_display} · Price Action 3H</div>
        </div>
      </div>
      <div class="status-pill ${status_class}"><span class="dot"></span> ${status_label}</div>
    </header>
    <section class="hero">
      <div class="hero-label">ราคาปัจจุบัน (${symbol_display})</div>
      <div class="hero-price">${hero_price}</div>
      ${hero_delta_html}
      ${freshness_html}
    </section>
    ${banners_html}
    ${progress_html}
    <div class="grid">
      ${cards_html}
    </div>
    <section class="control">
      <div class="control-title">ควบคุมบอท</div>
      ${unlock_button_html}
      <form class="control-row" method="POST" action="/control/pause">
        <div class="control-info">
          <div class="control-label">การเทรดอัตโนมัติ</div>
          <div class="control-sub">${pause_sub}</div>
        </div>
        <button type="submit" class="btn ${pause_btn_class}">${pause_btn_label}</button>
      </form>
      <form class="control-row" method="POST" action="/control/symbol" style="flex-direction:column; align-items:stretch;">
        <div class="control-info">
          <div class="control-label">เหรียญที่เทรด (กำลังรัน: ${running_symbol})</div>
          <input class="symbol-input" type="text" name="symbol" value="${symbol_input_value}" autocapitalize="characters" autocomplete="off">
          <div class="quick-picks">
            <span class="pick" onclick="document.querySelector('.symbol-input').value='BTCTHB'">BTCTHB</span>
            <span class="pick" onclick="document.querySelector('.symbol-input').value='ETHTHB'">ETHTHB</span>
            <span class="pick" onclick="document.querySelector('.symbol-input').value='XRPTHB'">XRPTHB</span>
            <span class="pick" onclick="document.querySelector('.symbol-input').value='USDTTHB'">USDTTHB</span>
          </div>
          <div class="control-sub" style="margin-top:6px;">พิมพ์คู่เหรียญตามที่ InnovestX รองรับ (ตัวพิมพ์ใหญ่) เปลี่ยนได้จริงเมื่อบอทไม่ได้ถือโพซิชันอยู่เท่านั้น</div>
        </div>
        ${password_field_html}
        <button type="submit" class="btn btn-accent" style="margin-top:10px;">เปลี่ยนเหรียญ</button>
      </form>
      <form class="control-row" method="POST" action="/control/trade_size" style="flex-direction:column; align-items:stretch;">
        <div class="control-info">
          <div class="control-label">อัตราเงินที่ใช้เข้าซื้อต่อไม้ (กำลังตั้ง: ${trade_size_percent}% ของเงินว่าง)</div>
          <input class="symbol-input" style="text-transform:none;" type="number" step="1" min="1" max="100" name="trade_size_percent" value="${trade_size_percent}">
          <div class="control-sub" style="margin-top:6px;">เช่น ตั้ง 50 = ใช้เงินบาทว่างครึ่งหนึ่งเข้าซื้อทุกครั้งที่มีสัญญาณ ที่เหลือจะไม่ถูกแตะ</div>
        </div>
        ${password_field_html}
        <button type="submit" class="btn btn-accent" style="margin-top:10px;">บันทึกค่า</button>
      </form>
      <form class="control-row" method="POST" action="/control/max_losses" style="flex-direction:column; align-items:stretch;">
        <div class="control-info">
          <div class="control-label">ขาดทุนติดกันกี่ไม้ถึงหยุด (กำลังตั้ง: ${max_consecutive_losses} ไม้)</div>
          <input class="symbol-input" style="text-transform:none;" type="number" step="1" min="1" max="20" name="max_consecutive_losses" value="${max_consecutive_losses}">
          <div class="control-sub" style="margin-top:6px;">ถ้าขาดทุนติดต่อกันครบจำนวนนี้ บอทจะหยุดเทรด (HALTED) ทันที</div>
        </div>
        ${password_field_html}
        <button type="submit" class="btn btn-accent" style="margin-top:10px;">บันทึกค่า</button>
      </form>
      <form class="control-row" method="POST" action="/control/risk" style="flex-direction:column; align-items:stretch;">
        <div class="control-info">
          <div class="control-label">ขาดทุนสูงสุดที่ยอมรับต่อวัน (กำลังตั้ง: ${max_daily_loss_percent}%)</div>
          <input class="symbol-input" style="text-transform:none;" type="number" step="0.1" min="0.1" max="100" name="max_daily_loss_percent" value="${max_daily_loss_percent}">
          <div class="control-sub" style="margin-top:6px;">ถ้าขาดทุนสะสมวันนี้ถึง % นี้ บอทจะหยุดเทรดอัตโนมัติ (HALTED) จนกว่าจะข้ามวันใหม่ หรือปลดล็อกเอง</div>
        </div>
        ${password_field_html}
        <button type="submit" class="btn btn-accent" style="margin-top:10px;">บันทึกค่า</button>
      </form>
    </section>
    <footer>
      <span>อัปเดตล่าสุด ${last_updated}</span>
      <span class="sep">·</span>
      <a href="#" class="refresh-link" onclick="location.reload();return false;">รีเฟรชตอนนี้</a>
    </footer>
  </div>
</body>
</html>""")


def _fmt_thb(value):
    try:
        return f"{value:,.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _render_card(label, value, sub="", value_class=""):
    sub_html = f'<div class="card-sub">{sub}</div>' if sub else ""
    return f'<div class="card"><div class="card-label">{label}</div><div class="card-value {value_class}">{value}</div>{sub_html}</div>'


def render_dashboard(running_symbol, state, control):
    state = state or {}
    status = state.get("status", "IDLE")
    entry_price = float(state.get("entry_price", 0.0) or 0.0)
    highest_price = float(state.get("highest_price", 0.0) or 0.0)
    quantity = float(state.get("quantity", 0.0) or 0.0)
    fee_pct = float(state.get("roundtrip_fee_percent", InnovestXTradingBot.DEFAULT_ROUNDTRIP_FEE_PERCENT) or 0.0)
    daily_pnl = float(state.get("daily_realized_pnl", 0.0) or 0.0)
    daily_start = float(state.get("daily_start_balance", 0.0) or 0.0)
    consecutive_losses = int(state.get("consecutive_losses", 0) or 0)
    price_history = state.get("price_history", []) or []
    current_price = price_history[-1][1] if price_history else None
    last_price_ts = price_history[-1][0] if price_history else None

    now = time.time()
    price_age_sec = (now - last_price_ts) if last_price_ts is not None else None
    PRICE_STALE_THRESHOLD_SEC = 150  # เกิน ~2.5 เท่าของ poll interval (60วิ) ถือว่าเก่าเกินไป น่าสงสัย

    active_max_consecutive_losses = control.get("max_consecutive_losses", InnovestXTradingBot.MAX_CONSECUTIVE_LOSSES)

    # ---- status pill ----
    status_map = {
        "HOLDING": ("status-holding", "HOLDING"),
        "HALTED": ("status-halted", "HALTED"),
    }
    status_class, status_label = status_map.get(status, ("status-idle", "IDLE"))

    # ---- hero ----
    if current_price is not None:
        whole, _, dec = f"{current_price:,.2f}".partition(".")
        hero_price = f"฿{whole}<span class=\"hero-decimal\">.{dec}</span>"
    else:
        hero_price = "฿---"

    # ---- ความสดของราคา (แยกจากเวลา render หน้า) ----
    freshness_html = ""
    if price_age_sec is not None:
        age_min = int(price_age_sec // 60)
        age_sec = int(price_age_sec % 60)
        age_text = f"{age_min} นาที {age_sec} วิ" if age_min > 0 else f"{age_sec} วิ"
        if price_age_sec > PRICE_STALE_THRESHOLD_SEC:
            freshness_html = f'<div class="hero-delta negative">⚠️ ราคาอาจไม่สด — ดึงมาเมื่อ {age_text} ที่แล้ว</div>'
        else:
            freshness_html = f'<div class="hero-sub">อัปเดตราคาเมื่อ {age_text} ที่แล้ว</div>'

    hero_delta_html = ""
    if status == "HOLDING" and entry_price > 0 and current_price is not None:
        delta_pct = (current_price - entry_price) / entry_price * 100
        cls = "positive" if delta_pct >= 0 else "negative"
        arrow = "▲" if delta_pct >= 0 else "▼"
        hero_delta_html = f'<div class="hero-delta {cls}">{arrow} {delta_pct:+.2f}% จากต้นทุน</div>'
    elif status == "IDLE":
        hero_delta_html = '<div class="hero-sub">รอสัญญาณเข้าซื้อ...</div>'

    # ---- banners ----
    banners = []
    if price_age_sec is not None and price_age_sec > PRICE_STALE_THRESHOLD_SEC:
        age_min = int(price_age_sec // 60)
        banners.append(f'<div class="banner banner-danger">⚠️ ราคาที่แสดงอาจไม่ใช่ราคาสด (เก่าไปแล้ว {age_min}+ นาที) — ตรวจสอบ log บน Render ว่าดึงราคาล้มเหลวซ้ำๆ หรือไม่</div>')

    reason = ""
    if status == "HALTED":
        active_threshold = control.get("max_daily_loss_percent", InnovestXTradingBot.MAX_DAILY_LOSS_PERCENT)
        daily_loss_percent = -daily_pnl / daily_start * 100 if daily_start > 0 else 0.0
        if daily_loss_percent >= active_threshold:
            reason = f"ขาดทุนสะสมวันนี้เกิน {active_threshold:.1f}%"
        elif consecutive_losses >= active_max_consecutive_losses:
            reason = f"ขาดทุนติดกัน {consecutive_losses} ไม้ ครบเพดาน {active_max_consecutive_losses} ไม้"
        else:
            reason = "Circuit breaker ทำงาน (ดูรายละเอียดใน log)"
        banners.append(f'<div class="banner banner-danger">🛑 บอทหยุดเทรดชั่วคราว — {reason} (ปลดล็อกอัตโนมัติวันถัดไป หรือกดปลดล็อกเองด้านล่าง)</div>')

    if control.get("paused"):
        banners.append('<div class="banner banner-info">⏸ บอทหยุดเทรดชั่วคราว (สั่งจากหน้านี้) — จะไม่เปิดออเดอร์ใหม่จนกว่าจะกด "เริ่มเทรดต่อ"</div>')

    if control.get("active_symbol") != running_symbol:
        banners.append(f'<div class="banner banner-info">🔄 คำขอเปลี่ยนเหรียญเป็น {control.get("active_symbol")} กำลังรออยู่ — จะเปลี่ยนทันทีหลังขายโพซิชัน {running_symbol} เสร็จ</div>')

    if not os.environ.get("DASHBOARD_PASSWORD"):
        banners.append('<div class="banner banner-warning">⚠️ หน้านี้ยังไม่มีรหัสผ่านป้องกัน ใครมีลิงก์นี้ก็สั่งหยุด/เปลี่ยนเหรียญได้ — แนะนำตั้งค่า DASHBOARD_PASSWORD ใน Environment Variables ของ Render</div>')

    banners_html = "".join(banners)

    # ---- progress bar (สะสมข้อมูล 2 ชม.) ----
    progress_html = ""
    if status == "IDLE":
        if price_history:
            elapsed = max(0.0, now - price_history[0][0])
        else:
            elapsed = 0.0
        if elapsed < 7200:
            pct = min(100, elapsed / 7200 * 100)
            minutes_done = int(elapsed // 60)
            minutes_left = max(0, 120 - minutes_done)
            sub = f"{minutes_done} / 120 นาที · เหลืออีกประมาณ {minutes_left} นาที" if price_history else \
                "ยังไม่มีข้อมูลราคาเลย รอรอบแรกของบอท (ทุก 60 วินาที)"
            progress_html = (
                '<section class="progress-card">'
                '<div class="progress-label">กำลังสะสมข้อมูลราคา (ต้องครบ 2 ชั่วโมงก่อนเริ่มวิเคราะห์สัญญาณซื้อ)</div>'
                f'<div class="progress-bar"><div class="progress-fill" style="width:{pct:.0f}%"></div></div>'
                f'<div class="progress-sub">{sub}</div>'
                '</section>'
            )

    # ---- cards ----
    cards = []
    if status == "HOLDING":
        unrealized = quantity * (current_price - entry_price) if current_price is not None else 0.0
        unrealized_pct = (current_price - entry_price) / entry_price * 100 if entry_price > 0 and current_price is not None else 0.0
        cards.append(_render_card("กำไร/ขาดทุน (ยังไม่ขาย)", f"{'+' if unrealized >= 0 else ''}{_fmt_thb(unrealized)} ฿",
                                   sub=f"{unrealized_pct:+.2f}%", value_class="positive" if unrealized >= 0 else "negative"))
        cards.append(_render_card("ต้นทุนเฉลี่ย", f"{_fmt_thb(entry_price)} ฿"))
        cards.append(_render_card("จุดสูงสุดตั้งแต่ถือ", f"{_fmt_thb(highest_price)} ฿"))
        cards.append(_render_card("ค่าธรรมเนียม (ประมาณ)", f"{fee_pct:.2f}%", value_class="accent"))
        cards.append(_render_card("กำไรวันนี้ (รับรู้แล้ว)", f"{'+' if daily_pnl >= 0 else ''}{_fmt_thb(daily_pnl)} ฿",
                                   value_class="positive" if daily_pnl >= 0 else "negative"))
        cards.append(_render_card("ขาดทุนติดกัน", f"{consecutive_losses} / {active_max_consecutive_losses} ไม้"))
    else:
        cards.append(_render_card("กำไรวันนี้ (รับรู้แล้ว)", f"{'+' if daily_pnl >= 0 else ''}{_fmt_thb(daily_pnl)} ฿",
                                   value_class="positive" if daily_pnl >= 0 else "negative"))
        cards.append(_render_card("ทุนเริ่มต้นวันนี้", f"{_fmt_thb(daily_start)} ฿"))
        cards.append(_render_card("ขาดทุนติดกัน", f"{consecutive_losses} / {active_max_consecutive_losses} ไม้"))
        cards.append(_render_card("ค่าธรรมเนียม (ประมาณล่าสุด)", f"{fee_pct:.2f}%", value_class="accent"))

    cards_html = "".join(cards)

    # ---- control panel state ----
    if control.get("paused"):
        pause_sub = "ตอนนี้: หยุดอยู่ (ไม่เปิดออเดอร์ใหม่)"
        pause_btn_class = "btn-accent"
        pause_btn_label = "▶ เริ่มเทรดต่อ"
    else:
        pause_sub = "ตอนนี้: กำลังทำงานปกติ"
        pause_btn_class = "btn-neutral"
        pause_btn_label = "⏸ หยุดชั่วคราว"

    password_field_html = ""
    if os.environ.get("DASHBOARD_PASSWORD"):
        password_field_html = '<input class="password-input" type="password" name="password" placeholder="รหัสผ่านหน้าควบคุม" style="margin-top:8px;">'

    last_updated = datetime.now().strftime("%H:%M:%S")

    unlock_button_html = ""
    if status == "HALTED":
        unlock_button_html = f'''<form class="control-row" method="POST" action="/control/unlock">
      <div class="control-info">
        <div class="control-label">บอทถูกล็อกอยู่ (HALTED)</div>
        <div class="control-sub">{reason}</div>
      </div>
      {password_field_html}
      <button type="submit" class="btn btn-danger">🔓 ปลดล็อกตอนนี้</button>
    </form>'''

    return DASHBOARD_TEMPLATE.safe_substitute(
        symbol_display=f"{running_symbol[:-3]}/{running_symbol[-3:]}" if running_symbol.endswith("THB") else running_symbol,
        status_class=status_class,
        status_label=status_label,
        hero_price=hero_price,
        hero_delta_html=hero_delta_html,
        freshness_html=freshness_html,
        banners_html=banners_html,
        progress_html=progress_html,
        cards_html=cards_html,
        unlock_button_html=unlock_button_html,
        pause_sub=pause_sub,
        pause_btn_class=pause_btn_class,
        pause_btn_label=pause_btn_label,
        running_symbol=running_symbol,
        symbol_input_value=control.get("active_symbol", running_symbol),
        max_daily_loss_percent=control.get("max_daily_loss_percent", InnovestXTradingBot.MAX_DAILY_LOSS_PERCENT),
        trade_size_percent=control.get("trade_size_percent", InnovestXTradingBot.DEFAULT_TRADE_SIZE_PERCENT),
        max_consecutive_losses=active_max_consecutive_losses,
        password_field_html=password_field_html,
        last_updated=last_updated,
    )


# ==================== Web Service (หน้าเว็บสถานะ + ควบคุมบอท สำหรับ Render) ====================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # กัน log ของ http.server เองไปปนกับ log ของบอท (คำขอ GET ทุกครั้งไม่จำเป็นต้องขึ้น log)

    def do_GET(self):
        # 1. เพิ่มส่วนนี้: ถ้า UptimeRobot ยิงมาที่ /health ให้ตอบ OK สั้นๆ กลับไปทันที
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        # 2. ส่วนเดิม: สำหรับการเข้าดูหน้า Dashboard ผ่านเบราว์เซอร์ปกติ
        try:
            running_symbol = RUNNING_SYMBOL["value"] or DEFAULT_SYMBOL
            state = db.reference(f"bots/{running_symbol}/state").get() or {}
            control = load_control()
            html = render_dashboard(running_symbol, state, control)
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

            if self.path == "/control/pause":
                control = load_control()
                control["paused"] = not control.get("paused", False)
                save_control(control)
                logger.info(f"[เว็บควบคุม] ตั้งค่า paused={control['paused']}")

            elif self.path == "/control/symbol":
                requested = fields.get("symbol", [""])[0].strip().upper()
                if re.match(r"^[A-Z0-9]{2,20}$", requested):
                    control = load_control()
                    control["active_symbol"] = requested
                    save_control(control)
                    logger.info(f"[เว็บควบคุม] คำขอเปลี่ยนเหรียญเป็น {requested}")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธคำขอเปลี่ยนเหรียญ: รูปแบบไม่ถูกต้อง ({requested})")

            elif self.path == "/control/trade_size":
                raw_value = fields.get("trade_size_percent", [""])[0].strip()
                try:
                    value = float(raw_value)
                except ValueError:
                    value = None
                if value is not None and 1 <= value <= 100:
                    control = load_control()
                    control["trade_size_percent"] = value
                    save_control(control)
                    logger.info(f"[เว็บควบคุม] ตั้งค่าอัตราเงินที่ใช้เข้าซื้อต่อไม้เป็น {value}%")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธค่าอัตราเงินที่ใช้เข้าซื้อ: '{raw_value}' (ต้องอยู่ระหว่าง 1-100)")

            elif self.path == "/control/max_losses":
                raw_value = fields.get("max_consecutive_losses", [""])[0].strip()
                try:
                    value = int(float(raw_value))
                except ValueError:
                    value = None
                if value is not None and 1 <= value <= 20:
                    control = load_control()
                    control["max_consecutive_losses"] = value
                    save_control(control)
                    logger.info(f"[เว็บควบคุม] ตั้งค่าจำนวนไม้ขาดทุนติดกันก่อนหยุดเป็น {value} ไม้")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธค่าจำนวนไม้ขาดทุนติดกัน: '{raw_value}' (ต้องอยู่ระหว่าง 1-20)")

            elif self.path == "/control/risk":
                raw_value = fields.get("max_daily_loss_percent", [""])[0].strip()
                try:
                    value = float(raw_value)
                except ValueError:
                    value = None
                if value is not None and 0.1 <= value <= 100:
                    control = load_control()
                    control["max_daily_loss_percent"] = value
                    save_control(control)
                    logger.info(f"[เว็บควบคุม] ตั้งค่าขาดทุนสูงสุดต่อวันเป็น {value}%")
                else:
                    logger.warning(f"[เว็บควบคุม] ปฏิเสธค่าขาดทุนสูงสุดต่อวัน: '{raw_value}' (ต้องอยู่ระหว่าง 0.1-100)")

            elif self.path == "/control/unlock":
                control = load_control()
                control["unlock_requested"] = True
                save_control(control)
                logger.info("[เว็บควบคุม] ได้รับคำขอปลดล็อกบอท (HALTED -> IDLE) — จะมีผลในรอบ loop ถัดไป (ภายใน 60 วิ)")

            self.send_response(303)
            self.send_header("Location", "/")
            self.end_headers()
        except Exception as e:
            logger.error(f"Dashboard control error: {e}")
            self.send_response(500)
            self.end_headers()


def start_dummy_health_check_server():
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    server.serve_forever()


if __name__ == "__main__":
    api_key = os.environ.get("INVX_API_KEY")
    api_secret = os.environ.get("INVX_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit(
            "กรุณาตั้งค่า environment variable INVX_API_KEY และ INVX_API_SECRET ก่อนรัน "
            "(ห้าม hardcode API key/secret ลงในไฟล์โค้ดโดยเด็ดขาด)"
        )

    # เปิด Server จำลองหลอก Render ให้ตรวจเจอ Port + เสิร์ฟแดชบอร์ด
    health_thread = threading.Thread(target=start_dummy_health_check_server, daemon=True)
    health_thread.start()

    POLL_INTERVAL_SEC = 15

    control = load_control()
    save_control(control)  # เขียนกลับให้ path บน Firebase มีค่าเริ่มต้นแน่นอนตั้งแต่แรก

    current_symbol = control["active_symbol"]
    RUNNING_SYMBOL["value"] = current_symbol
    bot = InnovestXTradingBot(api_key=api_key, api_secret=api_secret, symbol=current_symbol)
    bot.reconcile_state_on_startup()

    logger.info(f"เริ่มบอทเทรด {current_symbol} (poll ทุก {POLL_INTERVAL_SEC} วิ) — กด Ctrl+C เพื่อหยุดอย่างปลอดภัย")

    stop_all = False
    while not stop_all:
        control = load_control()

        # --- เช็คคำขอเปลี่ยนเหรียญจากหน้าเว็บ ---
        if control["active_symbol"] != current_symbol:
            if bot.state.get("quantity", 0) <= 0:
                logger.info(f"🔄 เปลี่ยนเหรียญเทรดจาก {current_symbol} เป็น {control['active_symbol']} (สั่งจากหน้าเว็บ)")
                current_symbol = control["active_symbol"]
                bot = InnovestXTradingBot(api_key=api_key, api_secret=api_secret, symbol=current_symbol, stop_loss_percent=3.0)
                bot.reconcile_state_on_startup()
                RUNNING_SYMBOL["value"] = current_symbol
            else:
                logger.info(f"⏳ มีคำขอเปลี่ยนเป็น {control['active_symbol']} แต่ตอนนี้ถือ {current_symbol} อยู่ — รอขายก่อน")

        # --- ซิงค์ค่าเพดานขาดทุนต่อวันจากหน้าเว็บ (มีผลทันที ไม่ต้อง restart) ---
        if bot.max_daily_loss_percent != control["max_daily_loss_percent"]:
            logger.info(f"🔧 ปรับเพดานขาดทุนต่อวันจาก {bot.max_daily_loss_percent}% เป็น {control['max_daily_loss_percent']}% (สั่งจากหน้าเว็บ)")
            bot.max_daily_loss_percent = control["max_daily_loss_percent"]

        # --- ซิงค์อัตราเงินที่ใช้เข้าซื้อต่อไม้จากหน้าเว็บ (มีผลทันที ไม่ต้อง restart) ---
        if bot.trade_size_percent != control["trade_size_percent"]:
            logger.info(f"🔧 ปรับอัตราเงินที่ใช้เข้าซื้อจาก {bot.trade_size_percent}% เป็น {control['trade_size_percent']}% (สั่งจากหน้าเว็บ)")
            bot.trade_size_percent = control["trade_size_percent"]

        # --- ซิงค์จำนวนไม้ขาดทุนติดกันก่อนหยุดจากหน้าเว็บ (มีผลทันที ไม่ต้อง restart) ---
        if bot.max_consecutive_losses != control["max_consecutive_losses"]:
            logger.info(f"🔧 ปรับจำนวนไม้ขาดทุนติดกันก่อนหยุดจาก {bot.max_consecutive_losses} เป็น {control['max_consecutive_losses']} ไม้ (สั่งจากหน้าเว็บ)")
            bot.max_consecutive_losses = control["max_consecutive_losses"]

        # --- เช็คคำขอปลดล็อก (HALTED -> IDLE) จากหน้าเว็บ ---
        # แก้ค่าตรงที่ bot.state ในหน่วยความจำเลย (ไม่ใช่แค่บน Firebase) เพราะบอทที่รันอยู่
        # จะไม่อ่าน state ซ้ำระหว่างรัน ถ้าไปแก้ Firebase ตรงๆ บอทจะเขียนทับกลับเป็น HALTED เหมือนเดิม
        if control["unlock_requested"]:
            if bot.state.get("status") == "HALTED":
                logger.info("🔓 ปลดล็อกบอทตามคำขอจากหน้าเว็บ: HALTED -> IDLE (รีเซ็ตขาดทุนติดกันเป็น 0)")
                bot.state["status"] = "IDLE"
                bot.state["consecutive_losses"] = 0
                bot.save_state()
            control["unlock_requested"] = False
            save_control(control)

        # --- เช็คสถานะหยุดชั่วคราวจากหน้าเว็บ ---
        if control["paused"]:
            logger.info("⏸ บอทหยุดชั่วคราว (สั่งโดยผู้ใช้ผ่านหน้าเว็บ) — ข้ามการเทรดรอบนี้")
        else:
            try:
                bot.run_once()
            except Exception:
                logger.exception("เกิดข้อผิดพลาดไม่คาดคิดใน main loop — บอทจะพยายามทำงานต่อในรอบถัดไป")

        for _ in range(POLL_INTERVAL_SEC):
            if bot._stop_requested:
                stop_all = True
                break
            time.sleep(1)

    logger.info("บอทหยุดทำงานเรียบร้อย (state ถูกบันทึกแล้ว)")