"""
InnovestX Automated Trading Bot — Price Action 3H Strategy (v2, hardened)

แก้ไขจากโค้ดต้นฉบับตามที่รีวิวไว้:
  1. เก็บ price_history จริง (เดิมไม่มีโค้ดสะสมราคาเลย ทำให้เงื่อนไขซื้อไม่มีวันครบ)
  2. คำนวณค่าธรรมเนียม (fee) เข้าไปในจุดตัดสินใจขาย ไม่ให้ trailing stop ขายทั้งที่ขาดทุนสุทธิ
  3. จัดการ error code เฉพาะทาง (4005 / 4011 / 4019 / 4042) + retry/backoff เมื่อเจอ network error, 429, 5xx
  4. ยืนยันราคาจับคู่จริงด้วย retry-poll แทนการ sleep(3) แบบตายตัว
  5. Circuit breaker: หยุดเทรดอัตโนมัติเมื่อขาดทุนสะสมรายวันเกินเพดาน หรือขาดทุนติดกันหลายไม้
  6. Log ทุก request-uid ลงไฟล์ (bot_transactions.log) ตาม Architect's Tip ในคู่มือ
  7. ใช้ Decimal ปัดจำนวนเหรียญตาม quantityIncrement จริง (กันปัญหาความแม่นยำของ float)
  8. เช็ค minimum notional ก่อนส่งคำสั่งซื้อ
  9. Graceful shutdown (Ctrl+C / SIGTERM) จะบันทึก state ก่อนปิดเสมอ
  10. requests มี timeout ทุกครั้ง กัน bot ค้างถ้า network แขวน

⚠️ สมมติฐานที่ต้องตรวจสอบกับเอกสาร API ฉบับเต็มก่อนรันจริง (ผมไม่มีข้อมูลยืนยัน 100%):
  - FEE_ESTIMATE_PATH ด้านล่าง: คู่มือพูดถึงฟีเจอร์ "Get Estimate Fee" แต่ไม่ได้ให้ path
    ตรงๆ ผมตั้งตาม naming convention ของ endpoint อื่นๆ ที่คุณมีอยู่แล้ว — ต้องยืนยันก่อนใช้จริง
  - get_latest_price(): คู่มือเรียก ticker ว่า "Subscribe" และบอกว่า "ส่งข้อมูลทุก 1 นาที"
    ซึ่งฟังดูเหมือน WebSocket push มากกว่า REST request/response ปกติ ถ้า endpoint นี้ใช้ไม่ได้
    แบบ REST จริง ให้เปลี่ยนมาใช้ WebSocket client แทนการ poll ในฟังก์ชันนี้
"""

import time
import uuid
import hmac
import hashlib
import json
import os
import signal
import logging
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
# Render (และ host อื่นๆ ส่วนใหญ่) ลบไฟล์ในดิสก์ทิ้งทุกครั้งที่ deploy ใหม่/restart/
# free-tier sleep แล้วตื่น — ของเดิมเก็บ state (status, entry_price, quantity, ตัวนับ
# circuit breaker ฯลฯ) ไว้ในไฟล์ bot_state.json บนดิสก์เฉยๆ พอ restart ทีนึงข้อมูลหายหมด
# บอทจะคิดว่าตัวเองว่าง (IDLE) ทั้งที่จริงถือเหรียญอยู่ หรือลืมไปแล้วว่าวันนี้ HALTED เพราะ
# ขาดทุนเกินเพดานไปแล้ว → ย้ายไปเก็บบน Firebase Realtime Database แทน ข้อมูลอยู่ถาวร
# ไม่หายตอน restart
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


class InnovestXTradingBot:
    # ---- ค่าคงที่ที่ปรับได้ ----
    MAX_DAILY_LOSS_PERCENT = 5.0          # หยุดเทรดถ้าขาดทุนสะสมวันนี้เกิน % ของทุนเริ่มวัน
    MAX_CONSECUTIVE_LOSSES = 3            # หยุดเทรดถ้าขาดทุนติดกันกี่ไม้
    DEFAULT_ROUNDTRIP_FEE_PERCENT = 0.50  # fallback ถ้าดึงค่าธรรมเนียมจริงไม่ได้ (ปรับให้ตรงจริง!)
    MIN_ORDER_THB = 100.0
    MAX_ACCEPTABLE_SLIPPAGE_PERCENT = 1.0   # ถ้าราคาจริงเพี้ยนจากที่คาดเกิน % นี้ จะแจ้งเตือน
    REQUEST_TIMEOUT_SEC = 10
    MAX_RETRIES = 3
    FEE_ESTIMATE_PATH = "/api/v1/digital-asset/order/fee/inquiry"

    def __init__(self, api_key, api_secret, symbol="BTCTHB", base_currency="THB",
                 target_currency="BTC", trailing_stop_percent=2.0, stop_loss_percent=1.5):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.base_currency = base_currency
        self.target_currency = target_currency
        self.host = "api.innovestxonline.com"
        self.base_url = f"https://{self.host}"
        # path ที่เก็บ state บน Firebase Realtime Database — แยกตาม symbol กันชนกันถ้า
        # รันบอทหลายตัว/หลายเหรียญพร้อมกันในอนาคต (เดิมใช้ไฟล์ bot_state.json บนดิสก์
        # ซึ่งหายทุกครั้งที่ Render restart — ดูคอมเมนต์ init_firebase() ด้านบน)
        self.state_path = f"bots/{symbol}/state"

        self.trailing_stop_percent = trailing_stop_percent
        self.stop_loss_percent = stop_loss_percent

        self._stop_requested = False
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        self.state = self.load_state()

    # ==================== State management ====================

    def load_state(self):
        default_state = {
            "status": "IDLE",            # IDLE / HOLDING / HALTED
            "entry_price": 0.0,
            "highest_price": 0.0,
            "quantity": 0.0,
            "roundtrip_fee_percent": self.DEFAULT_ROUNDTRIP_FEE_PERCENT,
            "price_history": [],         # [[timestamp_sec, price], ...] เก็บย้อนหลัง 3 ชม.
            "trade_date": None,          # "YYYY-MM-DD" (UTC) สำหรับรีเซ็ต circuit breaker รายวัน
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
        """เช็คว่า state ใน state file ตรงกับยอดจริงในพอร์ตหรือไม่ ก่อนเริ่ม loop"""
        logger.info("กำลังตรวจสอบสถานะกับยอดจริงในพอร์ต (Reconcile)...")
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
            logger.info(f"Reconcile ผ่าน: state={self.state['status']} ตรงกับพอร์ตจริง ({coin_free} {self.target_currency})")

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
        path = "/api/v1/digital-asset/ticker/subscribe"
        body = {"symbol": self.symbol}
        res = self.send_request("POST", path, body=body)
        if res and res.get("code") == "0000":
            data = res.get("data")
            if isinstance(data, list) and data:
                data = data[0]
            if isinstance(data, dict) and "close" in data:
                try:
                    return float(data["close"])
                except (TypeError, ValueError):
                    pass
        logger.warning("ดึงราคาปัจจุบันล้มเหลว")
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
        # 1. ปรับปรุงพารามิเตอร์ขาส่ง (Request) ตามคู่มือ InnovestX: บังคับส่ง symbol, amount, price, side (ไม่มี orderType)
        dummy_amount = 0.01      # จำนวนเหรียญจำลองที่ใช้ทดสอบคำนวณ
        dummy_price = 100000.0   # ราคาสมมติต่อหน่วย
        total_value = dummy_amount * dummy_price  # มูลค่ารวมจำลอง (1,000 THB)
        
        body = {
            "symbol": self.symbol,
            "amount": dummy_amount,
            "price": dummy_price,
            "side": 0  # 0 = Buy (ฝั่งซื้อ)
        }
        
        res = self.send_request("POST", self.FEE_ESTIMATE_PATH, body=body)
        
        if res and res.get("code") == "0000":
            try:
                # 2. ปรับปรุงขาตอบกลับ (Response) ตามคู่มือ: ระบบส่งค่ากลับมาเป็น 'orderFee' (ค่าฟีดิบเป็นจำนวนเงิน) ไม่ใช่ 'feePercent'
                order_fee_str = res["data"].get("orderFee", "0")
                order_fee = float(order_fee_str)
                
                # 3. คำนวณอัตราค่าธรรมเนียมกลับเป็นเปอร์เซ็นต์จริง: (ค่าธรรมเนียมจริง / มูลค่าคำสั่งซื้อรวม) * 100
                if order_fee > 0 and total_value > 0:
                    buy_fee_pct = (order_fee / total_value) * 100
                    return buy_fee_pct * 2  # คูณ 2 เพื่อประมาณการค่าฟีแบบไป-กลับ (ซื้อ + ขาย)
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"เกิดข้อผิดพลาดในการคำนวณค่าธรรมเนียมจริง: {e}")
                
        logger.warning(f"ดึงค่าธรรมเนียมจริงไม่ได้ ใช้ default {self.DEFAULT_ROUNDTRIP_FEE_PERCENT}% แทน (ควรตรวจสอบ FEE_ESTIMATE_PATH และพารามิเตอร์)")
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
        """Poll ยืนยันราคาเฉลี่ยที่ match จริง แทนการ sleep คงที่ (กัน matching engine ช้ากว่าที่คาด)"""
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
        if daily_loss_percent >= self.MAX_DAILY_LOSS_PERCENT:
            halt_reason = f"ขาดทุนสะสมวันนี้ {daily_loss_percent:.2f}% เกินเพดาน {self.MAX_DAILY_LOSS_PERCENT}%"
        elif self.state["consecutive_losses"] >= self.MAX_CONSECUTIVE_LOSSES:
            halt_reason = f"ขาดทุนติดกัน {self.state['consecutive_losses']} ไม้ ถึงเพดาน {self.MAX_CONSECUTIVE_LOSSES} ไม้"

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
            price_open_1h = self._price_open_current_hour()
            if None in (price_1h_ago, price_2h_ago, price_open_1h):
                logger.info("ข้อมูลราคาบางช่วงขาดหาย (gap) รอรอบถัดไป")
                return

            rule_trend_up = current_price > price_1h_ago > price_2h_ago
            rule_current_hour_green = current_price > price_open_1h
            logger.info(f"เทรนด์ 3ชม: {current_price} > {price_1h_ago} > {price_2h_ago} -> {rule_trend_up} | "
                        f"ชม.นี้เขียว: {current_price} > {price_open_1h} -> {rule_current_hour_green}")

            if not (rule_trend_up and rule_current_hour_green):
                return

            logger.info("🔥 สัญญาณเข้าซื้อครบเงื่อนไข")
            thb_free, _, has_pending = self.get_free_balance()
            if has_pending:
                logger.info("ข้ามการซื้อ: มีออเดอร์ค้างอยู่ในระบบ")
                return
            if thb_free <= self.MIN_ORDER_THB:
                logger.info(f"เงินว่างไม่พอสำหรับซื้อขั้นต่ำ (คงเหลือ {thb_free:.2f} THB)")
                return

            rules = self.get_symbol_rules()
            buy_value = round(thb_free * 0.95, 2)
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

        self.state.update({
            "status": "IDLE",
            "entry_price": 0.0,
            "highest_price": 0.0,
            "quantity": 0.0,
        })
        self._register_trade_result(pnl_thb)

    # ==================== Main loop ====================

    def run(self, poll_interval_sec=60):
        logger.info(f"เริ่มบอทเทรด {self.symbol} (poll ทุก {poll_interval_sec} วิ) — กด Ctrl+C เพื่อหยุดอย่างปลอดภัย")
        self.reconcile_state_on_startup()
        while not self._stop_requested:
            try:
                self._maybe_reset_daily_counters()

                if self.state["status"] == "HALTED":
                    logger.warning("⛔ บอทอยู่ในสถานะ HALTED จาก Circuit Breaker — เฝ้าดูอย่างเดียว ไม่ส่งคำสั่งใหม่")
                else:
                    price = self.get_latest_price()
                    if price is not None:
                        self._record_price_tick(price)
                        self.run_strategy(price)
            except Exception:
                logger.exception("เกิดข้อผิดพลาดไม่คาดคิดใน main loop — บอทจะพยายามทำงานต่อในรอบถัดไป")

            for _ in range(poll_interval_sec):
                if self._stop_requested:
                    break
                time.sleep(1)

        logger.info("บอทหยุดทำงานเรียบร้อย (state ถูกบันทึกแล้ว)")


if __name__ == "__main__":
    api_key = os.environ.get("INVX_API_KEY")
    api_secret = os.environ.get("INVX_API_SECRET")
    if not api_key or not api_secret:
        raise SystemExit(
            "กรุณาตั้งค่า environment variable INVX_API_KEY และ INVX_API_SECRET ก่อนรัน "
            "(ห้าม hardcode API key/secret ลงในไฟล์โค้ดโดยเด็ดขาด)"
        )

    bot = InnovestXTradingBot(api_key=api_key, api_secret=api_secret, symbol="BTCTHB")
    bot.run(poll_interval_sec=60)
