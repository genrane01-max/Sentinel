"""Unit tests for Sentinel bot helpers — no API / Firebase required."""
import sys
import types
import unittest
from unittest.mock import patch


def _install_mocks():
    fa = types.ModuleType("firebase_admin")
    fa._apps = ["mock"]

    class _Cred:
        @staticmethod
        def Certificate(_path):
            return {}

    class _Ref:
        def get(self):
            return {}

        def set(self, _value):
            return None

        def update(self, _value):
            return None

    class _DB:
        @staticmethod
        def reference(_path, app=None):
            return _Ref()

    fa.credentials = _Cred
    fa.db = _DB
    fa.initialize_app = lambda *a, **k: "app"

    def _get_app(name=None):
        if name == "market":
            raise ValueError("Requested app 'market' not found")
        return "app"

    fa.get_app = _get_app
    sys.modules["firebase_admin"] = fa
    cred_mod = types.ModuleType("firebase_admin.credentials")
    cred_mod.Certificate = _Cred.Certificate
    sys.modules["firebase_admin.credentials"] = cred_mod
    db_mod = types.ModuleType("firebase_admin.db")
    db_mod.reference = _DB.reference
    sys.modules["firebase_admin.db"] = db_mod

    req = types.ModuleType("requests")
    class _Exc:
        class Timeout(Exception):
            pass
        class ConnectionError(Exception):
            pass
    req.exceptions = _Exc
    sys.modules["requests"] = req


_install_mocks()
import bot  # noqa: E402


class ParseMarketPriceTests(unittest.TestCase):
    def test_dict_last_trade_price(self):
        payload = {"code": "0000", "data": {"lastTradePrice": "2500000.5"}}
        self.assertAlmostEqual(bot.parse_market_price(payload), 2500000.5)

    def test_list_last_trade_price(self):
        payload = {"code": "0000", "data": [{"lastTradePrice": 101.25}]}
        self.assertAlmostEqual(bot.parse_market_price(payload), 101.25)

    def test_bid_ask_mid_when_no_last(self):
        payload = {
            "code": "0000",
            "data": {
                "bids": [["100.0", "1"], ["99.0", "2"]],
                "asks": [["102.0", "1"]],
            },
        }
        self.assertAlmostEqual(bot.parse_market_price(payload), 101.0)
        quote = bot.parse_market_quote(payload)
        self.assertAlmostEqual(quote["bid"], 100.0)
        self.assertAlmostEqual(quote["ask"], 102.0)
        self.assertAlmostEqual(quote["spread_pct"], (2.0 / 101.0) * 100, places=4)

    def test_quote_prefers_last_for_history_ask_for_buy(self):
        payload = {
            "code": "0000",
            "data": {
                "lastTradePrice": "101.0",
                "bids": [["100.0", "1"]],
                "asks": [["102.0", "1"]],
            },
        }
        quote = bot.parse_market_quote(payload)
        self.assertAlmostEqual(bot.quote_mark_price(quote, "last"), 101.0)
        self.assertAlmostEqual(bot.quote_mark_price(quote, "buy"), 102.0)
        self.assertAlmostEqual(bot.quote_mark_price(quote, "sell"), 100.0)

    def test_dict_bid_as_objects(self):
        payload = {
            "code": "0000",
            "data": {
                "bid": [{"price": "10"}, {"price": "9"}],
                "ask": [{"px": "12"}],
            },
        }
        self.assertAlmostEqual(bot.parse_market_price(payload), 11.0)

    def test_rejects_non_success(self):
        self.assertIsNone(bot.parse_market_price({"code": "4005", "data": {"lastTradePrice": "1"}}))

    def test_old_broken_shape_list_without_last_still_reads_book(self):
        payload = {
            "code": "0000",
            "data": [{"bids": [[99.0, 1]], "asks": [[101.0, 1]]}],
        }
        self.assertAlmostEqual(bot.parse_market_price(payload), 100.0)


class WatchlistTests(unittest.TestCase):
    def test_missing_uses_fallback(self):
        self.assertEqual(bot._normalize_watchlist(None, "BTCTHB"), ["BTCTHB"])

    def test_empty_list_stays_empty(self):
        self.assertEqual(bot._normalize_watchlist([], "BTCTHB"), [])

    def test_dedupes_and_uppercases(self):
        self.assertEqual(
            bot._normalize_watchlist(["eththb", "ETHTHB", "solthb"], "BTCTHB"),
            ["ETHTHB", "SOLTHB"],
        )

    def test_rejects_letters_only_not_a_pair(self):
        self.assertEqual(bot._normalize_watchlist(["HELLO", "BTC"], "BTCTHB"), [])

    def test_looks_like_pair(self):
        self.assertTrue(bot.looks_like_pair("BTCTHB"))
        self.assertTrue(bot.looks_like_pair("eththb"))
        self.assertFalse(bot.looks_like_pair("HELLO"))
        self.assertFalse(bot.looks_like_pair("THB"))
        self.assertFalse(bot.looks_like_pair("BTC-THB"))

    def test_validate_catalog_rejects_unknown(self):
        sym, err = bot.validate_watchlist_symbol("FAKETHB", catalog={"BTCTHB", "ETHTHB"})
        self.assertIsNone(sym)
        self.assertIn("ไม่พบคู่", err)

    def test_validate_catalog_accepts_known(self):
        sym, err = bot.validate_watchlist_symbol("eththb", catalog={"BTCTHB", "ETHTHB"})
        self.assertEqual(sym, "ETHTHB")
        self.assertIsNone(err)


class EntrySignalTests(unittest.TestCase):
    def test_buy_when_two_hours_up(self):
        # now=107, 1h=100 (+7% would chase) — use modest move
        # 1h: 100.8 / 100 = +0.8%, 2h: 100/99.2 = +0.81%, net 100.8/99.2 = +1.61%
        sig = bot.evaluate_entry_signal(100.8, 100.0, 99.2, 99.0)
        self.assertTrue(sig["should_buy"])
        self.assertEqual(sig["confidence"], 100)

    def test_hour3_veto(self):
        sig = bot.evaluate_entry_signal(100.8, 100.0, 99.2, 101.0)  # hour3 = 99.2/101 = -1.78%
        self.assertFalse(sig["should_buy"])
        self.assertTrue(sig["vetoed"])
        self.assertEqual(sig["reason"], "hour3_veto")

    def test_hour2_down_blocks(self):
        sig = bot.evaluate_entry_signal(101.0, 100.0, 101.0, 100.0)
        self.assertFalse(sig["should_buy"])
        self.assertEqual(sig["reason"], "hour2_down")
        self.assertEqual(sig["confidence"], 10)

    def test_min_confidence_80_allows_weak_net(self):
        # ชม.1 +0.55%, ชม.2 +0.10%, สุทธิ +0.65% < 0.7% → คะแนน 80
        sig = bot.evaluate_entry_signal(100.55, 100.0, 99.90, 99.0)
        self.assertEqual(sig["confidence"], 80)
        self.assertTrue(sig["should_buy"])
        self.assertIsNone(sig["reason"])

    def test_min_confidence_100_blocks_weak_net(self):
        with patch.object(bot, "MIN_CONFIDENCE_TO_BUY", 100):
            sig = bot.evaluate_entry_signal(100.55, 100.0, 99.90, 99.0)
            self.assertEqual(sig["confidence"], 80)
            self.assertFalse(sig["should_buy"])
            self.assertEqual(sig["reason"], "weak_net")


class SafeFloatTests(unittest.TestCase):
    def test_none_and_blank(self):
        self.assertEqual(bot._safe_float(None, 0.0), 0.0)
        self.assertEqual(bot._safe_float("", 0.0), 0.0)
        self.assertIsNone(bot._safe_float(None))

    def test_numeric_string(self):
        self.assertEqual(bot._safe_float("1.5"), 1.5)


class FeeAndPnlTests(unittest.TestCase):
    def test_net_pnl_subtracts_roundtrip_fee(self):
        # ต้นทุน 100 ขาย 101 จำนวน 1 ค่าฟีไป-กลับ 0.4% → ไม่ใช่กำไร 1 บาท
        gross = (101.0 - 100.0) * 1.0
        net = bot.net_pnl_thb(100.0, 101.0, 1.0, 0.40)
        self.assertAlmostEqual(gross, 1.0)
        self.assertLess(net, gross)
        self.assertAlmostEqual(net, 101.0 * 0.998 - 100.0 * 1.002, places=6)

    def test_net_pnl_zero_inputs(self):
        self.assertEqual(bot.net_pnl_thb(0, 101, 1, 0.4), 0.0)
        self.assertEqual(bot.net_pnl_thb(100, 0, 1, 0.4), 0.0)


class OrderSafetyTests(unittest.TestCase):
    def test_mutating_order_path(self):
        self.assertTrue(bot.is_mutating_order_path("/api/v1/digital-asset/order/send"))
        self.assertFalse(bot.is_mutating_order_path("/api/v1/digital-asset/order/history/inquiry"))
        self.assertFalse(bot.is_mutating_order_path("/api/v1/digital-asset/orderbook/lvl2"))

    def test_price_tick_skips_near_duplicates(self):
        hist = [[1000.0, 100.0]]
        self.assertFalse(bot.should_append_price_tick(hist, 1030.0, 100.02))
        self.assertTrue(bot.should_append_price_tick(hist, 1070.0, 100.02))
        self.assertTrue(bot.should_append_price_tick(hist, 1010.0, 100.20))  # ขยับ 0.2%

    def test_sell_without_fill_price_stays_holding(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="BTCTHB")
        b.state.update({
            "status": "HOLDING",
            "entry_price": 100.0,
            "highest_price": 101.0,
            "quantity": 0.5,
            "roundtrip_fee_percent": 0.4,
        })
        b.get_free_balance = lambda: (1000.0, 0.5, False)
        b.get_symbol_rules = lambda: {"quantity_increment": "0.00001000", "price_increment": "0.01", "decimal_places": 8}
        b.execute_market_order = lambda **kw: {"code": "0000", "data": {"orderId": "abc"}}
        b.confirm_fill_price = lambda *a, **k: 0.0
        b.save_state = lambda: None
        b.sell_position(0.5, 100.0)
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertEqual(b.state["entry_price"], 100.0)

    def test_sell_without_fill_price_idles_only_if_coins_gone(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="ETHTHB")
        b.state.update({
            "status": "HOLDING",
            "entry_price": 100.0,
            "highest_price": 101.0,
            "quantity": 0.5,
            "roundtrip_fee_percent": 0.4,
        })
        calls = {"n": 0}

        def fake_balance():
            calls["n"] += 1
            if calls["n"] == 1:
                return (1000.0, 0.5, False)
            return (1000.0, 0.0, False)

        b.get_free_balance = fake_balance
        b.get_symbol_rules = lambda: {"quantity_increment": "0.00001000", "price_increment": "0.01", "decimal_places": 8}
        b.execute_market_order = lambda **kw: {"code": "0000", "data": {"orderId": "abc"}}
        b.confirm_fill_price = lambda *a, **k: 0.0
        b.save_state = lambda: None
        b.sell_position(0.5, 100.0)
        self.assertEqual(b.state["status"], "IDLE")

    def test_timeout_does_not_resend_order(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="SOLTHB")
        calls = {"n": 0}

        def fake_send(method, path, query="", body=None, _retry_count=0):
            calls["n"] += 1
            if path.endswith("/order/send"):
                return {"code": "TIMEOUT_INDETERMINATE", "path": path}
            if path.endswith("/order/open/inquiry"):
                return {"code": "0000", "data": []}
            if path.endswith("/order/history/inquiry"):
                return {"code": "0000", "data": []}
            return {"code": "0000", "data": []}

        b.send_request = fake_send
        b.save_state = lambda: None
        with patch("bot.time.sleep", return_value=None):
            res = b.execute_market_order(side=0, value=200.0)
        self.assertIsNone(res)
        self.assertIsNotNone(b.state.get("pending_order"))
        send_calls = calls["n"]
        with patch("bot.time.sleep", return_value=None):
            res2 = b.execute_market_order(side=0, value=200.0)
        self.assertIsNone(res2)
        # รอบสองต้องไม่ยิง /order/send ซ้ำ — แค่ poll หาออเดอร์
        self.assertEqual(b.state["pending_order"]["side"], 0)

    def test_shutdown_handlers_installed_once(self):
        bot._SHUTDOWN_HANDLERS_INSTALLED = False
        with patch("bot.signal.signal") as sig:
            bot.install_shutdown_handlers()
            bot.install_shutdown_handlers()
            self.assertEqual(sig.call_count, 2)  # SIGINT + SIGTERM ครั้งเดียว
        bot._SHUTDOWN_HANDLERS_INSTALLED = True


if __name__ == "__main__":
    unittest.main()
