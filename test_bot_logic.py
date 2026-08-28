"""Unit tests for Sentinel bot helpers — no API / Firebase required."""
import sys
import time
import types
import unittest
from unittest.mock import patch
import os


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
    req.post = lambda *a, **k: types.SimpleNamespace(ok=True, status_code=200)
    req.get = lambda *a, **k: types.SimpleNamespace(ok=True, status_code=200)
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

    def test_level2_side_rows(self):
        payload = {
            "code": "0000",
            "data": [
                {"side": 0, "price": "100.0"},
                {"side": 1, "price": "102.0"},
                {"lastTradePrice": "101.0"},
            ],
        }
        quote = bot.parse_market_quote(payload)
        self.assertAlmostEqual(quote["bid"], 100.0)
        self.assertAlmostEqual(quote["ask"], 102.0)
        self.assertAlmostEqual(quote["last"], 101.0)
        self.assertAlmostEqual(bot.quote_mark_price(quote, "buy"), 102.0)


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
    def test_buy_on_pullback_after_up_hour(self):
        # ชม.2 +1.61%, ชม.1 -0.79%, สุทธิ +0.81%, 15 นาทีเด้งขึ้น
        sig = bot.evaluate_entry_signal(100.0, 100.8, 99.2, 99.0, price_15m_ago=99.85)
        self.assertTrue(sig["should_buy"])
        self.assertEqual(sig["direction"], "up")
        self.assertEqual(sig["confidence"], 100)
        self.assertIsNone(sig["reason"])

    def test_does_not_chase_while_still_rising(self):
        # ของเดิมซื้อเคสนี้ — ตอนนี้รอย่อ
        sig = bot.evaluate_entry_signal(100.8, 100.0, 99.2, 99.0)
        self.assertFalse(sig["should_buy"])
        self.assertEqual(sig["reason"], "waiting_pullback")
        self.assertEqual(sig["direction"], "up")

    def test_hour3_veto(self):
        sig = bot.evaluate_entry_signal(100.0, 100.8, 99.2, 101.0)  # hour3 = 99.2/101 = -1.78%
        self.assertFalse(sig["should_buy"])
        self.assertTrue(sig["vetoed"])
        self.assertEqual(sig["reason"], "hour3_veto")

    def test_hour2_down_blocks(self):
        sig = bot.evaluate_entry_signal(99.5, 100.5, 101.0, 100.0)
        self.assertFalse(sig["should_buy"])
        self.assertEqual(sig["reason"], "hour2_down")

    def test_deep_pullback_blocks(self):
        # ชม.2 +2.0%, ชม.1 -2.5%
        sig = bot.evaluate_entry_signal(97.5, 100.0, 98.04, 98.0)
        self.assertFalse(sig["should_buy"])
        self.assertEqual(sig["reason"], "deep_pullback")

    def test_still_falling_15m_blocks(self):
        sig = bot.evaluate_entry_signal(100.0, 100.8, 99.2, 99.0, price_15m_ago=100.4)
        self.assertFalse(sig["should_buy"])
        self.assertEqual(sig["reason"], "still_falling")

    def test_missing_15m_waits_instead_of_buying(self):
        sig = bot.evaluate_entry_signal(100.0, 100.8, 99.2, 99.0)
        self.assertFalse(sig["should_buy"])
        self.assertEqual(sig["reason"], "waiting_15m")

    def test_bounce_15m_allows_buy(self):
        sig = bot.evaluate_entry_signal(100.0, 100.8, 99.2, 99.0, price_15m_ago=99.85)
        self.assertTrue(sig["should_buy"])


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
        self.assertFalse(bot.should_append_price_tick(hist, 1006.0, 100.02))  # ยังไม่ครบ 7 วิ และขยับน้อย
        self.assertTrue(bot.should_append_price_tick(hist, 1007.0, 100.02))   # ครบช่วงแล้ว
        self.assertTrue(bot.should_append_price_tick(hist, 1003.0, 100.20))  # ขยับ 0.2%

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

    def test_sell_keeps_unsellable_dust_for_next_buy(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="ETHTHB")
        b.state.update({
            "status": "HOLDING",
            "entry_price": 100000.0,
            "highest_price": 101000.0,
            "quantity": 0.0227,
            "roundtrip_fee_percent": 0.4,
            "dust_quantity": 0.0,
        })
        b.get_free_balance = lambda: (1000.0, 0.0227544, False)
        b.get_symbol_rules = lambda: {
            "quantity_increment": "0.0001",
            "price_increment": "0.01",
            "decimal_places": 8,
        }
        b.execute_market_order = lambda **kw: {"code": "0000", "data": {"orderId": "abc"}}
        b.confirm_fill_price = lambda *a, **k: 101000.0
        b.save_state = lambda: None
        b._register_trade_result = lambda pnl: None
        b._after_successful_sell = lambda *a, **k: None
        b.sell_position(0.0227, 101000.0)
        self.assertEqual(b.state["status"], "IDLE")
        self.assertEqual(b.state["quantity"], 0.0)
        self.assertAlmostEqual(b.state["dust_quantity"], 0.0000544, places=7)

        b._adopt_filled_buy(100000.0, 0.05, 0.4)
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertAlmostEqual(b.state["quantity"], 0.0500544, places=7)
        self.assertEqual(b.state["dust_quantity"], 0.0)

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

    def test_pending_does_not_expire_by_time_alone(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="ADAUSDT")
        b.state["pending_order"] = {"side": 0, "ts": time.time() - 10_000, "value": 200}
        self.assertTrue(b._has_unresolved_order())


class OrderMatchTests(unittest.TestCase):
    def test_parse_epoch_ms_and_iso(self):
        self.assertAlmostEqual(bot.parse_order_timestamp({"transactTime": 1_700_000_000_000}), 1_700_000_000.0)
        self.assertAlmostEqual(bot.parse_order_timestamp({"createdAt": 1_700_000_000}), 1_700_000_000.0)
        ts = bot.parse_order_timestamp({"time": "2024-01-01T00:00:00Z"})
        self.assertIsNotNone(ts)

    def test_ignores_old_history_order(self):
        now = time.time()
        old = {"symbol": "BTCTHB", "side": 0, "orderId": "old", "transactTime": (now - 86400) * 1000}
        recent = {"symbol": "BTCTHB", "side": 0, "orderId": "new", "transactTime": now * 1000}
        self.assertFalse(bot.order_is_recent_match(old, "BTCTHB", 0, now - 10))
        self.assertTrue(bot.order_is_recent_match(recent, "BTCTHB", 0, now - 10))

    def test_history_without_timestamp_is_not_matched(self):
        order = {"symbol": "BTCTHB", "side": 0, "orderId": "mystery"}
        self.assertFalse(bot.order_is_recent_match(order, "BTCTHB", 0, time.time() - 10))

    def test_open_order_without_timestamp_is_matched(self):
        order = {"symbol": "BTCTHB", "side": 0, "orderId": "live", "orderState": "Working"}
        self.assertTrue(
            bot.order_is_recent_match(order, "BTCTHB", 0, time.time() - 10, allow_open_without_time=True)
        )


class BuyPathTests(unittest.TestCase):
    def _bot(self, symbol="BTCTHB"):
        b = bot.InnovestXTradingBot("k", "s", symbol=symbol)
        b.save_state = lambda: None
        b.save_market = lambda **k: None
        b.get_symbol_rules = lambda: {
            "quantity_increment": "0.00001000",
            "price_increment": "0.01",
            "decimal_places": 8,
        }
        b.estimate_roundtrip_fee_percent = lambda: 0.4
        b.state["status"] = "IDLE"
        b.state["quote"] = {"bid": 99.0, "ask": 100.0, "last": 99.5, "spread_pct": 0.1}
        return b

    def test_does_not_buy_if_coins_already_in_wallet(self):
        b = self._bot()
        b.get_free_balance = lambda: (5000.0, 0.01, False)

        def boom(**kwargs):
            raise AssertionError("must not send buy when coins already exist")

        b.execute_market_order = boom
        with patch("bot.time.sleep", return_value=None):
            ok = b.try_enter_position(100.0)
        self.assertFalse(ok)
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertGreater(b.state["entry_price"], 0.0)
        self.assertTrue(b.state["entry_price_estimated"])
        self.assertGreater(b.state["quantity"], 0)

    def test_uses_actual_wallet_qty_not_estimate(self):
        b = self._bot("ETHTHB")
        calls = {"n": 0}

        def fake_balance():
            calls["n"] += 1
            if calls["n"] == 1:
                return (5000.0, 0.0, False)
            return (4000.0, 0.01234567, False)

        b.get_free_balance = fake_balance
        b.execute_market_order = lambda **kw: {"code": "0000", "data": {"orderId": "oid-1"}}
        b.confirm_fill_price = lambda *a, **k: 101.5
        with patch("bot.time.sleep", return_value=None):
            ok = b.try_enter_position(100.0)
        self.assertTrue(ok)
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertAlmostEqual(b.state["entry_price"], 101.5)
        self.assertAlmostEqual(b.state["quantity"], 0.01234)

    def test_unknown_fill_price_adopts_estimated_cost_instead_of_waiting(self):
        b = self._bot("SOLTHB")
        calls = {"n": 0}

        def fake_balance():
            calls["n"] += 1
            if calls["n"] == 1:
                return (5000.0, 0.0, False)
            return (4000.0, 0.02, False)

        b.get_free_balance = fake_balance
        b.execute_market_order = lambda **kw: {"code": "0000", "data": {"orderId": "oid-2"}}
        b.confirm_fill_price = lambda *a, **k: 0.0
        with patch("bot.time.sleep", return_value=None):
            ok = b.try_enter_position(100.0)
        self.assertTrue(ok)
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertAlmostEqual(b.state["entry_price"], 100.0)
        self.assertTrue(b.state["entry_price_estimated"])
        self.assertGreater(b.state["quantity"], 0)

    def test_unknown_fill_tracks_high_if_coin_already_ran(self):
        b = self._bot("SOLTHB")
        b.state["quote"] = {"bid": 118.0, "ask": 100.0, "last": 120.0, "spread_pct": 0.1}
        calls = {"n": 0}

        def fake_balance():
            calls["n"] += 1
            if calls["n"] == 1:
                return (5000.0, 0.0, False)
            return (4000.0, 0.02, False)

        b.get_free_balance = fake_balance
        b.execute_market_order = lambda **kw: {"code": "0000", "data": {"orderId": "oid-3"}}
        b.confirm_fill_price = lambda *a, **k: 0.0
        with patch("bot.time.sleep", return_value=None):
            ok = b.try_enter_position(100.0)
        self.assertTrue(ok)
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertAlmostEqual(b.state["entry_price"], 100.0)
        self.assertAlmostEqual(b.state["highest_price"], 120.0)

    def test_unknown_cost_without_price_stays_halted_then_auto_resumes(self):
        b = self._bot("ADAUSDT")
        b.state["quote"] = {}
        b.get_latest_price = lambda: None
        b.get_free_balance = lambda: (5000.0, 0.01, False)
        b.execute_market_order = lambda **kw: (_ for _ in ()).throw(AssertionError("must not buy"))
        with patch("bot.time.sleep", return_value=None):
            ok = b.try_enter_position(0.0)
        self.assertFalse(ok)
        self.assertEqual(b.state["status"], "HALTED")
        self.assertEqual(b.state["entry_price"], 0.0)

        b.state["quote"] = {"bid": 104.0, "ask": 106.0, "last": 105.0, "spread_pct": 0.1}
        b.get_latest_price = lambda: 105.0
        self.assertTrue(b.resume_from_halt())
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertAlmostEqual(b.state["entry_price"], 106.0)
        self.assertTrue(b.state["entry_price_estimated"])

    def test_timeout_then_coins_arrive_does_not_resend(self):
        b = self._bot("XRPTHB")
        sent = {"n": 0}
        balances = {"n": 0}

        def fake_send(method, path, query="", body=None, _retry_count=0):
            if path.endswith("/order/send"):
                sent["n"] += 1
                return {"code": "TIMEOUT_INDETERMINATE", "path": path}
            if path.endswith("/order/open/inquiry"):
                return {"code": "0000", "data": []}
            if path.endswith("/order/history/inquiry"):
                return {"code": "0000", "data": []}
            return {"code": "0000", "data": []}

        def fake_balance():
            balances["n"] += 1
            if balances["n"] == 1:
                return (5000.0, 0.0, False)
            return (4000.0, 0.03, False)

        b.send_request = fake_send
        b.get_free_balance = fake_balance
        b.confirm_fill_price = lambda *a, **k: 0.0
        with patch("bot.time.sleep", return_value=None):
            ok = b.try_enter_position(100.0)
        self.assertTrue(ok)
        self.assertEqual(sent["n"], 1)
        self.assertEqual(b.state["status"], "HOLDING")
        self.assertTrue(b.state["entry_price_estimated"])
        with patch("bot.time.sleep", return_value=None):
            ok2 = b.try_enter_position(100.0)
        self.assertFalse(ok2)
        self.assertEqual(sent["n"], 1)

    def test_paused_watch_does_not_buy(self):
        b = self._bot()
        b.execute_market_order = lambda **kw: (_ for _ in ()).throw(AssertionError("must not buy"))
        paused_control = {"paused_symbols": ["BTCTHB"], "watchlist": ["BTCTHB"]}
        with patch.object(bot, "load_control", return_value=paused_control):
            ok = b.try_enter_position(100.0)
        self.assertFalse(ok)
        self.assertEqual(b.state["status"], "IDLE")

    def test_stop_loss_pauses_watch_and_notifies(self):
        b = self._bot("ETHTHB")
        paused = {}
        notes = []
        with patch.object(bot, "set_watch_paused", side_effect=lambda *a, **k: paused.update({"sym": a[0], "on": a[1], "reason": k.get("reason")})) as _, \
             patch.object(bot, "notify_telegram", side_effect=lambda t: notes.append(t)):
            b._after_successful_sell(-12.5, reason="stop_loss")
        self.assertEqual(paused["sym"], "ETHTHB")
        self.assertTrue(paused["on"])
        self.assertEqual(paused["reason"], "stop_loss")
        self.assertTrue(any("ตัดขาดทุน" in t for t in notes))
        self.assertTrue(any("เฝ้าต่อ" in t for t in notes))


class TrailingStopTests(unittest.TestCase):
    def _holding(self, entry=100.0, highest=100.3, bid=99.2, last=99.3, ask=99.5):
        b = bot.InnovestXTradingBot("k", "s", symbol="BTCTHB")
        b.save_state = lambda: None
        b.save_market = lambda **k: None
        b.trailing_stop_percent = 1.0
        b.stop_loss_percent = 3.0
        b.state.update({
            "status": "HOLDING",
            "entry_price": entry,
            "highest_price": highest,
            "quantity": 1.0,
            "roundtrip_fee_percent": 0.4,
            "quote": {"bid": bid, "ask": ask, "last": last, "spread_pct": 0.3},
        })
        sold = []
        b.sell_position = lambda qty, current_price=None, reason="": sold.append(reason)
        return b, sold

    def test_tiny_peak_then_small_pullback_does_not_sell(self):
        b, sold = self._holding(highest=100.3, bid=99.2, last=99.3)
        b.run_strategy(99.3)
        self.assertEqual(sold, [])
        self.assertEqual(b.state["status"], "HOLDING")

    def test_real_peak_trailing_sells_when_still_in_profit(self):
        b, sold = self._holding(highest=102.0, bid=100.85, last=100.9, ask=101.0)
        b.run_strategy(100.9)
        self.assertEqual(sold, ["trailing"])

    def test_real_peak_does_not_trailing_sell_below_breakeven(self):
        b, sold = self._holding(highest=102.0, bid=100.2, last=100.3, ask=100.4)
        b.run_strategy(100.3)
        self.assertEqual(sold, [])

    def test_hard_stop_loss_still_sells_after_tiny_peak(self):
        b, sold = self._holding(highest=100.2, bid=96.5, last=96.6, ask=96.8)
        b.run_strategy(96.6)
        self.assertEqual(sold, ["stop_loss"])


def _reversal_history(now, marks):
    """marks: ราคาที่แต่ละจุดของหน้าต่างกลับตัว (เก่า → ใหม่ รวมจุดเริ่ม) ตามค่าคงที่ปัจจุบัน"""
    window_sec = bot.REVERSAL_WINDOW_MINUTES * 60
    bucket_sec = bot.REVERSAL_BUCKET_MINUTES * 60
    n = int(round(bot.REVERSAL_WINDOW_MINUTES / bot.REVERSAL_BUCKET_MINUTES))
    if len(marks) != n + 1:
        raise AssertionError(f"need {n + 1} marks for current reversal window, got {len(marks)}")
    return [[now - window_sec + i * bucket_sec, marks[i]] for i in range(n + 1)]


class MomentumReversalTests(unittest.TestCase):
    def test_detects_three_bucket_fade(self):
        now = 2_000_000.0
        hist = _reversal_history(now, [100.80, 100.65, 100.50, 100.45])
        sig = bot.detect_momentum_reversal(hist, now=now)
        self.assertTrue(sig["is_reversal"], sig)
        self.assertIsNone(sig["reason"])
        self.assertLessEqual(sig["total_change"], bot.REVERSAL_MIN_TOTAL_PERCENT)

    def test_not_enough_down_buckets(self):
        now = 2_000_000.0
        hist = _reversal_history(now, [100.80, 100.79, 100.78, 100.40])
        sig = bot.detect_momentum_reversal(hist, now=now)
        self.assertFalse(sig["is_reversal"])
        self.assertEqual(sig["reason"], "not_enough_down_buckets")

    def test_not_enough_total_drop(self):
        now = 2_000_000.0
        hist = _reversal_history(now, [100.50, 100.37, 100.24, 100.30])
        sig = bot.detect_momentum_reversal(hist, now=now)
        self.assertFalse(sig["is_reversal"])
        self.assertIn(sig["reason"], ("not_enough_total_drop", "not_enough_down_buckets"))

    def test_waiting_history(self):
        now = 2_000_000.0
        hist = [[now - 60, 100.0], [now, 99.7]]
        sig = bot.detect_momentum_reversal(hist, now=now)
        self.assertFalse(sig["is_reversal"])
        self.assertEqual(sig["reason"], "waiting_history")

    def test_sells_above_breakeven_without_trailing_arm(self):
        now = time.time()
        b = bot.InnovestXTradingBot("k", "s", symbol="BTCTHB")
        b.save_state = lambda: None
        b.save_market = lambda **k: None
        b.trailing_stop_percent = 1.0
        b.stop_loss_percent = 3.0
        b.state.update({
            "status": "HOLDING",
            "entry_price": 100.0,
            "highest_price": 100.5,  # ยังไม่ถึง 101.4 จึงยังไม่ arm trailing
            "quantity": 1.0,
            "roundtrip_fee_percent": 0.4,
            "quote": {"bid": 100.50, "ask": 100.60, "last": 100.52, "spread_pct": 0.1},
            "price_history": _reversal_history(now, [100.80, 100.65, 100.50, 100.50]),
        })
        sold = []
        b.sell_position = lambda qty, current_price=None, reason="": sold.append(reason)
        b.run_strategy(100.52)
        self.assertEqual(sold, ["momentum_reversal"])

    def test_reversal_below_breakeven_does_not_sell(self):
        now = time.time()
        b = bot.InnovestXTradingBot("k", "s", symbol="BTCTHB")
        b.save_state = lambda: None
        b.save_market = lambda **k: None
        b.trailing_stop_percent = 1.0
        b.stop_loss_percent = 3.0
        b.state.update({
            "status": "HOLDING",
            "entry_price": 100.0,
            "highest_price": 100.5,
            "quantity": 1.0,
            "roundtrip_fee_percent": 0.4,
            "quote": {"bid": 100.20, "ask": 100.30, "last": 100.22, "spread_pct": 0.1},
            "price_history": _reversal_history(now, [100.80, 100.65, 100.50, 100.22]),
        })
        sold = []
        b.sell_position = lambda qty, current_price=None, reason="": sold.append(reason)
        b.run_strategy(100.22)
        self.assertEqual(sold, [])
        self.assertEqual(b.state["status"], "HOLDING")

    def test_stop_loss_still_wins_over_reversal(self):
        now = time.time()
        b = bot.InnovestXTradingBot("k", "s", symbol="BTCTHB")
        b.save_state = lambda: None
        b.save_market = lambda **k: None
        b.trailing_stop_percent = 1.0
        b.stop_loss_percent = 3.0
        b.state.update({
            "status": "HOLDING",
            "entry_price": 100.0,
            "highest_price": 100.5,
            "quantity": 1.0,
            "roundtrip_fee_percent": 0.4,
            "quote": {"bid": 96.50, "ask": 96.80, "last": 96.60, "spread_pct": 0.3},
            "price_history": _reversal_history(now, [100.80, 100.65, 100.50, 96.60]),
        })
        sold = []
        b.sell_position = lambda qty, current_price=None, reason="": sold.append(reason)
        b.run_strategy(96.60)
        self.assertEqual(sold, ["stop_loss"])

    def test_notifies_reversal_sell(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="ETHTHB")
        notes = []
        with patch.object(bot, "notify_telegram", lambda msg: notes.append(msg)):
            b._after_successful_sell(12.5, reason="momentum_reversal")
        self.assertEqual(len(notes), 1)
        self.assertIn("สัญญาณกลับตัว", notes[0])
        self.assertIn("ETH", notes[0])

    def _successful_sell_bot(self, reason):
        b = bot.InnovestXTradingBot("k", "s", symbol="ETHTHB")
        b.state.update({
            "status": "HOLDING",
            "entry_price": 100.0,
            "highest_price": 101.0,
            "quantity": 0.5,
            "roundtrip_fee_percent": 0.4,
            "dust_quantity": 0.0,
        })
        b.get_free_balance = lambda: (1000.0, 0.5, False)
        b.get_symbol_rules = lambda: {
            "quantity_increment": "0.00001000",
            "price_increment": "0.01",
            "decimal_places": 8,
        }
        b.execute_market_order = lambda **kw: {"code": "0000", "data": {"orderId": "abc"}}
        b.confirm_fill_price = lambda *a, **k: 101.0
        b.save_state = lambda: None
        b._register_trade_result = lambda pnl: None
        b._after_successful_sell = lambda *a, **k: None
        b.sell_position(0.5, 101.0, reason=reason)
        return b

    def test_reversal_sell_sets_cooldown(self):
        before = time.time()
        b = self._successful_sell_bot("momentum_reversal")
        until = float(b.state.get("reversal_cooldown_until") or 0.0)
        self.assertGreater(until, before + bot.REVERSAL_COOLDOWN_MINUTES * 60 - 2)
        self.assertLessEqual(until, time.time() + bot.REVERSAL_COOLDOWN_MINUTES * 60 + 1)

    def test_trailing_sell_clears_cooldown(self):
        b = self._successful_sell_bot("trailing")
        self.assertEqual(float(b.state.get("reversal_cooldown_until") or 0.0), 0.0)

    def test_cooldown_blocks_new_entry(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="BTCTHB")
        b.save_state = lambda: None
        b.save_market = lambda **k: None
        b.state["status"] = "IDLE"
        b.state["reversal_cooldown_until"] = time.time() + 600
        called = {"bal": False}

        def fake_bal():
            called["bal"] = True
            return (5000.0, 0.0, False)

        b.get_free_balance = fake_bal
        b.execute_market_order = lambda **kw: (_ for _ in ()).throw(AssertionError("must not buy during cooldown"))
        ok = b.try_enter_position(100.0)
        self.assertFalse(ok)
        self.assertFalse(called["bal"])
        self.assertEqual(b.state["status"], "IDLE")

    def test_expired_cooldown_does_not_block_entry(self):
        b = bot.InnovestXTradingBot("k", "s", symbol="BTCTHB")
        b.save_state = lambda: None
        b.save_market = lambda **k: None
        b.get_symbol_rules = lambda: {
            "quantity_increment": "0.00001000",
            "price_increment": "0.01",
            "decimal_places": 8,
        }
        b.state["status"] = "IDLE"
        b.state["quote"] = {"bid": 99.0, "ask": 100.0, "last": 99.5, "spread_pct": 0.1}
        b.state["reversal_cooldown_until"] = time.time() - 1
        called = {"bal": False}

        def fake_bal():
            called["bal"] = True
            return (0.0, 0.0, False)

        b.get_free_balance = fake_bal
        b.execute_market_order = lambda **kw: (_ for _ in ()).throw(AssertionError("must not buy with empty wallet"))
        ok = b.try_enter_position(100.0)
        self.assertFalse(ok)
        self.assertTrue(called["bal"])


class WatchPauseTests(unittest.TestCase):
    def test_apply_pause_and_resume(self):
        control = {"watchlist": ["ETHTHB", "SOLTHB"], "paused_symbols": []}
        control = bot.apply_watch_pause(control, "eththb", True, reason="stop_loss")
        self.assertEqual(control["paused_symbols"], ["ETHTHB"])
        self.assertEqual(control["pause_reasons"]["ETHTHB"], "stop_loss")
        self.assertTrue(bot.is_watch_paused("ETHTHB", control))
        self.assertFalse(bot.is_watch_paused("SOLTHB", control))
        control = bot.apply_watch_pause(control, "ETHTHB", False)
        self.assertEqual(control["paused_symbols"], [])
        self.assertFalse(bot.is_watch_paused("ETHTHB", control))

    def test_notify_skips_without_credentials(self):
        with patch.dict(os.environ, {"TELEGRAM_BOT_TOKEN": "", "TELEGRAM_CHAT_ID": ""}):
            self.assertFalse(bot.notify_telegram("hello"))


class HistoryGapTests(unittest.TestCase):
    def test_trim_drops_stale_segment_before_downtime_hole(self):
        now = 2_000_000.0
        old = [[now - 10800, 100.0], [now - 7200, 101.0]]
        hole_then_new = old + [[now - 30, 102.0], [now - 15, 102.2]]
        trimmed = bot.trim_to_continuous_recent(hole_then_new, now=now, max_gap_sec=600)
        self.assertEqual(len(trimmed), 2)
        self.assertEqual(trimmed[0][1], 102.0)
        elapsed = bot.history_elapsed_sec(hole_then_new, now=now, max_gap_sec=600)
        self.assertLess(elapsed, 60)

    def test_trim_keeps_continuous_two_hours(self):
        now = 2_000_000.0
        hist = [[now - 7200 + i * 15, 100.0 + i * 0.01] for i in range(0, 480)]
        trimmed = bot.trim_to_continuous_recent(hist, now=now, max_gap_sec=600)
        self.assertEqual(len(trimmed), len(hist))
        self.assertGreaterEqual(bot.history_elapsed_sec(hist, now=now), 7190)

    def test_stale_history_counts_as_waiting_on_dashboard(self):
        now = time.time()
        # ประวัติเก่า 3 ชม. แต่มีรูตอนย้ายเครื่อง — หน้าเว็บต้องโชว์แถบรอ ไม่ใช่คิดว่าครบแล้ว
        state = {
            "status": "IDLE",
            "price_history": [
                [now - 10800, 100.0],
                [now - 9000, 101.0],
                [now - 20, 102.0],
            ],
        }
        trend = bot._trend_from_state(state)
        self.assertEqual(trend["reason"], "waiting_history")
        self.assertLess(trend["elapsed"], 120)
        html = bot.render_dashboard(
            ["BTCTHB"],
            {"BTCTHB": state},
            {
                "watchlist": ["BTCTHB"],
                "paused": False,
                "max_open_positions": 3,
                "max_consecutive_losses": 3,
                "max_daily_loss_percent": 5,
                "trade_size_percent": 10,
                "trailing_stop_percent": 1,
                "stop_loss_percent": 3,
            },
            {},
        )
        self.assertIn("กำลังสะสมข้อมูลราคา", html)
        self.assertIn("รอข้อมูล", html)

    def test_ninety_minutes_still_waiting_two_hours(self):
        now = time.time()
        hist = [[now - 5400 + i * 15, 100.0] for i in range(0, 360)]
        trend = bot._trend_from_state({"status": "IDLE", "price_history": hist})
        self.assertEqual(trend["reason"], "waiting_history")
        self.assertGreater(trend["elapsed"], 5000)
        self.assertLess(trend["elapsed"], 7200)


class ControlPersistenceTests(unittest.TestCase):
    def setUp(self):
        bot._CONTROL_CACHE["value"] = None
        bot._CONTROL_CACHE["fresh"] = False

    def tearDown(self):
        bot._CONTROL_CACHE["value"] = None
        bot._CONTROL_CACHE["fresh"] = False

    def test_failed_load_does_not_invent_btc_watchlist(self):
        with patch.object(bot, "primary_ref") as ref:
            ref.return_value.get.side_effect = TimeoutError("Firebase ไม่ตอบใน 12 วินาที")
            control = bot.load_control()
        self.assertEqual(control["watchlist"], [])
        self.assertTrue(control.get("_uninitialized"))
        self.assertFalse(bot._CONTROL_CACHE["fresh"])

    def test_save_skips_uninitialized_so_watchlist_is_not_overwritten(self):
        writes = []

        class _Ref:
            def set(self, value):
                writes.append(value)

        with patch.object(bot, "primary_ref", return_value=_Ref()):
            ok = bot.save_control(bot._uninitialized_control())
        self.assertFalse(ok)
        self.assertEqual(writes, [])

    def test_failed_load_reuses_cached_watchlist(self):
        bot._CONTROL_CACHE["value"] = {
            "watchlist": ["ETHTHB", "SOLTHB"],
            "active_symbol": "ETHTHB",
            "paused": False,
            "paused_symbols": [],
            "pause_reasons": {},
            "max_daily_loss_percent": 5,
            "trade_size_percent": 30,
            "max_consecutive_losses": 3,
            "max_open_positions": 3,
            "trailing_stop_percent": 1,
            "stop_loss_percent": 3,
            "unlock_requested": False,
        }
        bot._CONTROL_CACHE["fresh"] = True
        with patch.object(bot, "primary_ref") as ref:
            ref.return_value.get.side_effect = TimeoutError("Firebase ไม่ตอบใน 12 วินาที")
            control = bot.load_control()
        self.assertEqual(control["watchlist"], ["ETHTHB", "SOLTHB"])
        self.assertFalse(control.get("_uninitialized"))

    def test_successful_load_then_save_writes_real_watchlist(self):
        writes = []

        class _Ref:
            def get(self):
                return {"watchlist": ["ETHTHB", "XRPTHB"], "trade_size_percent": 30}

            def set(self, value):
                writes.append(value)

        with patch.object(bot, "primary_ref", return_value=_Ref()):
            control = bot.load_control()
            self.assertEqual(control["watchlist"], ["ETHTHB", "XRPTHB"])
            self.assertTrue(bot._CONTROL_CACHE["fresh"])
            self.assertTrue(bot.save_control(control))
        self.assertEqual(writes[0]["watchlist"], ["ETHTHB", "XRPTHB"])
        self.assertNotIn("_uninitialized", writes[0])

    def test_wait_for_fresh_control_retries_then_stays_uninitialized(self):
        with patch.object(bot, "init_firebase", return_value=False), \
             patch.object(bot, "load_control", side_effect=lambda: bot._uninitialized_control()), \
             patch("bot.time.sleep", return_value=None):
            control = bot.wait_for_fresh_control(attempts=3, base_sleep=0)
        self.assertTrue(control.get("_uninitialized"))
        self.assertEqual(control["watchlist"], [])

    def test_dashboard_warns_when_control_uninitialized(self):
        html = bot.render_dashboard(
            [],
            {},
            bot._uninitialized_control(),
            {},
        )
        self.assertIn("ยังอ่านรายการเหรียญจาก Firebase ไม่ได้", html)
        self.assertIn("ไม่เขียนทับ", html)

    def test_health_server_is_threaded(self):
        self.assertTrue(issubclass(bot.ThreadingHTTPServer, bot.ThreadingMixIn))


if __name__ == "__main__":
    unittest.main()

