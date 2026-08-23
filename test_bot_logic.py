"""Unit tests for Sentinel bot helpers — no API / Firebase required."""
import sys
import types
import unittest


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

    class _DB:
        @staticmethod
        def reference(_path):
            return _Ref()

    fa.credentials = _Cred
    fa.db = _DB
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


class SafeFloatTests(unittest.TestCase):
    def test_none_and_blank(self):
        self.assertEqual(bot._safe_float(None, 0.0), 0.0)
        self.assertEqual(bot._safe_float("", 0.0), 0.0)
        self.assertIsNone(bot._safe_float(None))

    def test_numeric_string(self):
        self.assertEqual(bot._safe_float("1.5"), 1.5)


if __name__ == "__main__":
    unittest.main()
