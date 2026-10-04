import unittest
from unittest.mock import Mock, patch

from trove.common.v1 import source_pb2
from trove.greeks.v1 import greeks_pb2
from trove.instruments.v1 import instruments_pb2
from trove.market_data.v1 import market_data_pb2
from trove.positions.v1 import positions_pb2

from trove_export import export_greeks_csv as export


class CompatibilityTests(unittest.TestCase):
    def setUp(self):
        self.instrument = instruments_pb2.Instrument(
            id="opaque-id", name="EW C 7250.0 12/18/2026",
            underlying_group="EW", type=export.INST_FOPTION,
            option_right=export.OPT_CALL, strike=7250,
        )
        self.instrument.expiry.FromJsonString("2026-12-18T00:00:00Z")
        self.position = positions_pb2.Position(
            instrument_id=self.instrument.id, size=-12, fund=positions_pb2.FUND_LSOF_USD,
            source=source_pb2.Source(type=source_pb2.SOURCE_TYPE_EXEGY),
        )
        self.greeks = greeks_pb2.Greeks(
            instrument_id=self.instrument.id, delta=0.4, gamma=0.01,
            theta=-2, vega=5, vol_time=0.2, implied_volatility=0.25,
            underlying_price=7300,
        )

    def rows(self, market, greeks=True):
        iid = self.instrument.id
        return export.build_rows(
            [self.position], {iid: self.instrument},
            {iid: self.greeks} if greeks else {}, {iid: market},
        )

    def test_new_schemas_preserve_desk_row(self):
        market = market_data_pb2.MarketData(
            instrument_id=self.instrument.id,
            quote=market_data_pb2.Quote(bid_price=10, ask_price=12),
        )
        expected = [
            0, "EW C 7250.0 12/18/2026", "7250.0", "0.25", "0.25", "",
            "0.4", "0.4", "2026-12-18", "0.2", "5.0", "", "0.01", "-2.0",
            "", "10.0", "12.0", "-12", "Call", "", "", "", "7300.0",
            "0.0", "", "SOURCE_TYPE_EXEGY", "USD",
        ]
        self.assertEqual(self.rows(market), [expected])
        self.assertEqual(len(export.HEADER), len(expected))
        self.assertIn(b"Position Source,Fund\r\n", export.csv_bytes([expected]))

    def test_absent_quote_does_not_use_last_trade(self):
        market = market_data_pb2.MarketData(
            last_trade=market_data_pb2.LastTrade(price=123),
        )
        row = self.rows(market)[0]
        self.assertEqual(row[15:17], ["", ""])
        self.assertEqual(row[22], "7300.0")

    def test_future_midpoint_and_missing_greeks_defaults(self):
        self.instrument.type = export.INST_FUTURE
        self.instrument.name = "ES F 12/18/2026"
        self.instrument.underlying_group = "ES"
        self.position.size = -20
        market = market_data_pb2.MarketData(
            quote=market_data_pb2.Quote(bid_price=7000, ask_price=7002),
        )
        row = self.rows(market, greeks=False)[0]
        self.assertEqual(row[6:8], ["1.0", "1.0"])
        self.assertEqual([row[i] for i in (9, 10, 12, 13)], ["0.0"] * 4)
        self.assertEqual(row[17], "-20")
        self.assertEqual(row[22], "7001.0")
        market.quote.ask_price = 0
        self.assertEqual(self.rows(market)[0][22], "7300.0")

    def test_greeks_batch_uses_new_service_and_retries_missing_ids(self):
        client = Mock()
        client.greeks.get_greeks_batch.side_effect = [
            {"a": self.greeks}, {}, {"b": self.greeks}, {"c": self.greeks},
        ]
        with patch.object(export, "_GREEKS_CHUNK", 2):
            result = export._fetch_greeks_merged(client, ["a", "b", "c"])
        self.assertEqual(set(result), {"a", "b", "c"})
        self.assertEqual(
            [call.args[0] for call in client.greeks.get_greeks_batch.call_args_list],
            [["a", "b"], ["c"], ["b"], ["c"]],
        )
        client.market_data.get_greeks_batch.assert_not_called()

    def test_snapshot_filter_and_all_instruments(self):
        other = instruments_pb2.Instrument(id="other", underlying_group="NQ")
        client = Mock()
        client.positions.list.return_value = [
            self.position, positions_pb2.Position(instrument_id="other"),
        ]
        client.instruments.collect.return_value = [self.instrument, other]
        client.greeks.get_greeks_batch.side_effect = lambda ids: {i: self.greeks for i in ids}
        client.market_data.get_batch.side_effect = lambda ids: {
            i: market_data_pb2.MarketData(instrument_id=i) for i in ids
        }
        positions, instruments, greeks, market = export.load_snapshot(client)
        self.assertEqual(positions, [self.position])
        self.assertEqual(set(instruments), {self.instrument.id})
        self.assertEqual(set(greeks), set(market))
        positions, instruments, _, _ = export.load_snapshot(client, sp500_only=False)
        self.assertEqual(len(positions), 2)
        self.assertEqual(set(instruments), {self.instrument.id, "other"})


if __name__ == "__main__":
    unittest.main()
