"""
Export Trove positions + greeks + market data to a CSV matching the desk
greeks sheet column layout; uploads to S3 (no local file).

Trove's Greeks proto does not include wing sensitivities or ATM vol columns;
those cells are left empty. Model vol is filled from implied_volatility where present.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import re
from datetime import datetime, timezone

from trove import (
    filter_pb2,
    instrument_pb2,
    position_pb2,
)

from trove_export.trove_grpc_client import TroveGrpcClient

INST_IEOPTION = instrument_pb2.INSTRUMENT_TYPE_IEOPTION
INST_FOPTION = instrument_pb2.INSTRUMENT_TYPE_FOPTION
INST_FUTURE = instrument_pb2.INSTRUMENT_TYPE_FUTURE
INST_EQUITY = instrument_pb2.INSTRUMENT_TYPE_EQUITY
OPT_CALL = instrument_pb2.OPTION_RIGHT_CALL
OPT_PUT = instrument_pb2.OPTION_RIGHT_PUT

FUND_USD = 1
FUND_AUD = 2

_GREEKS_CHUNK = int(os.environ.get("TROVE_GREEKS_BATCH_CHUNK", "128"))
_MARKET_CHUNK = int(os.environ.get("TROVE_MARKET_BATCH_CHUNK", "128"))

# S&P 500 complex: E-mini / index / SPY (see Trove instrument.underlying_group + name)
_SP500_GROUPS_EXACT = frozenset(
    s.lower()
    for s in (
        "ES",
        "SPX",
        "SPXW",
        "SPY",
        "SS",  # SPDR S&P 500 ETF (IB-style group)
    )
)
_SP500_UG_EW = re.compile(r"^EW\d*$", re.IGNORECASE)
# Name fallback when group is missing or nonstandard
_SP500_NAME_PREFIX = re.compile(
    r"^(ES\s|EW\d+|SPXW|SPX|SPY)\b",
    re.IGNORECASE,
)


def _extra_sp500_groups() -> frozenset[str]:
    raw = os.environ.get("TROVE_SP500_EXTRA_GROUPS", "")
    if not raw.strip():
        return frozenset()
    return frozenset(x.strip().lower() for x in raw.split(",") if x.strip())


def is_sp500_related(inst) -> bool:
    """True for ES, EW*, SPX, SPXW, SPY, SS / SPDR S&P 500 ETF, etc."""
    ug = (inst.underlying_group or "").strip()
    if ug:
        u = ug.lower()
        if u in _SP500_GROUPS_EXACT or u in _extra_sp500_groups():
            return True
        if _SP500_UG_EW.match(ug):
            return True
    name = inst.name or ""
    nu = name.upper()
    if _SP500_NAME_PREFIX.match(name):
        return True
    if "SPDR" in nu and ("S&P" in name or "SP 500" in nu or "S&P 500" in name):
        return True
    return False


def _chunks(ids: list[str], size: int):
    for i in range(0, len(ids), size):
        yield ids[i : i + size]


def _fetch_greeks_merged(client: TroveGrpcClient, instrument_ids: list[str]) -> dict:
    merged: dict = {}
    for chunk in _chunks(instrument_ids, _GREEKS_CHUNK):
        merged.update(client.get_greeks_batch(instrument_ids=chunk))
    missing = [i for i in instrument_ids if i not in merged]
    for iid in missing:
        merged.update(client.get_greeks_batch(instrument_ids=[iid]))
    return merged


def _fetch_market_merged(client: TroveGrpcClient, instrument_ids: list[str]) -> dict:
    merged: dict = {}
    for chunk in _chunks(instrument_ids, _MARKET_CHUNK):
        merged.update(client.get_market_data_batch(instrument_ids=chunk))
    missing = [i for i in instrument_ids if i not in merged]
    for iid in missing:
        merged.update(client.get_market_data_batch(instrument_ids=[iid]))
    return merged


def _fund_label(fund: int) -> str:
    if fund == FUND_USD:
        return "USD"
    if fund == FUND_AUD:
        return "AUD"
    return f"?{fund}"


def _maturity_str(inst) -> str:
    """ISO maturity YYYY-MM-DD (matches desk export)."""
    try:
        if not inst.HasField("expiry"):
            return ""
        dt = inst.expiry.ToDatetime().replace(tzinfo=timezone.utc)
        return dt.date().isoformat()
    except Exception:
        return ""


def _expiry_mmddyyyy(inst) -> str:
    """US-style date for legacy short name (MM/DD/YYYY)."""
    try:
        if not inst.HasField("expiry"):
            return ""
        dt = inst.expiry.ToDatetime().replace(tzinfo=timezone.utc)
        return dt.strftime("%m/%d/%Y")
    except Exception:
        return ""


# Already in legacy desk format (same pattern as Exegy-style exports)
_LEGACY_SHORT_FUT = re.compile(
    r"^[A-Z0-9][A-Z0-9]* F \d{2}/\d{2}/\d{4}$",
)
_LEGACY_SHORT_OPT = re.compile(
    r"^[A-Z0-9][A-Z0-9]* [CP] \d+\.\d+ \d{2}/\d{2}/\d{4}$",
)


def _root_symbol(inst) -> str:
    ug = (inst.underlying_group or "").strip()
    if ug:
        return ug
    return (inst.trading_class or "").strip()


def _option_cp_letter(inst) -> str:
    if inst.option_right == OPT_CALL:
        return "C"
    if inst.option_right == OPT_PUT:
        return "P"
    return ""


def _strike_display(strike: float) -> str:
    """Strike with one decimal for whole numbers (e.g. 7250.0) like the reference CSV."""
    if strike is None or abs(float(strike)) < 1e-12:
        return "0.0"
    sf = float(strike)
    if abs(sf - round(sf)) < 1e-6:
        return f"{sf:.1f}"
    return repr(sf)


def instrument_short_name_legacy(inst) -> str:
    """
    Desk-style short name: ``ES F MM/DD/YYYY`` or ``EW3 C 7250.0 MM/DD/YYYY``.
    If Trove already uses that pattern (e.g. ``EW C 6810.0 03/31/2026``), keep it.
    """
    raw = (inst.name or "").strip()
    if raw and (_LEGACY_SHORT_FUT.match(raw) or _LEGACY_SHORT_OPT.match(raw)):
        return raw
    exp = _expiry_mmddyyyy(inst)
    if not exp:
        return raw or ""
    root = _root_symbol(inst)
    if not root:
        return raw or ""
    if inst.type == INST_FUTURE:
        return f"{root} F {exp}"
    if inst.type in (INST_IEOPTION, INST_FOPTION):
        cp = _option_cp_letter(inst)
        if not cp:
            return raw or ""
        sk = _strike_display(inst.strike)
        return f"{root} {cp} {sk} {exp}"
    return raw or ""


def _strike_cell(inst) -> str:
    if inst.type in (INST_IEOPTION, INST_FOPTION):
        return _strike_display(inst.strike) if inst.strike else ""
    if inst.strike and abs(inst.strike) > 1e-12:
        return _strike_display(inst.strike)
    return ""


def _cfi_variant(inst) -> str:
    if inst.type not in (INST_IEOPTION, INST_FOPTION):
        return ""
    if inst.option_right == OPT_CALL:
        return "Call"
    if inst.option_right == OPT_PUT:
        return "Put"
    return ""


def _is_futures_or_stock(inst) -> bool:
    return inst.type in (INST_FUTURE, INST_EQUITY)


def _call_delta(inst, gk) -> str:
    if not gk:
        return ""
    d = gk.delta
    if inst.type not in (INST_IEOPTION, INST_FOPTION):
        return _fmt_float(d)
    if inst.option_right == OPT_CALL:
        return _fmt_float(d)
    if inst.option_right == OPT_PUT:
        return _fmt_float(d + 1.0)
    return _fmt_float(d)


def _delta_columns(inst, gk) -> tuple[str, str]:
    """Futures and equity are delta-1; options use Trove greeks."""
    if _is_futures_or_stock(inst):
        return "1.0", "1.0"
    return (
        _fmt_float(gk.delta) if gk else "",
        _call_delta(inst, gk),
    )


def _fmt_float(x: float | None) -> str:
    """Full float text like the reference export (repr preserves precision / sci notation)."""
    if x is None:
        return ""
    xf = float(x)
    if xf == 0.0:
        return "0.0"
    return repr(xf)


def _fmt_optional_float(x: float | None) -> str:
    """Implied vol: omit when absent or zero (reference leaves blank)."""
    if x is None or x == 0.0:
        return ""
    return repr(float(x))


def _greek_field(gk, attr: str, inst) -> str:
    """Vol time / vega / gamma / theta: explicit 0.0 for delta-1 when greeks missing."""
    if gk:
        return _fmt_float(getattr(gk, attr, 0.0))
    if _is_futures_or_stock(inst):
        return "0.0"
    return ""


# Desk: USD ES future leg that is not the 250-lot anchor is shown 118 more short.
_ES_F_USD_NON_ANCHOR_OFFSET = -118.0
_ES_F_USD_ANCHOR_SIZE = 250


def _es_f_usd_fut_position_adjustment(pos, inst) -> float:
    if pos.fund != FUND_USD or inst.type != INST_FUTURE:
        return 0.0
    if _root_symbol(inst).strip().upper() != "ES":
        return 0.0
    s = float(pos.size)
    if int(round(s)) == _ES_F_USD_ANCHOR_SIZE:
        return 0.0
    return _ES_F_USD_NON_ANCHOR_OFFSET


def _position_cell(pos, inst) -> str:
    s = float(pos.size) + _es_f_usd_fut_position_adjustment(pos, inst)
    if abs(s - round(s)) < 1e-9:
        return str(int(round(s)))
    return repr(s)


def _instrument_underlying_price(
    inst,
    gk,
    bid: float | None,
    ask: float | None,
) -> str:
    """D1: midpoint of bid/ask when both sides quoted; else synthetic from greeks."""
    if _is_futures_or_stock(inst):
        if bid and ask and bid > 0 and ask > 0:
            return _fmt_float((float(bid) + float(ask)) / 2.0)
        su = gk.synthetic_underlying_price if gk else None
        return _fmt_float(su) if su and su > 0 else ""
    su = gk.synthetic_underlying_price if gk else None
    return _fmt_float(su) if su and su > 0 else ""


def load_snapshot(client: TroveGrpcClient, *, sp500_only: bool = True):
    positions = list(
        client.list_positions(position_filter=position_pb2.PositionFilter())
    )
    if not positions:
        return positions, {}, {}, {}

    ids = list({p.instrument_id for p in positions})
    instruments = client.list_instruments(
        instrument_filter=instrument_pb2.InstrumentFilter(
            ids=filter_pb2.StringFilter(includes=ids),
        )
    )
    inst_map = {i.id: i for i in instruments}
    if sp500_only:
        positions = [
            p
            for p in positions
            if inst_map.get(p.instrument_id)
            and is_sp500_related(inst_map[p.instrument_id])
        ]
        if not positions:
            return [], inst_map, {}, {}
        ids = list({p.instrument_id for p in positions})
        inst_map = {i: inst_map[i] for i in ids if i in inst_map}
    greeks = _fetch_greeks_merged(client, ids)
    market = _fetch_market_merged(client, ids)
    return positions, inst_map, greeks, market


def build_rows(positions, inst_map, greeks, market) -> list[list]:
    rows: list[list] = []

    def sort_key(p):
        inst = inst_map.get(p.instrument_id)
        name = instrument_short_name_legacy(inst) if inst else ""
        return (name, p.fund, p.instrument_id)

    for idx, pos in enumerate(sorted(positions, key=sort_key)):
        inst = inst_map.get(pos.instrument_id)
        if not inst:
            continue
        gk = greeks.get(pos.instrument_id)
        md = market.get(pos.instrument_id)

        iv = gk.implied_volatility if gk else None
        if iv and iv > 0:
            model_vol = float(iv)
        elif gk or _is_futures_or_stock(inst):
            model_vol = 0.0
        else:
            model_vol = None

        bid = md.bid_price if md else None
        ask = md.ask_price if md else None

        delta_cell, call_delta_cell = _delta_columns(inst, gk)

        row = [
            idx,
            instrument_short_name_legacy(inst),
            _strike_cell(inst),
            _fmt_optional_float(iv) if iv and iv > 0 else "",
            _fmt_float(model_vol) if model_vol is not None else "",
            "",  # Instrument ATM Vol
            delta_cell,
            call_delta_cell,
            _maturity_str(inst),
            _greek_field(gk, "vol_time", inst),
            _greek_field(gk, "vega", inst),
            "",  # Time Weighted Vega Adj
            _greek_field(gk, "gamma", inst),
            _greek_field(gk, "theta", inst),
            "",  # Fair Value
            _fmt_float(bid) if bid and bid > 0 else "",
            _fmt_float(ask) if ask and ask > 0 else "",
            _position_cell(pos, inst),
            _cfi_variant(inst),
            "",  # Wing ATM
            "",  # Wing Call
            "",  # Wing Put
            _instrument_underlying_price(inst, gk, bid, ask),
            "0.0",
            "",  # Implicit ATM Slope
            _fund_label(pos.fund),
        ]
        rows.append(row)
    return rows


HEADER = [
    "",
    "Instrument Short Name",
    "Instrument Strike",
    "Instrument Implied Vol",
    "Instrument Model Vol",
    "Instrument ATM Vol",
    "Instrument Delta",
    "Instrument Call Delta",
    "Instrument Maturity",
    "Instrument Volatility Time",
    "Instrument Vega",
    "Instrument Time Weighted Vega Adj",
    "Instrument Gamma",
    "Instrument Calendar Theta",
    "Instrument Fair Value",
    "Instrument Bid",
    "Instrument Ask",
    "Instrument Net Position",
    "Instrument CFI Variant Name",
    "Instrument Wing ATM Slope Sensitivity",
    "Instrument Wing Call Curvature Sensitivity",
    "Instrument Wing Put Curvature Sensitivity",
    "Instrument Underlying Price",
    "Instrument Underlying Offset",
    "Instrument Implicit ATM Slope",
    "Fund",
]


def csv_bytes(rows: list[list]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(HEADER)
    for row in rows:
        w.writerow(row)
    return buf.getvalue().encode("utf-8")


def default_export_filename() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"greeks_{ts}.csv"


def upload_csv_to_s3(s3_uri: str, filename: str, body: bytes) -> str:
    import boto3

    bucket, key_prefix = _parse_s3_uri(s3_uri)
    key = f"{key_prefix}{filename}"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="text/csv; charset=utf-8",
    )
    return f"s3://{bucket}/{key}"


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Return (bucket, key_prefix) for ``s3://bucket/path/to/prefix/``."""
    u = uri.strip()
    if not u.startswith("s3://"):
        raise ValueError(f"Not an s3 URI: {uri!r}")
    path = u[5:]
    if "/" not in path:
        return path, ""
    bucket, rest = path.split("/", 1)
    return bucket, rest


def prune_s3_prefix(s3_uri: str, max_files: int) -> int:
    """
    Ensure at most ``max_files`` objects exist under ``s3_uri`` by deleting
    the oldest objects (by LastModified, then key). Returns number deleted.
    """
    import boto3

    bucket, prefix = _parse_s3_uri(s3_uri)
    s3 = boto3.client("s3")
    keys_meta: list[tuple] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys_meta.append((obj["LastModified"], obj["Key"]))

    if len(keys_meta) <= max_files:
        return 0

    keys_meta.sort(key=lambda t: (t[0], t[1]))
    n_remove = len(keys_meta) - max_files
    to_delete = [k for _, k in keys_meta[:n_remove]]

    deleted = 0
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i : i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        deleted += len(batch)
    return deleted


def main():
    ap = argparse.ArgumentParser(description="Export Trove greeks CSV (desk format)")
    ap.add_argument(
        "--host",
        default=os.environ.get("TROVE_HOST", "trove-core-internal.laniakeafunds.com"),
    )
    ap.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("TROVE_PORT", "50051")),
    )
    ap.add_argument(
        "--s3-uri",
        default=os.environ.get(
            "TROVE_EXPORT_S3_URI", "s3://laniakea-trading/trove-exports/"
        ),
        help="S3 prefix for the CSV upload (default: s3://laniakea-trading/trove-exports/)",
    )
    ap.add_argument(
        "--s3-max-files",
        type=int,
        default=int(os.environ.get("TROVE_EXPORT_S3_MAX_FILES", "500")),
        help="After upload, delete oldest objects in --s3-uri until at most this many remain",
    )
    ap.add_argument(
        "--all-instruments",
        action="store_true",
        help="Do not restrict to S&P 500 (ES, EW*, SPX, SPXW, SPY, SS, SPDR S&P 500 ETF)",
    )
    args = ap.parse_args()

    filename = default_export_filename()
    client = TroveGrpcClient(host=args.host, port=args.port)
    positions, inst_map, greeks, market = load_snapshot(
        client, sp500_only=not args.all_instruments
    )
    rows = build_rows(positions, inst_map, greeks, market)
    body = csv_bytes(rows)
    base = args.s3_uri.rstrip("/") + "/"
    uri = upload_csv_to_s3(base, filename, body)
    print(f"Uploaded {len(rows)} rows to {uri}", flush=True)

    removed = prune_s3_prefix(base, max_files=args.s3_max_files)
    if removed:
        print(
            f"Pruned {removed} older object(s) under {base!r} (cap {args.s3_max_files})",
            flush=True,
        )


if __name__ == "__main__":
    main()
