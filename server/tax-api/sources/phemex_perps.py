"""
Phemex Closed P&L (Perps).

Typische Spalten:
  Time(UTC) | Contract | Side | Order Type | Position Size | Entry Price | Closed Price |
  Realized PnL | Closed Size | Closed Value | Status | Settlement Currency

Output:
  kind=PNL_GAIN / PNL_LOSS, asset=Settlement-Currency (USDT/USD), amount=|PnL|.
  Close-Size + Contract werden in source_meta abgelegt.
"""
import json
from .base import ParsedTx, ParseResult, ParseError
from ._helpers import read_rows, parse_dt, parse_dec, split_pair, first_present

def parse(text: str) -> ParseResult:
    res = ParseResult()
    headers, rows = read_rows(text)
    if not rows:
        raise ParseError("CSV enthält keine Datenzeilen.")

    for i, row in enumerate(rows, start=2):
        ts_s = first_present(row, "time(utc)", "close time", "time", "closed time", "settlement time")
        ts = parse_dt(ts_s)
        if not ts:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Datum nicht parsebar ('{ts_s}')")
            continue

        pnl = parse_dec(first_present(row, "realized pnl", "realised pnl", "closed pnl", "pnl", "profit and loss"))
        if pnl is None:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Realized PnL fehlt")
            continue

        contract = first_present(row, "contract", "symbol", "instrument")
        # Settlement-Currency: explizit oder aus Pair ableiten (BTCUSDT → USDT)
        settle = first_present(row, "settlement currency", "settle currency", "currency")
        if not settle:
            split = split_pair(contract)
            settle = split[1] if split else "USDT"

        kind = "PNL_GAIN" if pnl >= 0 else "PNL_LOSS"

        meta = dict(row)
        meta["_contract"] = contract
        meta["_settle"] = settle

        res.transactions.append(ParsedTx(
            ts=ts,
            kind=kind,
            asset=settle.upper(),
            amount=abs(pnl),
            quote_asset=None,
            quote_amount=None,
            fee_asset=None,
            fee_amount=parse_dec(first_present(row, "trading fee", "fee", "commission")),
            eur_value=abs(pnl) if settle.upper() == "EUR" else None,
            source_meta=json.dumps(meta, ensure_ascii=False),
        ))
    return res
