"""
Phemex Funding Fee History.

Typische Spalten:
  Time(UTC) | Contract | Funding Currency | Funding Fee | Funding Rate | Position Size

Funding Fee positiv = Spieler erhält → FUNDING_RECEIVED
Funding Fee negativ = Spieler zahlt   → FUNDING_PAID
"""
import json
from .base import ParsedTx, ParseResult, ParseError
from ._helpers import read_rows, parse_dt, parse_dec, first_present

def parse(text: str) -> ParseResult:
    res = ParseResult()
    headers, rows = read_rows(text)
    if not rows:
        raise ParseError("CSV enthält keine Datenzeilen.")

    for i, row in enumerate(rows, start=2):
        ts_s = first_present(row, "time(utc)", "time", "funding time", "settlement time")
        ts = parse_dt(ts_s)
        if not ts:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Datum nicht parsebar ('{ts_s}')")
            continue

        fee = parse_dec(first_present(row, "funding fee", "funding payment", "fee", "amount"))
        if fee is None:
            res.skipped += 1
            continue

        currency = first_present(row, "funding currency", "currency", "settlement currency") or "USDT"
        kind = "FUNDING_RECEIVED" if fee >= 0 else "FUNDING_PAID"

        meta = dict(row)
        meta["_contract"] = first_present(row, "contract", "symbol")

        res.transactions.append(ParsedTx(
            ts=ts,
            kind=kind,
            asset=currency.upper(),
            amount=abs(fee),
            quote_asset=None,
            quote_amount=None,
            fee_asset=None,
            fee_amount=None,
            eur_value=abs(fee) if currency.upper() == "EUR" else None,
            source_meta=json.dumps(meta, ensure_ascii=False),
        ))
    return res
