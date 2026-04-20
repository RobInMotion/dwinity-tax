"""
Bitpanda Trade History CSV.

Typische Spalten:
  Transaction ID | Timestamp | Transaction Type | In/Out | Amount Fiat | Fiat |
  Amount Asset | Asset | Asset market price | Asset market price currency |
  Asset class | Product ID | Fee | Fee asset | Spread | Spread Currency

Transaction Type: buy, sell, deposit, withdrawal, transfer
In/Out: incoming = Asset kommt rein (BUY, REWARD), outgoing = raus (SELL, WITHDRAWAL)
"""
import json
from .base import ParsedTx, ParseResult, ParseError
from ._helpers import read_rows, parse_dt, parse_dec, first_present

def parse(text: str) -> ParseResult:
    # Bitpanda hängt Marketing-Zeilen oben dran
    lines = text.splitlines()
    start = 0
    for idx, ln in enumerate(lines[:30]):
        low = ln.lower()
        if "transaction id" in low or ("timestamp" in low and "asset" in low):
            start = idx
            break
    text2 = "\n".join(lines[start:])

    res = ParseResult()
    headers, rows = read_rows(text2)
    if not rows:
        raise ParseError("CSV enthält keine Datenzeilen.")

    for i, row in enumerate(rows, start=start + 2):
        ts_s = first_present(row, "timestamp", "time", "date")
        ts = parse_dt(ts_s)
        if not ts:
            res.skipped += 1
            continue

        ttype = first_present(row, "transaction type", "type").lower().strip()
        in_out = first_present(row, "in/out", "direction").lower().strip()

        # Mapping
        if ttype == "buy":
            kind = "BUY"
        elif ttype == "sell":
            kind = "SELL"
        elif ttype in ("deposit", "transfer") and in_out in ("incoming", "in"):
            kind = "DEPOSIT"
        elif ttype in ("withdrawal", "transfer") and in_out in ("outgoing", "out"):
            kind = "WITHDRAWAL"
        elif "reward" in ttype or "interest" in ttype or "savings" in ttype:
            kind = "REWARD"
        elif "staking" in ttype:
            kind = "STAKING"
        else:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Type '{ttype}' nicht abgebildet")
            continue

        asset = first_present(row, "asset").upper()
        amount = parse_dec(first_present(row, "amount asset", "amount"))
        fiat_amount = parse_dec(first_present(row, "amount fiat"))
        fiat = first_present(row, "fiat", "asset market price currency").upper() or "EUR"
        fee = parse_dec(first_present(row, "fee"))
        fee_asset = first_present(row, "fee asset").upper() or None

        if amount is None or amount == 0:
            res.skipped += 1
            continue

        eur_value = fiat_amount if fiat == "EUR" else None

        res.transactions.append(ParsedTx(
            ts=ts, kind=kind, asset=asset,
            amount=abs(amount),
            quote_asset=fiat if kind in ("BUY", "SELL") else None,
            quote_amount=abs(fiat_amount) if fiat_amount is not None else None,
            fee_asset=fee_asset,
            fee_amount=abs(fee) if fee is not None else None,
            eur_value=eur_value,
            source_meta=json.dumps(row, ensure_ascii=False),
        ))
    return res
