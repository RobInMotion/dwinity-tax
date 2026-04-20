"""
Coinbase / Coinbase Advanced Trade — Transaction History CSV.

Modernes Format:
  ID | Timestamp | Transaction Type | Asset | Quantity Transacted |
  Spot Price Currency | Spot Price at Transaction | Subtotal |
  Total (inclusive of fees and/or spread) | Fees and/or Spread | Notes

Transaction Types: Buy, Sell, Convert, Receive, Send, Reward Income, Staking Income, Learning Reward, Coinbase Earn
"""
import json
from .base import ParsedTx, ParseResult, ParseError
from ._helpers import read_rows, parse_dt, parse_dec, first_present

KIND_MAP = {
    "buy": "BUY",
    "advanced trade buy": "BUY",
    "sell": "SELL",
    "advanced trade sell": "SELL",
    "convert": "BUY",      # Konvertierung: 1. Leg behandeln; Quote = anderes Asset (komplex, vereinfacht)
    "receive": "DEPOSIT",
    "send": "WITHDRAWAL",
    "reward income": "REWARD",
    "staking income": "STAKING",
    "learning reward": "REWARD",
    "coinbase earn": "REWARD",
    "earn": "REWARD",
    "airdrop": "AIRDROP",
}

def parse(text: str) -> ParseResult:
    # Coinbase exportiert oft mit Marketing-Header-Zeilen über der eigentlichen Tabelle.
    # Wir suchen die Zeile mit "Timestamp" oder "Transaction Type" als Header-Anker.
    lines = text.splitlines()
    start = 0
    for idx, ln in enumerate(lines[:30]):
        low = ln.lower()
        if "timestamp" in low and ("transaction type" in low or "asset" in low):
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
        kind = KIND_MAP.get(ttype)
        if not kind:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Type '{ttype}' nicht abgebildet")
            continue

        asset = first_present(row, "asset", "currency").upper()
        amount = parse_dec(first_present(row, "quantity transacted", "quantity", "amount"))
        quote_asset = first_present(row, "spot price currency", "price/fee/total unit", "fee currency").upper() or None
        subtotal = parse_dec(first_present(row, "subtotal"))
        total    = parse_dec(first_present(row, "total (inclusive of fees and/or spread)", "total"))
        fees     = parse_dec(first_present(row, "fees and/or spread", "fees", "fee"))
        spot_price = parse_dec(first_present(row, "spot price at transaction"))

        # Quote-Wert bevorzugt aus Subtotal (vor Fees), sonst Total - Fee
        quote_amount = subtotal
        if quote_amount is None and total is not None:
            quote_amount = (total - fees) if fees is not None and kind == "BUY" else (total + (fees or 0)) if kind == "SELL" else total

        if amount is None or amount == 0:
            res.skipped += 1
            continue

        eur_value = None
        if quote_asset == "EUR":
            eur_value = quote_amount
        elif spot_price is not None and amount is not None and quote_asset == "EUR":
            eur_value = spot_price * amount

        res.transactions.append(ParsedTx(
            ts=ts, kind=kind, asset=asset,
            amount=abs(amount),
            quote_asset=quote_asset,
            quote_amount=abs(quote_amount) if quote_amount is not None else None,
            fee_asset=quote_asset,
            fee_amount=abs(fees) if fees is not None else None,
            eur_value=eur_value,
            source_meta=json.dumps(row, ensure_ascii=False),
        ))
    return res
