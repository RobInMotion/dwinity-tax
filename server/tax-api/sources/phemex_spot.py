"""
Phemex Spot Trade History.

Typische Spalten (variiert nach Region/Update):
  Time | Symbol | Side | Type | Order Price | Order Qty | Avg Price | Total | Trading Fee | Status
oder
  Time(UTC) | Pair | Side | Order Type | Price | Filled Qty | Filled Total | Fee | Status

Symbol-Format: "sBTCUSDT" (Spot-Prefix) oder "BTC/USDT".
"""
import json
from .base import ParsedTx, ParseResult, ParseError
from ._helpers import read_rows, parse_dt, parse_dec, split_pair, split_amount_with_unit, first_present

def parse(text: str) -> ParseResult:
    res = ParseResult()
    headers, rows = read_rows(text)
    if not rows:
        raise ParseError("CSV enthält keine Datenzeilen.")

    for i, row in enumerate(rows, start=2):
        ts_s = first_present(row, "time(utc)", "time", "datetime", "trade time", "execution time")
        ts = parse_dt(ts_s)
        if not ts:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Datum nicht parsebar ('{ts_s}')")
            continue

        sym = first_present(row, "symbol", "pair", "market", "instrument")
        # Phemex-Spot-Prefix abschneiden (z.B. "sBTCUSDT" → "BTCUSDT")
        sym_clean = sym[1:] if sym and sym[0].islower() else sym
        split = split_pair(sym_clean)
        if not split:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Symbol nicht erkannt ('{sym}')")
            continue
        asset, quote = split

        side_raw = first_present(row, "side", "type", "direction").upper()
        if side_raw in ("BUY", "B", "LONG"):
            kind = "BUY"
        elif side_raw in ("SELL", "S", "SHORT"):
            kind = "SELL"
        else:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Unbekannte Seite '{side_raw}'")
            continue

        amount = parse_dec(first_present(row, "filled qty", "executed qty", "order qty", "amount", "quantity", "filled", "qty", "size"))
        total  = parse_dec(first_present(row, "filled total", "total", "value", "quote amount", "total quantity"))
        price  = parse_dec(first_present(row, "avg price", "avg trading price", "price", "execution price", "order price"))

        if total is None and price is not None and amount is not None:
            total = price * amount
        if amount is None or amount == 0:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Menge fehlt")
            continue

        fee_raw = first_present(row, "trading fee", "fee", "fees", "commission")
        fee_amt, fee_asset = split_amount_with_unit(fee_raw)
        # Wenn Fee ohne Unit → vermute Quote-Currency (Phemex-Standard)
        if fee_amt is not None and not fee_asset:
            fee_asset = quote

        eur_value = total if quote == "EUR" else None

        res.transactions.append(ParsedTx(
            ts=ts, kind=kind, asset=asset,
            amount=abs(amount),
            quote_asset=quote,
            quote_amount=abs(total) if total is not None else None,
            fee_asset=fee_asset,
            fee_amount=abs(fee_amt) if fee_amt is not None else None,
            eur_value=eur_value,
            source_meta=json.dumps(row, ensure_ascii=False),
        ))
    return res
