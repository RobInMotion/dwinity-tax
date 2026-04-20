"""
Kraken Trades CSV (`trades.csv` aus dem Account → History → Export).

Spalten:
  txid | ordertxid | pair | time | type | ordertype | price | cost | fee | vol | margin | misc | ledgers

pair ist im Kraken-Legacy-Format ("XXBTZEUR" → BTC/EUR).
"""
import json
from .base import ParsedTx, ParseResult, ParseError
from ._helpers import read_rows, parse_dt, parse_dec, kraken_split_pair, first_present

def parse(text: str) -> ParseResult:
    res = ParseResult()
    headers, rows = read_rows(text)
    if not rows:
        raise ParseError("CSV enthält keine Datenzeilen.")

    for i, row in enumerate(rows, start=2):
        ts_s = first_present(row, "time", "time(utc)", "datetime")
        ts = parse_dt(ts_s)
        if not ts:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Datum nicht parsebar")
            continue

        pair = first_present(row, "pair", "symbol", "market")
        split = kraken_split_pair(pair)
        if not split:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Pair '{pair}' nicht erkannt")
            continue
        asset, quote = split

        side_raw = first_present(row, "type", "side").lower()
        if side_raw in ("buy", "b"):
            kind = "BUY"
        elif side_raw in ("sell", "s"):
            kind = "SELL"
        else:
            res.skipped += 1
            continue

        vol  = parse_dec(first_present(row, "vol", "volume", "amount", "quantity"))
        cost = parse_dec(first_present(row, "cost", "total", "value"))
        fee  = parse_dec(first_present(row, "fee", "fees"))
        price= parse_dec(first_present(row, "price", "avg price"))

        if cost is None and price is not None and vol is not None:
            cost = price * vol
        if vol is None or vol == 0:
            res.skipped += 1
            continue

        eur_value = cost if quote == "EUR" else None

        res.transactions.append(ParsedTx(
            ts=ts, kind=kind, asset=asset,
            amount=abs(vol),
            quote_asset=quote,
            quote_amount=abs(cost) if cost is not None else None,
            fee_asset=quote if fee is not None else None,
            fee_amount=abs(fee) if fee is not None else None,
            eur_value=eur_value,
            source_meta=json.dumps(row, ensure_ascii=False),
        ))
    return res
