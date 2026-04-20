"""
Binance Spot Trade History Parser.

Erwartetes Format (typisch, Spalten variieren minimal über Zeit):
  Date(UTC) | Pair | Side | Price | Executed | Amount | Fee
oder
  Date | Market | Type | Price | Amount | Total | Fee

Pair = z.B. "BTCUSDT" → Asset=BTC, Quote=USDT
Executed/Amount = Menge des primären Assets
Amount/Total = Gegen-Wert in Quote
Fee = "0.00012BTC" oder "0.5USDT" — Asset im String enthalten
"""
import csv
import io
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

from .base import ParsedTx, ParseResult, ParseError

# Bekannte Quote-Currencies (längste zuerst — wichtig für Match!)
QUOTES = ["USDT", "BUSD", "USDC", "FDUSD", "TUSD", "EUR", "GBP", "TRY", "BRL", "AUD", "BTC", "ETH", "BNB", "XRP", "DAI", "USD"]

DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
]

def _parse_dt(s: str) -> Optional[datetime]:
    s = (s or "").strip().rstrip("Z")
    if not s:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None

def _parse_dec(s) -> Optional[Decimal]:
    if s is None:
        return None
    s = str(s).strip().replace(",", "")
    if not s:
        return None
    # entferne nicht-numerische Suffixe (z.B. "0.5USDT")
    m = re.match(r"^-?\d+(\.\d+)?", s)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except InvalidOperation:
        return None

def _split_pair(pair: str) -> Optional[tuple]:
    """BTCUSDT → ('BTC', 'USDT'). Liefert None wenn keine bekannte Quote passt."""
    p = (pair or "").upper().replace("-", "").replace("/", "").replace("_", "").strip()
    if not p:
        return None
    for q in QUOTES:
        if p.endswith(q) and len(p) > len(q):
            return (p[: -len(q)], q)
    return None

def _split_fee(fee_str: str) -> tuple:
    """'0.00012BTC' → (Decimal('0.00012'), 'BTC'). 0.5 ohne Asset → (Decimal('0.5'), None)."""
    s = (fee_str or "").strip()
    if not s:
        return (None, None)
    m = re.match(r"^(-?\d+(?:\.\d+)?)\s*([A-Za-z]+)?$", s.replace(",", ""))
    if not m:
        return (_parse_dec(s), None)
    amt = _parse_dec(m.group(1))
    asset = (m.group(2) or "").upper() or None
    return (amt, asset)

# Header-Mapping (case-insensitive, Whitespace-tolerant)
HEADER_ALIASES = {
    "date":     ["date(utc)", "date", "datetime", "time", "executed at", "datum"],
    "pair":     ["pair", "market", "symbol", "currency pair"],
    "side":     ["side", "type", "operation", "kind"],
    "price":    ["price", "avg price", "avg trading price", "execution price"],
    "executed": ["executed", "amount", "filled", "quantity", "qty"],
    "total":    ["total", "amount", "value", "quote amount"],
    "fee":      ["fee", "fees", "trading fee", "commission"],
}

def _norm(s: str) -> str:
    return (s or "").strip().lower().replace("_", " ")

def _resolve_columns(headers):
    h_norm = [_norm(h) for h in headers]
    out = {}
    for key, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            if alias in h_norm:
                out[key] = h_norm.index(alias)
                break
    return out

def parse(text: str) -> ParseResult:
    res = ParseResult()
    # CSV-Sniffer für Trennzeichen
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.reader(io.StringIO(text), dialect=dialect)
    rows = list(reader)
    if len(rows) < 2:
        raise ParseError("CSV enthält keine Datenzeilen.")
    headers = rows[0]
    cols = _resolve_columns(headers)
    required = ["date", "pair", "side"]
    missing = [k for k in required if k not in cols]
    if missing:
        raise ParseError(f"Spalten fehlen: {missing}. Gefunden: {headers}")

    has_executed = "executed" in cols
    has_total = "total" in cols
    has_price = "price" in cols
    has_fee = "fee" in cols

    for i, row in enumerate(rows[1:], start=2):
        if not row or all(not c.strip() for c in row):
            res.skipped += 1
            continue

        date_s = row[cols["date"]] if cols["date"] < len(row) else ""
        ts = _parse_dt(date_s)
        if not ts:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Datum nicht parsebar ('{date_s}')")
            continue

        pair_raw = row[cols["pair"]] if cols["pair"] < len(row) else ""
        split = _split_pair(pair_raw)
        if not split:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Pair nicht erkannt ('{pair_raw}')")
            continue
        asset, quote = split

        side_raw = (row[cols["side"]] if cols["side"] < len(row) else "").strip().upper()
        if side_raw in ("BUY", "KAUF", "B"):
            kind = "BUY"
        elif side_raw in ("SELL", "VERKAUF", "S"):
            kind = "SELL"
        else:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Unbekannte Seite '{side_raw}'")
            continue

        amount = _parse_dec(row[cols["executed"]]) if has_executed and cols["executed"] < len(row) else None
        total  = _parse_dec(row[cols["total"]])    if has_total    and cols["total"]    < len(row) else None
        price  = _parse_dec(row[cols["price"]])    if has_price    and cols["price"]    < len(row) else None

        # Wenn total fehlt aber price+amount da → ableiten
        if total is None and price is not None and amount is not None:
            total = price * amount

        if amount is None or amount == 0:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Menge fehlt")
            continue

        fee_amount = fee_asset = None
        if has_fee and cols["fee"] < len(row):
            fee_amount, fee_asset = _split_fee(row[cols["fee"]])

        eur_value = total if quote == "EUR" else None

        meta = {h: row[idx] for idx, h in enumerate(headers) if idx < len(row)}
        res.transactions.append(ParsedTx(
            ts=ts,
            kind=kind,
            asset=asset,
            amount=abs(amount),
            quote_asset=quote,
            quote_amount=abs(total) if total is not None else None,
            fee_asset=fee_asset,
            fee_amount=abs(fee_amount) if fee_amount is not None else None,
            eur_value=eur_value,
            source_meta=json.dumps(meta, ensure_ascii=False),
        ))
    return res
