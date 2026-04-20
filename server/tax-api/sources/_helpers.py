"""Geteilte Hilfsfunktionen für CSV-Adapter."""
import csv
import io
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional, List, Dict

DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%dT%H:%M:%S+00:00",
    "%d.%m.%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
]

def parse_dt(s) -> Optional[datetime]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    # Unix-Timestamp (int oder float)
    if re.fullmatch(r"\d{10}(\.\d+)?", s):
        return datetime.fromtimestamp(float(s), tz=timezone.utc)
    if re.fullmatch(r"\d{13}", s):
        return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc)
    s_clean = s.rstrip("Z")
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s_clean, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    # ISO-Fallback
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return None

def parse_dec(s) -> Optional[Decimal]:
    if s is None:
        return None
    s = str(s).strip()
    if not s or s.lower() in ("-", "—", "n/a", "null", "none"):
        return None
    s = s.replace(",", "")
    m = re.match(r"^-?\d+(\.\d+)?(?:[eE][+-]?\d+)?", s)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except InvalidOperation:
        return None

def split_amount_with_unit(s) -> tuple:
    """'0.00012BTC' → (Decimal('0.00012'), 'BTC'). '0.5' → (Decimal('0.5'), None)."""
    if s is None:
        return (None, None)
    s = str(s).strip()
    if not s:
        return (None, None)
    m = re.match(r"^(-?\d+(?:\.\d+)?)\s*([A-Za-z]+)?$", s.replace(",", ""))
    if not m:
        return (parse_dec(s), None)
    return (parse_dec(m.group(1)), (m.group(2) or "").upper() or None)

def norm(s) -> str:
    return (s or "").strip().lower().replace("_", " ").replace("-", " ").replace(".", " ")

def sniff_csv(text: str):
    sample = text[:8192]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.excel

def read_rows(text: str) -> tuple:
    """Liefert (headers, list_of_dicts)."""
    dialect = sniff_csv(text)
    reader = csv.reader(io.StringIO(text), dialect=dialect)
    rows = [r for r in reader if any(c.strip() for c in r)]
    if not rows:
        return ([], [])
    headers = [h.strip().lstrip("\ufeff") for h in rows[0]]
    h_norm = [norm(h) for h in headers]
    out = []
    for r in rows[1:]:
        d = {}
        for i, h in enumerate(h_norm):
            d[h] = r[i].strip() if i < len(r) else ""
        out.append(d)
    return (h_norm, out)

def first_present(row: Dict, *keys, default=""):
    """Erster nicht-leerer Wert aus row für irgendeinen der Schlüssel."""
    for k in keys:
        v = row.get(norm(k), "")
        if v:
            return v
    return default

# Bekannte Quote-Currencies (längste zuerst!)
QUOTES = ["USDT", "BUSD", "USDC", "FDUSD", "TUSD", "DAI", "EUR", "GBP", "TRY", "BRL", "AUD",
          "CHF", "JPY", "CAD", "BTC", "ETH", "BNB", "XRP", "SOL", "USD"]

def split_pair(pair: str) -> Optional[tuple]:
    """BTCUSDT/BTC-USDT/BTC_USDT/BTC/USDT → ('BTC', 'USDT')."""
    p = (pair or "").upper().strip()
    if not p:
        return None
    # Schon getrennt?
    for sep in ("/", "-", "_"):
        if sep in p:
            parts = p.split(sep)
            if len(parts) == 2 and parts[0] and parts[1]:
                return (parts[0], parts[1])
    # Zusammengeschrieben → längste passende Quote
    for q in QUOTES:
        if p.endswith(q) and len(p) > len(q):
            return (p[: -len(q)], q)
    return None

# Kraken Asset-Mapping (Legacy "X" / "Z" Prefixes)
KRAKEN_ASSETS = {
    "XXBT": "BTC", "XBT": "BTC",
    "XETH": "ETH",
    "XLTC": "LTC",
    "XXLM": "XLM",
    "XXMR": "XMR",
    "XXRP": "XRP",
    "XZEC": "ZEC",
    "ZUSD": "USD",
    "ZEUR": "EUR",
    "ZGBP": "GBP",
    "ZJPY": "JPY",
    "ZCAD": "CAD",
    "ZAUD": "AUD",
}

def kraken_normalize_asset(a: str) -> str:
    a = (a or "").upper().strip()
    return KRAKEN_ASSETS.get(a, a)

def kraken_split_pair(pair: str) -> Optional[tuple]:
    """Kraken nutzt 'XXBTZUSD' oder 'XBTUSD' oder 'BTC/EUR'."""
    p = (pair or "").upper().strip()
    if "/" in p:
        a, b = p.split("/", 1)
        return (kraken_normalize_asset(a), kraken_normalize_asset(b))
    # 8-Zeichen Legacy: XXBTZUSD
    if len(p) == 8 and p[0] in "XZ" and p[4] in "XZ":
        return (kraken_normalize_asset(p[:4]), kraken_normalize_asset(p[4:]))
    # Kürzer: BTCUSD, ETHEUR etc.
    return split_pair(p)
