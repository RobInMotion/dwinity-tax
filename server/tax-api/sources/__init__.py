"""
CSV-Quellen-Registry. Jede Quelle implementiert parse(text) -> ParseResult.
"""
from typing import Callable, Dict, List
from .base import ParsedTx, ParseResult, ParseError
from . import binance, kraken, coinbase, bitpanda, phemex_spot, phemex_perps, phemex_funding, phemex_statement

REGISTRY: Dict[str, Callable[[str], ParseResult]] = {
    "binance":           binance.parse,
    "kraken":            kraken.parse,
    "coinbase":          coinbase.parse,
    "bitpanda":          bitpanda.parse,
    "phemex_statement":  phemex_statement.parse,
    "phemex_spot":       phemex_spot.parse,
    "phemex_perps":      phemex_perps.parse,
    "phemex_funding":    phemex_funding.parse,
}

def list_sources() -> List[Dict[str, str]]:
    return [
        {"key": "phemex_statement","label": "Phemex — Account Statement (TAXATION_*.csv)"},
        {"key": "phemex_spot",     "label": "Phemex — Spot Trades (separater Export)"},
        {"key": "phemex_perps",    "label": "Phemex — Closed P&L (Perps)"},
        {"key": "phemex_funding",  "label": "Phemex — Funding History (Detail-Export)"},
        {"key": "binance",         "label": "Binance — Spot Trade History"},
        {"key": "kraken",          "label": "Kraken — Trades CSV"},
        {"key": "coinbase",        "label": "Coinbase — Transaction History"},
        {"key": "bitpanda",        "label": "Bitpanda — Trade History"},
    ]

def parse(source: str, text: str) -> ParseResult:
    fn = REGISTRY.get(source)
    if not fn:
        raise ParseError(f"Quelle '{source}' nicht unterstützt.")
    return fn(text)

__all__ = ["parse", "list_sources", "ParsedTx", "ParseResult", "ParseError"]
