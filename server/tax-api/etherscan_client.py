"""
Etherscan v2 Multichain API Client.
Ein API-Key für 50+ Chains. Free tier: 5 calls/sec, 100k/day.
"""
import os
import time
import logging
import urllib.parse
import urllib.request
import json
from typing import List, Dict, Optional

log = logging.getLogger("etherscan")

ETHERSCAN_BASE = "https://api.etherscan.io/v2/api"
API_KEY = os.environ.get("TAX_ETHERSCAN_KEY", "")

# chain_key → (chainid, native_symbol, label, explorer_url)
CHAINS = {
    "ethereum":  (1,     "ETH",   "Ethereum",     "https://etherscan.io"),
    "avalanche": (43114, "AVAX",  "Avalanche",    "https://snowtrace.io"),
    "polygon":   (137,   "POL",   "Polygon",      "https://polygonscan.com"),
    "bsc":       (56,    "BNB",   "BSC",          "https://bscscan.com"),
    "arbitrum":  (42161, "ETH",   "Arbitrum One", "https://arbiscan.io"),
    "optimism":  (10,    "ETH",   "Optimism",     "https://optimistic.etherscan.io"),
    "base":      (8453,  "ETH",   "Base",         "https://basescan.org"),
}

class EtherscanError(Exception):
    pass

# Simple rate limiter — 4 calls/sec to stay under free tier
_last_call = [0.0]
_MIN_INTERVAL = 0.25

def _throttle():
    now = time.time()
    delta = now - _last_call[0]
    if delta < _MIN_INTERVAL:
        time.sleep(_MIN_INTERVAL - delta)
    _last_call[0] = time.time()

def _call(chainid: int, params: Dict) -> List[Dict]:
    if not API_KEY:
        raise EtherscanError("TAX_ETHERSCAN_KEY ist nicht gesetzt.")
    _throttle()
    q = {"chainid": chainid, "apikey": API_KEY, **params}
    url = ETHERSCAN_BASE + "?" + urllib.parse.urlencode(q)
    log.debug("Etherscan call: %s", url.replace(API_KEY, "***"))
    try:
        with urllib.request.urlopen(url, timeout=20) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        raise EtherscanError(f"Network: {e}")
    status = str(data.get("status", "0"))
    if status != "1":
        msg = data.get("message", "")
        result = data.get("result", "")
        # "No transactions found" = leeres Result, kein Fehler
        if "no transactions" in str(result).lower() or "no transactions" in str(msg).lower():
            return []
        raise EtherscanError(f"API: {msg} — {result}")
    result = data.get("result", [])
    return result if isinstance(result, list) else []

def list_chains() -> List[Dict]:
    return [
        {"key": k, "chainid": v[0], "native": v[1], "label": v[2], "explorer": v[3]}
        for k, v in CHAINS.items()
    ]

def chain_info(chain_key: str):
    return CHAINS.get(chain_key)

def fetch_native_txs(chain_key: str, address: str, start_block: int = 0, max_pages: int = 5) -> List[Dict]:
    info = CHAINS.get(chain_key)
    if not info:
        raise EtherscanError(f"Unbekannte Chain: {chain_key}")
    out = []
    for page in range(1, max_pages + 1):
        rows = _call(info[0], {
            "module": "account", "action": "txlist", "address": address,
            "startblock": start_block, "endblock": 99999999,
            "page": page, "offset": 1000, "sort": "asc",
        })
        out.extend(rows)
        if len(rows) < 1000:
            break
    return out

def fetch_erc20_txs(chain_key: str, address: str, start_block: int = 0, max_pages: int = 5) -> List[Dict]:
    info = CHAINS.get(chain_key)
    if not info:
        raise EtherscanError(f"Unbekannte Chain: {chain_key}")
    out = []
    for page in range(1, max_pages + 1):
        rows = _call(info[0], {
            "module": "account", "action": "tokentx", "address": address,
            "startblock": start_block, "endblock": 99999999,
            "page": page, "offset": 1000, "sort": "asc",
        })
        out.extend(rows)
        if len(rows) < 1000:
            break
    return out

def fetch_internal_txs(chain_key: str, address: str, start_block: int = 0, max_pages: int = 3) -> List[Dict]:
    """Internal transactions (z.B. DEX-Refunds, Smart-Contract-Aufrufe)."""
    info = CHAINS.get(chain_key)
    if not info:
        raise EtherscanError(f"Unbekannte Chain: {chain_key}")
    out = []
    for page in range(1, max_pages + 1):
        rows = _call(info[0], {
            "module": "account", "action": "txlistinternal", "address": address,
            "startblock": start_block, "endblock": 99999999,
            "page": page, "offset": 1000, "sort": "asc",
        })
        out.extend(rows)
        if len(rows) < 1000:
            break
    return out
