"""
Wallet → Transaction Mapping.
Native + ERC-20 Transfers werden als DEPOSIT/WITHDRAWAL klassifiziert.
DeFi-Smart-Contract-Calls bleiben als-is markiert (Phase B).
"""
import json
import logging
import secrets
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict

import etherscan_client as es

log = logging.getLogger("wallet-sync")

def _ts(unix_str) -> datetime:
    return datetime.fromtimestamp(int(unix_str), tz=timezone.utc)

def _normalize_addr(addr: str) -> str:
    return (addr or "").lower().strip()

def _wei_to_dec(wei_str: str, decimals: int = 18) -> Decimal:
    try:
        return Decimal(wei_str) / (Decimal(10) ** decimals)
    except Exception:
        return Decimal(0)

def sync_wallet(chain_key: str, address: str) -> List[Dict]:
    """
    Holt alle Transaktionen einer Wallet von Etherscan und mappt sie auf unser
    Transaction-Schema (als Liste von Dicts, ready zum DB-Insert).
    """
    info = es.chain_info(chain_key)
    if not info:
        raise ValueError(f"Unbekannte Chain: {chain_key}")
    chainid, native_symbol, chain_label, explorer = info
    addr = _normalize_addr(address)
    out: List[Dict] = []

    # Native Transfers
    native = es.fetch_native_txs(chain_key, addr)
    log.info("Wallet %s/%s: %d native txs", chain_key, addr[:10], len(native))
    for tx in native:
        is_error = tx.get("isError") == "1"
        from_addr = _normalize_addr(tx.get("from", ""))
        to_addr   = _normalize_addr(tx.get("to", ""))
        value_wei = tx.get("value", "0")
        gas_used  = int(tx.get("gasUsed", "0"))
        gas_price = int(tx.get("gasPrice", "0"))
        fee = Decimal(gas_used * gas_price) / Decimal(10**18)

        amount = _wei_to_dec(value_wei)

        # Native value > 0
        if amount > 0 and not is_error:
            if to_addr == addr:
                kind = "DEPOSIT"
            elif from_addr == addr:
                kind = "WITHDRAWAL"
            else:
                continue
            out.append({
                "external_id": f"{tx['hash']}:native",
                "ts": _ts(tx["timeStamp"]),
                "kind": kind,
                "asset": native_symbol,
                "amount": amount,
                "fee_asset": native_symbol if from_addr == addr else None,
                "fee_amount": fee if from_addr == addr else None,
                "source_meta": json.dumps({
                    "chain": chain_label, "tx": tx["hash"],
                    "from": from_addr, "to": to_addr,
                    "block": tx.get("blockNumber"),
                    "explorer": f"{explorer}/tx/{tx['hash']}",
                }, ensure_ascii=False),
            })
        elif from_addr == addr and fee > 0 and not is_error:
            # Outgoing tx ohne value (z.B. Smart-Contract-Call) — nur Fee als Aufwand
            out.append({
                "external_id": f"{tx['hash']}:fee",
                "ts": _ts(tx["timeStamp"]),
                "kind": "FEE",
                "asset": native_symbol,
                "amount": fee,
                "source_meta": json.dumps({
                    "chain": chain_label, "tx": tx["hash"], "type": "smart-contract-call",
                    "to": to_addr, "method_id": tx.get("methodId"),
                    "explorer": f"{explorer}/tx/{tx['hash']}",
                }, ensure_ascii=False),
            })

    # ERC-20 Transfers
    erc20 = es.fetch_erc20_txs(chain_key, addr)
    log.info("Wallet %s/%s: %d ERC-20 txs", chain_key, addr[:10], len(erc20))
    for tx in erc20:
        from_addr = _normalize_addr(tx.get("from", ""))
        to_addr   = _normalize_addr(tx.get("to", ""))
        decimals  = int(tx.get("tokenDecimal", "18") or "18")
        value     = _wei_to_dec(tx.get("value", "0"), decimals)
        symbol    = (tx.get("tokenSymbol") or "?").upper()

        if value <= 0:
            continue
        if to_addr == addr:
            kind = "DEPOSIT"
        elif from_addr == addr:
            kind = "WITHDRAWAL"
        else:
            continue

        out.append({
            "external_id": f"{tx['hash']}:{tx.get('tokenID') or tx.get('contractAddress', '')}",
            "ts": _ts(tx["timeStamp"]),
            "kind": kind,
            "asset": symbol,
            "amount": value,
            "source_meta": json.dumps({
                "chain": chain_label, "tx": tx["hash"],
                "from": from_addr, "to": to_addr,
                "token_contract": tx.get("contractAddress"),
                "token_name": tx.get("tokenName"),
                "explorer": f"{explorer}/tx/{tx['hash']}",
            }, ensure_ascii=False),
        })

    return out
