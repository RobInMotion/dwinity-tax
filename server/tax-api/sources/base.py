"""Gemeinsame Datenstrukturen für alle CSV-Adapter."""
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional, List

class ParseError(Exception):
    pass

@dataclass
class ParsedTx:
    ts:           datetime           # UTC
    kind:         str                # BUY, SELL, DEPOSIT, WITHDRAWAL, FEE, REWARD, AIRDROP, STAKING
    asset:        str
    amount:       Decimal            # immer >= 0
    quote_asset:  Optional[str] = None
    quote_amount: Optional[Decimal] = None
    fee_asset:    Optional[str] = None
    fee_amount:   Optional[Decimal] = None
    eur_value:    Optional[Decimal] = None
    source_meta:  Optional[str] = None  # JSON-String der Original-Zeile

@dataclass
class ParseResult:
    transactions: List[ParsedTx] = field(default_factory=list)
    skipped:      int = 0
    warnings:     List[str] = field(default_factory=list)
