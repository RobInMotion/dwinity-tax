"""
Phemex Account Statement (TAXATION_*.csv).

Universal-Export aller Account-Bewegungen.

Spalten:
  UserId | ParentId | Time (UTC) | Account | Operation | Coin | Change | Remark

Operation-Mapping:
  Deposit              → DEPOSIT
  Withdraw/Withdrawal  → WITHDRAWAL
  Transfer(...)        → übersprungen (interner Account-Wechsel, steuerlich neutral)
  Funding Fee          → FUNDING_RECEIVED (Change > 0) / FUNDING_PAID (Change < 0)
  Realized PnL / Closed PnL → PNL_GAIN / PNL_LOSS
  Trading Fee          → FEE
  Bonus/Reward/Airdrop → REWARD / AIRDROP
  Spot Trade           → übersprungen (Pair fehlt → für Trades reine Spot-CSV nutzen)
"""
import json
from .base import ParsedTx, ParseResult, ParseError
from ._helpers import read_rows, parse_dt, parse_dec, first_present, norm

# Operation-Klassifikation (lowercase-Substring-Match)
def classify(op: str, change):
    o = (op or "").lower()
    if "fund" in o:                                  return "FUNDING_RECEIVED" if change >= 0 else "FUNDING_PAID"
    if "realized pnl" in o or "closed pnl" in o or "rpl" in o:
        return "PNL_GAIN" if change >= 0 else "PNL_LOSS"
    if "trading fee" in o or "trade fee" in o or o == "fee":  return "FEE"
    if "deposit" in o:                               return "DEPOSIT"
    if "withdraw" in o:                              return "WITHDRAWAL"
    if "airdrop" in o:                               return "AIRDROP"
    if "stak" in o:                                  return "STAKING"
    if "bonus" in o or "reward" in o or "earn" in o or "interest" in o or "saving" in o:
        return "REWARD"
    if "transfer" in o:                              return None  # internal, skip
    if "trade" in o:                                 return None  # ohne Pair nicht abbildbar
    if "liquidation" in o:                           return "PNL_LOSS"
    if "rebate" in o or "referral" in o or "commission" in o:
        return "REWARD"
    return None

def parse(text: str) -> ParseResult:
    res = ParseResult()
    headers, rows = read_rows(text)
    if not rows:
        raise ParseError("CSV enthält keine Datenzeilen.")

    # Sanity-Check: gefundene Headers loggen
    expected = {norm(h) for h in ("Time (UTC)", "Operation", "Coin", "Change")}
    missing = expected - set(headers)
    if missing:
        raise ParseError(f"Header-Spalten fehlen: {sorted(missing)}. Gefunden: {headers}")

    for i, row in enumerate(rows, start=2):
        ts = parse_dt(first_present(row, "time (utc)", "time(utc)", "time", "datetime"))
        if not ts:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Datum nicht parsebar")
            continue

        op    = first_present(row, "operation", "type")
        coin  = first_present(row, "coin", "currency", "asset").upper()
        change = parse_dec(first_present(row, "change", "amount", "delta"))
        if change is None or coin == "":
            res.skipped += 1
            continue

        kind = classify(op, change)
        if kind is None:
            res.skipped += 1
            res.warnings.append(f"Zeile {i}: Operation '{op}' übersprungen")
            continue

        meta = dict(row)
        meta["_account"] = first_present(row, "account")

        res.transactions.append(ParsedTx(
            ts=ts,
            kind=kind,
            asset=coin,
            amount=abs(change),
            quote_asset=None,
            quote_amount=None,
            fee_asset=None,
            fee_amount=None,
            eur_value=abs(change) if coin == "EUR" else None,
            source_meta=json.dumps(meta, ensure_ascii=False),
        ))

    if not res.transactions:
        raise ParseError(
            f"Keine verwertbaren Operationen gefunden. {res.skipped} Zeilen übersprungen. "
            f"Warnungen: {'; '.join(res.warnings[:5])}"
        )
    return res
