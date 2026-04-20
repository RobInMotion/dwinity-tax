"""
Dwinity Tax — Backend API.

Magic-Link Auth, SQLite-Persistierung, Storj-Integration als Stub.
Mail-Versand initial via Console (TAX_MAIL_MODE=console) — später SMTP/Brevo.
"""
import os
import secrets
import logging
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, List
from contextlib import asynccontextmanager

import boto3
from botocore.client import Config
from fastapi import FastAPI, HTTPException, Request, Response, Depends, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, String, DateTime, Integer, Numeric, ForeignKey, Index, select, delete, func
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

import sources as csv_sources
import etherscan_client
import wallet_sync

# ===== Config =====

SECRET_KEY    = os.environ["TAX_SECRET_KEY"]
DB_PATH       = os.environ.get("TAX_DB_PATH", "/opt/dwinity-tax-api/data/tax.db")
BASE_URL      = os.environ.get("TAX_BASE_URL", "http://localhost:5173")
MAIL_FROM     = os.environ.get("TAX_MAIL_FROM", "hello@dwinity.de")
MAIL_FROM_NAME= os.environ.get("TAX_MAIL_FROM_NAME", "Dwinity Tax")
MAIL_MODE     = os.environ.get("TAX_MAIL_MODE", "console")
SMTP_HOST     = os.environ.get("TAX_SMTP_HOST", "")
SMTP_PORT     = int(os.environ.get("TAX_SMTP_PORT", "465"))
SMTP_USER     = os.environ.get("TAX_SMTP_USER", "")
SMTP_PASS     = os.environ.get("TAX_SMTP_PASS", "")
SMTP_SSL      = os.environ.get("TAX_SMTP_SSL", "true").lower() in ("1", "true", "yes")

STORJ_BUCKET  = os.environ.get("STORJ_BUCKET", "dwinity-tax")

MAGIC_TTL_MIN = 15
SESSION_TTL_DAYS = 30
COOKIE_NAME   = "dt_session"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dwinity-tax")

# ===== DB =====

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id            = Column(String, primary_key=True)
    email         = Column(String, unique=True, nullable=False, index=True)
    created_at    = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_login_at = Column(DateTime, nullable=True)

class MagicLink(Base):
    __tablename__ = "magic_links"
    token      = Column(String, primary_key=True)
    email      = Column(String, nullable=False, index=True)
    expires_at = Column(DateTime, nullable=False)
    used_at    = Column(DateTime, nullable=True)

class Import(Base):
    __tablename__ = "imports"
    id                = Column(String, primary_key=True)
    user_id           = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    source            = Column(String, nullable=False)  # binance, kraken, coinbase, bitpanda
    original_filename = Column(String, nullable=False)
    storj_key         = Column(String, nullable=False)  # S3-Key für Roh-CSV
    uploaded_at       = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    transaction_count = Column(Integer, default=0)
    notes             = Column(String, nullable=True)
    transactions      = relationship("Transaction", cascade="all, delete-orphan", back_populates="imp")

class FxRate(Base):
    __tablename__ = "fx_rates"
    date     = Column(String, primary_key=True)   # YYYY-MM-DD
    currency = Column(String, primary_key=True)   # USD, GBP, JPY, ...
    rate     = Column(Numeric(18, 8), nullable=False)  # 1 EUR = rate * currency

class Transaction(Base):
    __tablename__ = "transactions"
    id           = Column(String, primary_key=True)
    user_id      = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    import_id    = Column(String, ForeignKey("imports.id"), nullable=False, index=True)
    ts           = Column(DateTime, nullable=False)            # UTC
    kind         = Column(String, nullable=False)
    asset        = Column(String, nullable=False)
    amount       = Column(Numeric(38, 18), nullable=False)
    quote_asset  = Column(String, nullable=True)
    quote_amount = Column(Numeric(38, 18), nullable=True)
    fee_asset    = Column(String, nullable=True)
    fee_amount   = Column(Numeric(38, 18), nullable=True)
    eur_value    = Column(Numeric(38, 18), nullable=True)
    source_meta  = Column(String, nullable=True)
    external_id  = Column(String, nullable=True, index=True)   # txhash:logindex bei Wallet-Imports → Dedup
    imp          = relationship("Import", back_populates="transactions")

    __table_args__ = (
        Index("ix_tx_user_ts", "user_id", "ts"),
        Index("ix_tx_user_external", "user_id", "external_id"),
    )

class Wallet(Base):
    __tablename__ = "wallets"
    id             = Column(String, primary_key=True)
    user_id        = Column(String, ForeignKey("users.id"), nullable=False, index=True)
    chain          = Column(String, nullable=False)            # ethereum, avalanche, polygon, bsc, arbitrum, optimism, base
    address        = Column(String, nullable=False)            # lowercase 0x...
    label          = Column(String, nullable=True)
    created_at     = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_synced_at = Column(DateTime, nullable=True)
    tx_count       = Column(Integer, default=0)

Base.metadata.create_all(engine)

# ===== Migration: external_id Spalte falls fehlt (SQLite ALTER TABLE) =====
from sqlalchemy import text as _sql_text
with engine.begin() as _conn:
    cols = _conn.execute(_sql_text("PRAGMA table_info(transactions)")).all()
    col_names = {c[1] for c in cols}
    if "external_id" not in col_names:
        _conn.execute(_sql_text("ALTER TABLE transactions ADD COLUMN external_id TEXT"))
        _conn.execute(_sql_text("CREATE INDEX IF NOT EXISTS ix_tx_user_external ON transactions(user_id, external_id)"))

def db():
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()

# ===== Storj =====

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["STORJ_ENDPOINT"],
    aws_access_key_id=os.environ["STORJ_ACCESS_KEY"],
    aws_secret_access_key=os.environ["STORJ_SECRET_KEY"],
    region_name=os.environ.get("STORJ_REGION", "eu1"),
    config=Config(signature_version="s3v4"),
)

# ===== Sessions (signed cookie) =====

serializer = URLSafeTimedSerializer(SECRET_KEY, salt="dwinity-tax-session")

def make_session_cookie(user_id: str) -> str:
    return serializer.dumps({"uid": user_id, "iat": int(datetime.now(timezone.utc).timestamp())})

def read_session_cookie(value: str) -> Optional[str]:
    try:
        data = serializer.loads(value, max_age=SESSION_TTL_DAYS * 86400)
        return data.get("uid")
    except (BadSignature, SignatureExpired):
        return None

def get_current_user(request: Request, s = Depends(db)) -> Optional[User]:
    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return None
    uid = read_session_cookie(cookie)
    if not uid:
        return None
    return s.get(User, uid)

# ===== Mail =====

def send_magic_link_email(email: str, link: str):
    subject = "Dein Login-Link für Dwinity Tax"
    text_body = f"""Hi,

klicke den folgenden Link, um dich bei Dwinity Tax anzumelden:

{link}

Der Link ist {MAGIC_TTL_MIN} Minuten gültig.
Wenn du diese E-Mail nicht angefordert hast, ignoriere sie einfach.

— Dwinity Tax
"""
    html_body = f"""<!doctype html>
<html><body style="margin:0;padding:32px 16px;background:#060614;font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#e8ecff;">
  <div style="max-width:480px;margin:0 auto;background:linear-gradient(135deg,rgba(157,77,255,0.08),rgba(0,212,255,0.06));border:1px solid rgba(255,255,255,0.08);border-radius:20px;padding:40px;">
    <div style="font-size:11px;letter-spacing:0.18em;text-transform:uppercase;color:#00d4ff;font-family:'JetBrains Mono',monospace;margin-bottom:16px;">// magic link</div>
    <div style="font-weight:600;font-size:20px;margin-bottom:8px;">
      <span style="color:#e8ecff;">dwinity</span><span style="background:linear-gradient(135deg,#00d4ff,#00ffa3);-webkit-background-clip:text;background-clip:text;color:transparent;">tax</span>
    </div>
    <h1 style="font-size:28px;font-weight:600;letter-spacing:-0.015em;margin:24px 0 12px;color:#e8ecff;line-height:1.15;">Sign in to your account</h1>
    <p style="color:rgba(232,236,255,0.62);line-height:1.6;margin:0 0 28px;font-size:14px;">
      Klick den Button unten, um dich anzumelden. Der Link ist <strong style="color:#00d4ff;">{MAGIC_TTL_MIN} Minuten</strong> gültig.
    </p>
    <p style="text-align:center;margin:0 0 28px;">
      <a href="{link}" style="display:inline-block;padding:14px 32px;background:linear-gradient(135deg,#00d4ff,#00ffa3);color:#060614;text-decoration:none;border-radius:10px;font-weight:700;font-size:15px;box-shadow:0 0 24px rgba(0,212,255,0.25);">Login → Dwinity Tax</a>
    </p>
    <p style="color:rgba(232,236,255,0.38);font-size:11px;line-height:1.5;margin:24px 0 0;border-top:1px solid rgba(255,255,255,0.06);padding-top:20px;font-family:'JetBrains Mono',monospace;word-break:break-all;">
      Fallback URL:<br>
      <a href="{link}" style="color:#00d4ff;text-decoration:none;">{link}</a>
    </p>
    <p style="color:rgba(232,236,255,0.38);font-size:11px;margin-top:20px;line-height:1.5;">
      Du hast diese Mail nicht angefordert? Dann ignoriere sie einfach — niemand kommt ohne den Link in deinen Account.
    </p>
    <div style="text-align:center;margin-top:32px;font-size:10px;color:rgba(232,236,255,0.3);font-family:'JetBrains Mono',monospace;letter-spacing:0.12em;">
      PART OF THE <span style="color:#00d4ff;">DWINITY</span> ECOSYSTEM
    </div>
  </div>
</body></html>"""

    if MAIL_MODE == "console":
        log.info("=== MAGIC LINK MAIL ===\nFrom: %s\nTo: %s\n\n%s", MAIL_FROM, email, text_body)
        return

    if MAIL_MODE == "smtp":
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = formataddr((MAIL_FROM_NAME, MAIL_FROM))
        msg["To"] = email
        msg.set_content(text_body)
        msg.add_alternative(html_body, subtype="html")

        ctx = ssl.create_default_context()
        try:
            if SMTP_SSL:
                with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=20) as s:
                    s.login(SMTP_USER, SMTP_PASS)
                    s.send_message(msg)
            else:
                with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
                    s.starttls(context=ctx)
                    s.login(SMTP_USER, SMTP_PASS)
                    s.send_message(msg)
            log.info("Magic-Link an %s gesendet via %s:%d", email, SMTP_HOST, SMTP_PORT)
        except Exception as e:
            log.error("SMTP-Fehler beim Senden an %s: %s", email, e)
            raise
        return

    log.warning("Mail mode %s not implemented yet", MAIL_MODE)

# ===== Models =====

class AuthRequest(BaseModel):
    email: EmailStr

class MeResponse(BaseModel):
    email: str
    created_at: datetime

# ===== Lifespan =====

@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Dwinity Tax API starting. DB=%s, Storj-Bucket=%s, MAIL_MODE=%s", DB_PATH, STORJ_BUCKET, MAIL_MODE)
    yield

app = FastAPI(title="Dwinity Tax API", lifespan=lifespan)

# Same-origin via Nginx → kein CORS nötig. Behalte als Sicherheitsnetz für Dev.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[BASE_URL, "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== Endpoints =====

@app.get("/api/health")
def health():
    return {"ok": True, "bucket": STORJ_BUCKET, "mail_mode": MAIL_MODE}

@app.post("/api/auth/request")
def auth_request(payload: AuthRequest, s = Depends(db)):
    email = payload.email.lower().strip()
    token = secrets.token_urlsafe(32)
    expires = datetime.now(timezone.utc) + timedelta(minutes=MAGIC_TTL_MIN)

    # Alte unverbrauchte Tokens für die Adresse aufräumen
    s.execute(delete(MagicLink).where(MagicLink.email == email, MagicLink.used_at.is_(None)))
    s.add(MagicLink(token=token, email=email, expires_at=expires))
    s.commit()

    link = f"{BASE_URL}/verify?token={token}"
    send_magic_link_email(email, link)
    return {"ok": True, "message": "Magic-Link wurde gesendet (oder ins Log geschrieben in Dev-Mode)."}

@app.get("/api/auth/verify")
def auth_verify(token: str, response: Response, s = Depends(db)):
    ml = s.get(MagicLink, token)
    now = datetime.now(timezone.utc)
    if not ml:
        raise HTTPException(status_code=400, detail="Token unbekannt oder bereits eingelöst.")
    # SQLite gibt naive datetimes zurück — als UTC interpretieren
    expires = ml.expires_at if ml.expires_at.tzinfo else ml.expires_at.replace(tzinfo=timezone.utc)
    if ml.used_at is not None:
        raise HTTPException(status_code=400, detail="Token bereits eingelöst.")
    if expires < now:
        raise HTTPException(status_code=400, detail="Token abgelaufen.")

    ml.used_at = now
    user = s.execute(select(User).where(User.email == ml.email)).scalar_one_or_none()
    if not user:
        user = User(id=secrets.token_urlsafe(16), email=ml.email)
        s.add(user)
    user.last_login_at = now
    s.commit()

    cookie = make_session_cookie(user.id)
    response.set_cookie(
        key=COOKIE_NAME,
        value=cookie,
        max_age=SESSION_TTL_DAYS * 86400,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )
    return {"ok": True, "email": user.email}

@app.post("/api/auth/logout")
def auth_logout(response: Response):
    response.delete_cookie(COOKIE_NAME, path="/")
    return {"ok": True}

@app.get("/api/me", response_model=MeResponse)
def me(user: Optional[User] = Depends(get_current_user)):
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    return MeResponse(email=user.email, created_at=user.created_at)

# ===== Imports & Transactions =====

MAX_CSV_BYTES = 20 * 1024 * 1024  # 20 MB

def require_user(user: Optional[User] = Depends(get_current_user)) -> User:
    if not user:
        raise HTTPException(status_code=401, detail="Nicht angemeldet")
    return user

def _dec_str(v):
    return None if v is None else str(v)

@app.get("/api/sources")
def get_sources():
    return csv_sources.list_sources()

@app.post("/api/imports")
async def upload_import(
    file: UploadFile = File(...),
    source: str = Form(...),
    user: User = Depends(require_user),
    s = Depends(db),
):
    raw = await file.read()
    if not raw:
        raise HTTPException(400, "Leere Datei.")
    if len(raw) > MAX_CSV_BYTES:
        raise HTTPException(413, f"CSV zu groß (max {MAX_CSV_BYTES // 1024 // 1024} MB).")

    # Decode (UTF-8 mit BOM-Tolerance, Fallback Latin-1)
    text = None
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            text = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise HTTPException(400, "Datei-Encoding nicht erkannt.")

    # Parsen
    try:
        result = csv_sources.parse(source, text)
    except csv_sources.ParseError as e:
        raise HTTPException(400, f"Parse-Fehler: {e}")
    except Exception as e:
        log.exception("Unerwarteter Parse-Fehler")
        raise HTTPException(500, f"Interner Fehler: {e}")

    if not result.transactions:
        raise HTTPException(400, "Keine Transaktionen gefunden. Warnings: " + "; ".join(result.warnings[:5]))

    # Storj-Upload (Roh-CSV als Audit-Trail)
    import_id = secrets.token_urlsafe(12)
    storj_key = f"users/{user.id}/imports/{import_id}.csv"
    try:
        s3.put_object(
            Bucket=STORJ_BUCKET,
            Key=storj_key,
            Body=raw,
            ContentType="text/csv",
            Metadata={"source": source, "user": user.id, "filename": file.filename or "import.csv"},
        )
    except Exception as e:
        log.exception("Storj-Upload fehlgeschlagen")
        raise HTTPException(502, f"Storj-Upload fehlgeschlagen: {e}")

    # DB-Insert
    imp = Import(
        id=import_id,
        user_id=user.id,
        source=source,
        original_filename=file.filename or "import.csv",
        storj_key=storj_key,
        transaction_count=len(result.transactions),
        notes=f"{len(result.warnings)} Warnungen, {result.skipped} übersprungen" if result.warnings or result.skipped else None,
    )
    s.add(imp)

    for tx in result.transactions:
        s.add(Transaction(
            id=secrets.token_urlsafe(12),
            user_id=user.id,
            import_id=import_id,
            ts=tx.ts.replace(tzinfo=None) if tx.ts.tzinfo else tx.ts,
            kind=tx.kind,
            asset=tx.asset,
            amount=tx.amount,
            quote_asset=tx.quote_asset,
            quote_amount=tx.quote_amount,
            fee_asset=tx.fee_asset,
            fee_amount=tx.fee_amount,
            eur_value=tx.eur_value,
            source_meta=tx.source_meta,
        ))
    s.commit()

    return {
        "id": import_id,
        "transaction_count": len(result.transactions),
        "skipped": result.skipped,
        "warnings": result.warnings[:10],
    }

@app.get("/api/imports")
def list_imports(user: User = Depends(require_user), s = Depends(db)):
    rows = s.execute(
        select(Import).where(Import.user_id == user.id).order_by(Import.uploaded_at.desc())
    ).scalars().all()
    return [{
        "id": r.id,
        "source": r.source,
        "filename": r.original_filename,
        "uploaded_at": r.uploaded_at.isoformat() if r.uploaded_at else None,
        "transaction_count": r.transaction_count,
        "notes": r.notes,
    } for r in rows]

@app.get("/api/imports/{import_id}")
def get_import(import_id: str, user: User = Depends(require_user), s = Depends(db)):
    imp = s.get(Import, import_id)
    if not imp or imp.user_id != user.id:
        raise HTTPException(404, "Import nicht gefunden.")
    return {
        "id": imp.id, "source": imp.source, "filename": imp.original_filename,
        "uploaded_at": imp.uploaded_at.isoformat() if imp.uploaded_at else None,
        "transaction_count": imp.transaction_count, "notes": imp.notes,
        "storj_key": imp.storj_key,
    }

@app.get("/api/imports/{import_id}/download")
def download_import(import_id: str, user: User = Depends(require_user), s = Depends(db)):
    from fastapi.responses import StreamingResponse
    imp = s.get(Import, import_id)
    if not imp or imp.user_id != user.id:
        raise HTTPException(404, "Import nicht gefunden.")
    try:
        obj = s3.get_object(Bucket=STORJ_BUCKET, Key=imp.storj_key)
    except Exception as e:
        raise HTTPException(502, f"Storj-Fehler: {e}")
    return StreamingResponse(
        obj["Body"].iter_chunks(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{imp.original_filename}"'},
    )

@app.get("/api/transactions/{tx_id}")
def get_transaction(tx_id: str, user: User = Depends(require_user), s = Depends(db)):
    t = s.get(Transaction, tx_id)
    if not t or t.user_id != user.id:
        raise HTTPException(404, "Transaktion nicht gefunden.")
    imp = s.get(Import, t.import_id)
    return {
        "id": t.id, "import_id": t.import_id,
        "ts": t.ts.isoformat() + "Z" if t.ts else None,
        "kind": t.kind, "asset": t.asset,
        "amount": _dec_str(t.amount),
        "quote_asset": t.quote_asset, "quote_amount": _dec_str(t.quote_amount),
        "fee_asset": t.fee_asset, "fee_amount": _dec_str(t.fee_amount),
        "eur_value": _dec_str(t.eur_value),
        "source_meta": t.source_meta,
        "import": {
            "source": imp.source if imp else None,
            "filename": imp.original_filename if imp else None,
            "uploaded_at": imp.uploaded_at.isoformat() if imp and imp.uploaded_at else None,
        } if imp else None,
    }

@app.delete("/api/imports/{import_id}")
def delete_import(import_id: str, user: User = Depends(require_user), s = Depends(db)):
    imp = s.get(Import, import_id)
    if not imp or imp.user_id != user.id:
        raise HTTPException(404, "Import nicht gefunden.")
    storj_key = imp.storj_key
    s.delete(imp)
    s.commit()
    try:
        s3.delete_object(Bucket=STORJ_BUCKET, Key=storj_key)
    except Exception as e:
        log.warning("Storj-Delete fehlgeschlagen für %s: %s", storj_key, e)
    return {"ok": True}

# ===== Wallet-Tracking (Phase A) =====

import re as _re

ETH_ADDR_RE = _re.compile(r"^0x[a-fA-F0-9]{40}$")

class WalletAdd(BaseModel):
    chain: str
    address: str
    label: Optional[str] = None

@app.get("/api/wallets/chains")
def wallets_chains():
    return etherscan_client.list_chains()

@app.get("/api/wallets")
def wallets_list(user: User = Depends(require_user), s = Depends(db)):
    rows = s.execute(
        select(Wallet).where(Wallet.user_id == user.id).order_by(Wallet.created_at.desc())
    ).scalars().all()
    return [{
        "id": w.id, "chain": w.chain, "address": w.address,
        "label": w.label, "tx_count": w.tx_count,
        "created_at": w.created_at.isoformat() if w.created_at else None,
        "last_synced_at": w.last_synced_at.isoformat() if w.last_synced_at else None,
    } for w in rows]

@app.post("/api/wallets")
def wallets_add(payload: WalletAdd, user: User = Depends(require_user), s = Depends(db)):
    addr = payload.address.lower().strip()
    if not ETH_ADDR_RE.match(addr):
        raise HTTPException(400, "Ungültige Adresse (erwartet: 0x… 40 hex chars).")
    if not etherscan_client.chain_info(payload.chain):
        raise HTTPException(400, f"Unbekannte Chain '{payload.chain}'.")
    # Duplikat?
    existing = s.execute(select(Wallet).where(
        Wallet.user_id == user.id, Wallet.chain == payload.chain, Wallet.address == addr
    )).scalar_one_or_none()
    if existing:
        raise HTTPException(409, "Wallet bereits hinzugefügt.")
    w = Wallet(
        id=secrets.token_urlsafe(12),
        user_id=user.id, chain=payload.chain, address=addr,
        label=payload.label or None, tx_count=0,
    )
    s.add(w)
    s.commit()
    return {"id": w.id}

@app.delete("/api/wallets/{wallet_id}")
def wallets_delete(wallet_id: str, user: User = Depends(require_user), s = Depends(db)):
    w = s.get(Wallet, wallet_id)
    if not w or w.user_id != user.id:
        raise HTTPException(404, "Wallet nicht gefunden.")
    # Auch alle assoziierten Transactions + den Pseudo-Import löschen
    pseudo_import_id = f"wallet:{w.id}"
    s.execute(delete(Transaction).where(
        Transaction.user_id == user.id, Transaction.import_id == pseudo_import_id
    ))
    imp = s.get(Import, pseudo_import_id)
    if imp:
        s.delete(imp)
    s.delete(w)
    s.commit()
    return {"ok": True}

@app.post("/api/wallets/{wallet_id}/sync")
def wallets_sync(wallet_id: str, user: User = Depends(require_user), s = Depends(db)):
    w = s.get(Wallet, wallet_id)
    if not w or w.user_id != user.id:
        raise HTTPException(404, "Wallet nicht gefunden.")

    try:
        txs = wallet_sync.sync_wallet(w.chain, w.address)
    except etherscan_client.EtherscanError as e:
        raise HTTPException(502, f"Etherscan: {e}")
    except Exception as e:
        log.exception("Wallet-Sync fehlgeschlagen")
        raise HTTPException(500, f"Sync-Fehler: {e}")

    pseudo_import_id = f"wallet:{w.id}"
    imp = s.get(Import, pseudo_import_id)
    if not imp:
        imp = Import(
            id=pseudo_import_id, user_id=user.id,
            source=f"{w.chain}_wallet",
            original_filename=f"{w.label or w.address[:10]}.{w.chain}",
            storj_key="", transaction_count=0,
            notes=f"Wallet-Sync: {w.chain} {w.address}",
        )
        s.add(imp)

    # Existierende external_ids für Dedup
    existing_eids = set(r[0] for r in s.execute(
        select(Transaction.external_id)
        .where(Transaction.user_id == user.id, Transaction.import_id == pseudo_import_id)
    ).all() if r[0])

    inserted = 0
    for tx in txs:
        eid = tx.get("external_id")
        if not eid or eid in existing_eids:
            continue
        s.add(Transaction(
            id=secrets.token_urlsafe(12),
            user_id=user.id,
            import_id=pseudo_import_id,
            ts=tx["ts"].replace(tzinfo=None) if tx["ts"].tzinfo else tx["ts"],
            kind=tx["kind"],
            asset=tx["asset"],
            amount=tx["amount"],
            quote_asset=tx.get("quote_asset"),
            quote_amount=tx.get("quote_amount"),
            fee_asset=tx.get("fee_asset"),
            fee_amount=tx.get("fee_amount"),
            eur_value=tx.get("eur_value"),
            source_meta=tx.get("source_meta"),
            external_id=eid,
        ))
        inserted += 1
        existing_eids.add(eid)

    w.last_synced_at = datetime.now(timezone.utc)
    w.tx_count = (w.tx_count or 0) + inserted
    imp.transaction_count = (imp.transaction_count or 0) + inserted
    s.commit()

    return {
        "ok": True, "fetched": len(txs), "inserted": inserted,
        "wallet": {"id": w.id, "tx_count": w.tx_count, "last_synced_at": w.last_synced_at.isoformat()},
    }

# ===== ECB-Wechselkurse (Phase 2.5) =====

import urllib.request
import zipfile
import io as _io_fx
import csv as _csv_fx

ECB_HIST_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"
# USDT/USDC ≈ 1 USD (für Steuerberichts-Zwecke akzeptabel)
USD_PROXIES = {"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "DAI", "USD"}

def refresh_ecb_rates(s) -> int:
    """Lädt ECB Historical Reference Rates (~600KB), schreibt fx_rates. Idempotent."""
    log.info("ECB-Rates: lade Historical CSV …")
    with urllib.request.urlopen(ECB_HIST_URL, timeout=30) as resp:
        zip_bytes = resp.read()
    with zipfile.ZipFile(_io_fx.BytesIO(zip_bytes)) as z:
        name = z.namelist()[0]
        text = z.read(name).decode("utf-8")

    reader = _csv_fx.reader(_io_fx.StringIO(text))
    rows = list(reader)
    if not rows:
        return 0
    headers = [h.strip() for h in rows[0]]  # Date, USD, JPY, BGN, ...
    inserted = 0
    # Bulk-Insert via ON CONFLICT DO NOTHING (SQLite UPSERT)
    for r in rows[1:]:
        if not r or not r[0].strip():
            continue
        date = r[0].strip()
        for i, currency in enumerate(headers[1:], start=1):
            if i >= len(r):
                continue
            v = r[i].strip()
            if not v or v in ("N/A", "-"):
                continue
            try:
                rate = Decimal(v)
            except Exception:
                continue
            existing = s.get(FxRate, (date, currency))
            if existing:
                existing.rate = rate
            else:
                s.add(FxRate(date=date, currency=currency, rate=rate))
                inserted += 1
    s.commit()
    log.info("ECB-Rates: %d neue Einträge", inserted)
    return inserted

def _normalize_currency(code: str) -> Optional[str]:
    if not code:
        return None
    c = code.upper()
    if c in USD_PROXIES:
        return "USD"
    if c == "EUR":
        return "EUR"
    return c  # JPY, GBP, ... wenn ECB hat

def get_eur_value(s, amount: Decimal, currency: str, dt: datetime) -> Optional[Decimal]:
    """Berechnet EUR-Wert. amount in 'currency', am Tag dt. None wenn keine Rate verfügbar."""
    if amount is None:
        return None
    norm = _normalize_currency(currency)
    if norm == "EUR":
        return amount
    if norm is None:
        return None
    date_str = dt.strftime("%Y-%m-%d") if dt else None
    if not date_str:
        return None
    # Exakter Tag oder nächster vorheriger Werktag (ECB hat keine Wochenend-Rates)
    row = s.execute(
        select(FxRate).where(
            FxRate.currency == norm,
            FxRate.date <= date_str,
        ).order_by(FxRate.date.desc()).limit(1)
    ).scalar_one_or_none()
    if not row or not row.rate or row.rate == 0:
        return None
    return amount / row.rate  # 1 EUR = row.rate USD → EUR = USD / row.rate

@app.post("/api/admin/refresh-rates")
def admin_refresh_rates(user: User = Depends(require_user), s = Depends(db)):
    n = refresh_ecb_rates(s)
    count = s.execute(select(func.count()).select_from(FxRate)).scalar_one()
    return {"inserted": n, "total_rates": count}

# ===== Steuerreport (Phase 2) =====

# Welche Kinds gehören zu welcher Steuer-Kategorie
KIND_GROUPS = {
    "perps":     ["PNL_GAIN", "PNL_LOSS"],
    "funding":   ["FUNDING_RECEIVED", "FUNDING_PAID"],
    "fees":      ["FEE"],
    "deposits":  ["DEPOSIT"],
    "withdrawals":["WITHDRAWAL"],
    "spot_buy":  ["BUY"],
    "spot_sell": ["SELL"],
    "rewards":   ["REWARD", "STAKING", "AIRDROP"],
}
NEGATIVE_KINDS = {"PNL_LOSS", "FUNDING_PAID", "FEE", "WITHDRAWAL", "SELL"}

def _signed_sum(items, kind):
    s = sum((float(it["total"]) for it in items if it["kind"] == kind), 0.0)
    return -s if kind in NEGATIVE_KINDS else s

@app.get("/api/report/years")
def report_years(user: User = Depends(require_user), s = Depends(db)):
    rows = s.execute(
        select(func.strftime("%Y", Transaction.ts).label("y"))
        .where(Transaction.user_id == user.id)
        .group_by("y").order_by("y")
    ).all()
    return {"years": [int(r[0]) for r in rows if r[0]]}

@app.get("/api/report/summary")
def report_summary(year: int = Query(...), user: User = Depends(require_user), s = Depends(db)):
    # ECB-Rates first-time-laden falls leer
    rate_count = s.execute(select(func.count()).select_from(FxRate)).scalar_one()
    if rate_count == 0:
        try:
            refresh_ecb_rates(s)
        except Exception as e:
            log.warning("ECB-Rates konnten nicht geladen werden: %s", e)

    # Pro-Transaktion EUR-Berechnung (für genaue Tages-Kurse)
    txs = s.execute(
        select(Transaction).where(
            Transaction.user_id == user.id,
            func.strftime("%Y", Transaction.ts) == str(year),
        )
    ).scalars().all()

    items_map = {}  # (kind, asset) → {total, count, eur_total}
    eur_unavailable_assets = set()
    for t in txs:
        key = (t.kind, t.asset)
        e = items_map.setdefault(key, {"total": Decimal(0), "count": 0, "eur_total": Decimal(0), "eur_partial": False})
        amt = t.amount or Decimal(0)
        e["total"] += amt
        e["count"] += 1
        eur = get_eur_value(s, amt, t.asset, t.ts)
        if eur is not None:
            e["eur_total"] += eur
        else:
            e["eur_partial"] = True
            eur_unavailable_assets.add(t.asset)

    items = [{
        "kind": k, "asset": a,
        "total": float(v["total"]),
        "count": v["count"],
        "eur_total": float(v["eur_total"]) if not v["eur_partial"] else None,
    } for (k, a), v in items_map.items()]

    # Pro Kind insgesamt (alle Assets summiert — funktioniert sauber wenn USDT-dominant)
    by_kind = {}
    for it in items:
        e = by_kind.setdefault(it["kind"], {"count": 0, "total_per_asset": {}})
        e["count"] += it["count"]
        e["total_per_asset"][it["asset"]] = e["total_per_asset"].get(it["asset"], 0.0) + it["total"]

    # Pro Asset
    by_asset = {}
    for it in items:
        a = by_asset.setdefault(it["asset"], {})
        a[it["kind"]] = a.get(it["kind"], 0.0) + it["total"]

    # Summary in Original-Currency (signed)
    def gross(kinds, signed=True):
        return {
            "received": sum(_signed_sum(items, k) for k in kinds if k not in NEGATIVE_KINDS),
            "paid":     sum(_signed_sum(items, k) for k in kinds if k in NEGATIVE_KINDS),
            "net":      sum(_signed_sum(items, k) for k in kinds),
        }

    summary = {
        "perps":       gross(KIND_GROUPS["perps"]),
        "funding":     gross(KIND_GROUPS["funding"]),
        "fees":        gross(KIND_GROUPS["fees"]),
        "deposits":    gross(KIND_GROUPS["deposits"]),
        "withdrawals": gross(KIND_GROUPS["withdrawals"]),
        "spot_buy":    gross(KIND_GROUPS["spot_buy"]),
        "spot_sell":   gross(KIND_GROUPS["spot_sell"]),
        "rewards":     gross(KIND_GROUPS["rewards"]),
    }

    # Total-Net (alle Kinds zusammen, signed)
    total_net = sum(b["net"] for b in summary.values())

    # EUR-Aggregation (nur für items wo eur_total verfügbar)
    def gross_eur(kinds):
        recv = paid = net = 0.0
        for k in kinds:
            for it in items:
                if it["kind"] != k or it["eur_total"] is None:
                    continue
                signed = -it["eur_total"] if k in NEGATIVE_KINDS else it["eur_total"]
                net += signed
                if k in NEGATIVE_KINDS: paid += signed
                else: recv += signed
        return {"received": recv, "paid": paid, "net": net}

    summary_eur = {key: gross_eur(KIND_GROUPS[key]) for key in KIND_GROUPS}
    total_net_eur = sum(b["net"] for b in summary_eur.values())

    return {
        "year": year,
        "summary": summary,
        "summary_eur": summary_eur,
        "total_net": total_net,
        "total_net_eur": total_net_eur,
        "by_kind": {k: {"count": v["count"], "total_per_asset": v["total_per_asset"]} for k, v in by_kind.items()},
        "by_asset": by_asset,
        "tx_count": sum(it["count"] for it in items),
        "eur_unavailable_assets": sorted(eur_unavailable_assets),
    }

@app.get("/api/report/export.csv")
def report_export(year: int = Query(...), user: User = Depends(require_user), s = Depends(db)):
    from fastapi.responses import StreamingResponse
    import csv as _csv
    import io as _io

    rows = s.execute(
        select(Transaction)
        .where(
            Transaction.user_id == user.id,
            func.strftime("%Y", Transaction.ts) == str(year),
        )
        .order_by(Transaction.ts)
    ).scalars().all()

    buf = _io.StringIO()
    w = _csv.writer(buf)
    w.writerow(["timestamp_utc", "kind", "asset", "amount", "quote_asset", "quote_amount", "fee_amount", "fee_asset", "eur_value", "ecb_rate_eur_to_asset", "import_id"])
    for t in rows:
        eur = get_eur_value(s, t.amount, t.asset, t.ts)
        rate = None
        if t.amount and eur and eur != 0:
            rate = float(t.amount) / float(eur) if eur != 0 else None
        w.writerow([
            (t.ts.isoformat() + "Z") if t.ts else "",
            t.kind,
            t.asset,
            str(t.amount) if t.amount is not None else "",
            t.quote_asset or "",
            str(t.quote_amount) if t.quote_amount is not None else "",
            str(t.fee_amount) if t.fee_amount is not None else "",
            t.fee_asset or "",
            f"{eur:.4f}" if eur is not None else (str(t.eur_value) if t.eur_value is not None else ""),
            f"{rate:.6f}" if rate else "",
            t.import_id,
        ])

    buf.seek(0)
    fname = f"dwinity-tax-{year}-{user.email.split('@')[0]}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )

@app.get("/api/transactions")
def list_transactions(
    import_id: Optional[str] = Query(None),
    kind: Optional[str] = Query(None),
    asset: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: User = Depends(require_user),
    s = Depends(db),
):
    q = select(Transaction).where(Transaction.user_id == user.id)
    if import_id: q = q.where(Transaction.import_id == import_id)
    if kind:      q = q.where(Transaction.kind == kind.upper())
    if asset:     q = q.where(Transaction.asset == asset.upper())
    q = q.order_by(Transaction.ts.desc()).offset(offset).limit(limit)
    rows = s.execute(q).scalars().all()

    total = s.execute(
        select(func.count()).select_from(Transaction).where(Transaction.user_id == user.id)
    ).scalar_one()

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "items": [{
            "id": t.id,
            "import_id": t.import_id,
            "ts": t.ts.isoformat() + "Z" if t.ts else None,
            "kind": t.kind,
            "asset": t.asset,
            "amount": _dec_str(t.amount),
            "quote_asset": t.quote_asset,
            "quote_amount": _dec_str(t.quote_amount),
            "fee_asset": t.fee_asset,
            "fee_amount": _dec_str(t.fee_amount),
            "eur_value": _dec_str(t.eur_value),
        } for t in rows],
    }
