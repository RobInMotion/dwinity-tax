# Dwinity Tax — Backend API

FastAPI-Service für die Krypto-Steuer-App **dwinity-tax**. Magic-Link-Auth, CSV-Import (8 Quellen), Wallet-Tracking via Etherscan v2 Multichain, Steuerreport mit ECB-EUR-Umrechnung. Roh-CSV-Backup verschlüsselt auf Storj DCS.

🌐 **Live:** https://app.tax.mkwt-strategy.tech
📦 **Frontend-Repo:** [`dwinity-tax-app`](https://github.com/RobInMotion/dwinity-tax-app)

## Stack

- **Python 3.10+** · FastAPI · SQLAlchemy · SQLite
- **Auth:** Magic-Link (Email) mit signed Session-Cookies (`itsdangerous`)
- **Storage:** SQLite (User/TX/Wallets) + Storj DCS S3 (Roh-CSVs)
- **Mail:** Hostinger SMTP
- **Blockchain:** Etherscan v2 Multichain API (Ethereum, Avalanche, Polygon, BSC, Arbitrum, Optimism, Base)
- **FX:** ECB Historical Reference Rates (täglicher EUR-Kurs)

## Deployment (auf Hostinger VPS)

- **Service:** systemd unit `dwinity-tax-api.service`, läuft als `www-data` auf `127.0.0.1:8082`
- **Working dir:** `/opt/dwinity-tax-api/`
- **Secrets:** `/etc/dwinity/tax.env` (mode 640, root:www-data)
- **DB:** SQLite unter `/opt/dwinity-tax-api/data/tax.db`
- **Storj-Bucket:** `dwinity-tax`
- **Reverse Proxy:** Nginx auf `app.tax.mkwt-strategy.tech` (SSL via Let's Encrypt)

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
# /etc/dwinity/tax.env mit allen Variablen anlegen (siehe .env.example)
sudo systemctl restart dwinity-tax-api
sudo journalctl -u dwinity-tax-api -f
```

### Environment-Variablen

| Variable | Beschreibung |
|---|---|
| `TAX_SECRET_KEY` | Random secret für Cookie-Signing (`token_urlsafe(48)`) |
| `TAX_DB_PATH` | Pfad zur SQLite-DB |
| `TAX_BASE_URL` | Public URL der App (für Magic-Link) |
| `TAX_MAIL_FROM`, `TAX_MAIL_FROM_NAME` | Absender |
| `TAX_MAIL_MODE` | `console` (dev) oder `smtp` |
| `TAX_SMTP_HOST/PORT/USER/PASS/SSL` | SMTP-Credentials |
| `STORJ_*` | Storj-S3-Gateway-Credentials + Bucket-Name |
| `TAX_ETHERSCAN_KEY` | Etherscan v2 API-Key (free tier reicht) |

## Module-Übersicht

| Datei | Verantwortung |
|---|---|
| `app.py` | FastAPI-App mit allen Routen + DB-Models |
| `sources/` | CSV-Adapter pro Börse (Phemex/Binance/Kraken/Coinbase/Bitpanda) |
| `etherscan_client.py` | Multi-Chain Etherscan v2 Wrapper |
| `wallet_sync.py` | Mapping On-Chain-Tx → Transaction-Schema |
| `requirements.txt` | Python-Dependencies (Pinned) |

## API-Endpoints (Übersicht)

```
GET    /api/health
POST   /api/auth/request    # Magic-Link
GET    /api/auth/verify
POST   /api/auth/logout
GET    /api/me

GET    /api/sources         # Liste der CSV-Quellen
POST   /api/imports         # CSV hochladen
GET    /api/imports
GET    /api/imports/{id}
GET    /api/imports/{id}/download   # Storj-Roh-CSV
DELETE /api/imports/{id}

GET    /api/transactions    # Filter: kind, asset, import_id
GET    /api/transactions/{id}

GET    /api/wallets/chains  # Unterstützte Chains
GET    /api/wallets
POST   /api/wallets
POST   /api/wallets/{id}/sync       # On-Chain-Pull
DELETE /api/wallets/{id}

GET    /api/report/years
GET    /api/report/summary?year=YYYY
GET    /api/report/export.csv?year=YYYY
POST   /api/admin/refresh-rates     # ECB neu laden
```

## Roadmap

Siehe [`ROADMAP.md`](./ROADMAP.md).

## Lizenz

Vertraulich · © 2026 Dwinity-Ecosystem · Teil der **Dwinity Blockchain Cloud**
