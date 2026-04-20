# Dwinity Tax — Monorepo

Krypto-Steuer-SaaS für Deutschland. Vertical-Produkt im Dwinity-Ecosystem.

🌐 **Landing:** https://tax.mkwt-strategy.tech
📊 **App:** https://app.tax.mkwt-strategy.tech

## Struktur

| Pfad | Inhalt | Live-Deploy |
|---|---|---|
| `index.html`, `impressum.html`, `datenschutz.html` | Landing-Page (Static) | `/var/www/dwinity-tax/` |
| `app/` | App-Frontend (Cosmos Dark Theme, Tabs Imports/Wallets/TX/Report) | `/var/www/dwinity-tax-app/` |
| `server/tax-api/` | FastAPI-Backend (Auth, Storj, Etherscan, ECB, Reports) | `/opt/dwinity-tax-api/` |
| `ROADMAP.md` | Phasen-Planung | — |

## Stack

| Schicht | Tech |
|---|---|
| Landing | HTML + Tailwind CDN |
| App-Frontend | HTML + Tailwind CDN + Vanilla JS, eigenes Theme (`theme.css`) |
| Backend | FastAPI + SQLAlchemy + SQLite |
| Storage | Storj DCS (S3-Gateway) für Roh-CSVs · SQLite für Metadaten |
| Auth | Magic-Link (Email) + Signed Session-Cookies |
| Mail | Hostinger SMTP |
| Blockchain | Etherscan v2 Multichain (7 Chains, ein API-Key) |
| FX | ECB Historical Reference Rates |
| Hosting | Hostinger VPS · Nginx · Let's Encrypt |

## Live-Phasen (Stand 2026-04-20)

- ✅ **Phase 0** — Auth + Storj + Service
- ✅ **Phase 1** — CSV-Import (Phemex Statement/Spot/Perps/Funding · Binance · Kraken · Coinbase · Bitpanda)
- ✅ **Phase 2** — Steuerreport mit Aggregation pro Jahr/Kind/Asset
- ✅ **Phase 2.5** — ECB-EUR-Umrechnung (USDT/USDC als USD-Proxy)
- ✅ **Phase A** — Wallet-Tracking (Ethereum, Avalanche, Polygon, BSC, Arbitrum, Optimism, Base)
- ✅ **Phase C** — Drill-Down Karte → Trade → Roh-CSV
- ✅ **UX-Pivot** — Cosmos Dark Theme (Glassmorphism, Cyan/Mint Neon)

Komplette Roadmap inkl. nächster Phasen: [`ROADMAP.md`](./ROADMAP.md)

## Deploy-Pipeline

Static (Landing + App):
```bash
git pull
sudo rsync -av --delete --exclude=app --exclude=server --exclude='*.md' . /var/www/dwinity-tax/
sudo rsync -av --delete app/ /var/www/dwinity-tax-app/
sudo chown -R www-data:www-data /var/www/dwinity-tax /var/www/dwinity-tax-app
```

API:
```bash
sudo rsync -av --delete --exclude=__pycache__ server/tax-api/ /opt/dwinity-tax-api/
sudo systemctl restart dwinity-tax-api
```

## Secrets (NICHT im Repo)

Alle Secrets liegen in `/etc/dwinity/tax.env` (mode 640, root:www-data).
Schema in [`server/tax-api/.env.example`](./server/tax-api/.env.example).

## Lizenz

Vertraulich · © 2026 Dwinity-Ecosystem · Teil der **Dwinity Blockchain Cloud**
