# Dwinity Tax — Roadmap

Stand: **2026-04-20**

## ✅ Live

### Phase 0 · Infrastruktur
- [x] FastAPI-Backend (`/opt/dwinity-tax-api/`, port 8082)
- [x] SQLite + Auth (Magic-Link, signed Cookies, 30 d TTL)
- [x] Hostinger SMTP (`rm@mkwt-strategy.tech`)
- [x] Storj-Bucket `dwinity-tax` (S3-Gateway)
- [x] Nginx + SSL (Let's Encrypt) auf `app.tax.mkwt-strategy.tech`

### Phase 1 · CSV-Import
- [x] 8 Adapter: Phemex (Statement / Spot / Perps / Funding) · Binance · Kraken · Coinbase · Bitpanda
- [x] Storj-Backup der Roh-CSV pro Import
- [x] Pair-Splitter mit 16 Quote-Currencies, Fee-Asset-Detection, L/R-Vorzeichen für Side
- [x] Multi-Encoding (UTF-8 BOM / Latin-1 Fallback)

### Phase 2 · Steuerreport
- [x] Jahres-Aggregation pro Kind/Asset (BUY/SELL/PNL/FUNDING/FEE/REWARD/DEPOSIT/WITHDRAWAL)
- [x] CSV-Export pro Jahr
- [x] Hero-Karte mit Gesamtsaldo, pro-Kategorie-Karten mit Erklärungstext
- [x] Asset-Aufschlüsselung mit übersetzten Kind-Namen
- [x] Drill-Down von Karte → Trade-Liste → Trade-Detail (Phase C)
- [x] Original-CSV-Download aus Storj pro Trade

### Phase 2.5 · EUR-Umrechnung
- [x] ECB Historical Rates Cache (`fx_rates`-Tabelle)
- [x] Pro-Trade EUR-Bewertung mit Tageskurs
- [x] USDT/USDC/BUSD/FDUSD/TUSD/DAI als USD-Proxy
- [x] Wochenend-Trades nutzen letzten Werktagskurs
- [x] CSV-Export inkl. EUR-Wert + ECB-Rate

### Phase A · Wallet-Tracking
- [x] Etherscan v2 Multichain Client (1 API-Key für 7 Chains)
- [x] Native + ERC-20 Transfer Mapping → DEPOSIT/WITHDRAWAL
- [x] Smart-Contract-Calls als FEE markiert
- [x] Dedup via `external_id` (txhash:logindex)
- [x] Frontend-Tab mit Add/Sync/Delete

### UX
- [x] Cosmos Dark Theme (Glassmorphism, Cyan/Mint Neon, Space Grotesk + JetBrains Mono)
- [x] Transparentes Logo-PNG mit Glow + Pulse-Animation
- [x] Magic-Link-Mail im Cosmos-Look

---

## 🔜 Next (Phase B · Smart Contract-Klassifizierung)

Aus den heute gesammelten On-Chain-Daten **DeFi-Interaktionen erkennen** und besser klassifizieren:

- [ ] Uniswap-Router-Calls → BUY/SELL (statt nur DEPOSIT/WITHDRAWAL)
- [ ] Aave/Compound → STAKING / INTEREST
- [ ] Lido / RocketPool / Liquid Staking → STAKING + Token-Wrap
- [ ] Pendle / Morpho / Curve LP → LP_DEPOSIT / LP_WITHDRAWAL
- [ ] Token-Approvals separat tracken (kein Steuerereignis)
- [ ] Method-ID Whitelist mit ABIs der Top-100-DeFi-Contracts

## 📅 Phase D · Verlustvortrag-Tracker (Perps)

- [ ] §20 Abs. 6 EStG: 20.000 € Verlustverrechnungsgrenze pro Jahr
- [ ] Auto-Berechnung verbleibender Vortrag in Folgejahr
- [ ] Multi-Year-Vergleichsansicht
- [ ] Pop-up-Warnung bei Überschreitung der Grenze

## 📅 Phase E · DATEV-Export & PDF-Report

- [ ] DATEV-ASCII-Format (Spaltenset für Steuerberater-Software)
- [ ] PDF-Steuerreport mit Anlage SO / KAP-Empfehlungen
- [ ] Einzelne Aufzeichnung pro Veranlagungsjahr

## 📅 Phase F · FIFO §23 EStG (Spot)

- [ ] FIFO-Engine pro (User, Asset)
- [ ] Haltefrist 1 Jahr Tracking
- [ ] Freigrenze 1.000 € (ab 2024) automatisch berücksichtigt
- [ ] Realisierte vs. unrealisierte Gewinne

## 📅 Phase G · Stripe-Integration

- [ ] Pricing-Tiers (Free 25 TX · Basic 79 € · Pro 149 €)
- [ ] Steuerberater-Lizenz (499 € + 29 €/Mandant)
- [ ] Webhook-Handler für Subscription-Events
- [ ] Quota-Enforcement im API-Layer

## 📅 Phase H · Steuerberater-Modus

- [ ] Mandanten-Verwaltung
- [ ] Mehrere User unter Kanzlei-Account
- [ ] Berechtigungen / Read-Only-Sharing

## 🌌 Long-Term · Self-Custody-Migration

Sobald die **Dwinity-Cloud-API** live ist:
- [ ] Migration der User-Daten in den wallet-gebundenen Digital Twin
- [ ] SIWE-Login (Sign-in with Ethereum) als Auth-Alternative zu Magic-Link
- [ ] Speicherung verschlüsselt im fragmentierten Storage-Netzwerk
- [ ] Recovery ausschließlich per Seed-Phrase
