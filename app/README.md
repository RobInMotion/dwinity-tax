# Dwinity Tax — Frontend (App)

Static-Frontend für die Krypto-Steuer-App **dwinity-tax**. Pure HTML + CSS + Vanilla JS, gehostet hinter Nginx auf Hostinger VPS.

🌐 **Live:** https://app.tax.mkwt-strategy.tech
📦 **Backend-Repo:** [`dwinity-tax-api`](https://github.com/RobInMotion/dwinity-tax-api)

## Pages

| Datei | Pfad | Inhalt |
|---|---|---|
| `index.html` | `/` | Login (Magic-Link Email-Form) |
| `verify.html` | `/verify` | Token-Einlösung mit Loading-State |
| `dashboard.html` | `/dashboard` | App-Shell mit Tabs Imports / Wallets / Transaktionen / Steuerreport |
| `theme.css` | `/theme.css` | Cosmos Dark Theme (Glassmorphism, Cyan/Mint Neon) |
| `img/` | `/img/*` | Logos (transparent PNG, mehrere Größen) |

## Design-System (Cosmos Theme)

- **Background:** `#060614` mit subtilen Lila/Cyan/Mint Radial-Gradients + Dot-Grid Overlay
- **Text:** Inter (Body) · Space Grotesk (Headlines) · JetBrains Mono (Numbers / Code-Tags)
- **Akzente:** Cyan `#00d4ff` · Mint `#00ffa3` · Purple `#9d4dff` · Red `#ff5577` · Amber `#ffb547`
- **Cards:** Glassmorphism (`backdrop-filter: blur(20px) saturate(140%)`) mit subtle Top-Glow-Highlight
- **Glow-Effekte:** auf Logo, Buttons, Karten beim Hover, wichtige Numbers
- **Pills:** Mint/Rot/Lila Borders + matching Text Colors
- **Reduced-Motion:** Respektiert `prefers-reduced-motion: reduce`

## Deploy

Repo direkt nach `/var/www/dwinity-tax-app/`:

```bash
git pull
sudo chown -R www-data:www-data /var/www/dwinity-tax-app
# Kein Build-Schritt — Nginx liefert direkt aus
```

## Roadmap

Siehe [`../dwinity-tax-api/ROADMAP.md`](https://github.com/RobInMotion/dwinity-tax-api/blob/main/ROADMAP.md) (Frontend folgt der Backend-Phasen-Planung).

## Lizenz

Vertraulich · © 2026 Dwinity-Ecosystem · Teil der **Dwinity Blockchain Cloud**
