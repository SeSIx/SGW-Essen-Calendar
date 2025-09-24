# SGW Essen Wasserball Kalender

🏊‍♂️ Einfacher Kalender für SG Wasserball Essen Termine

## 📱 Kalender abonnieren

**Direkt in Ihrer Kalender-App:**

```
https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_termine.ics
```

### So geht's:
- **Android**: Google Calendar → ☰ → Einstellungen → Kalender hinzufügen → Über URL
- **iOS**: Kalender → Kalender hinzufügen → Abonnement
- **Desktop**: Outlook/Thunderbird → Internetkalender hinzufügen

## 🎯 Termine hinzufügen

### Einzelner Termin:
```bash
python sgw_essen_scraper.py --add "20.12.2025" "15:00" "SGW Essen" "Weihnachtsfeier" "Weihnachtsmarkt" ""
```
*Format: DATUM ZEIT HEIM GAST ORT ERGEBNIS*

### Interaktive Eingabe:
```bash
python sgw_essen_scraper.py -new
```

### Termine anzeigen:
```bash
python sgw_essen_scraper.py --list
```

### Web-Scraping testen:
```bash
python sgw_essen_scraper.py --enable-scraping
```
*(Sobald die neue DSV-Website für 2025 online ist)*

## 🔄 Kalender aktualisieren

1. Termine hinzufügen (siehe oben)
2. Zu GitHub pushen:
   ```bash
   git add .
   git commit -m "Add new games"
   git push
   ```
3. **Fertig!** Kalender wird automatisch aktualisiert

## ⚙️ Installation

```bash
pip install -r requirements.txt
```

## 📋 Aktueller Status

- ✅ **Weihnachtsfeier**: 20.12.2025, 15:00 Uhr
- ✅ **Neue Saison**: Spiele ab November 2026
- ℹ️ **DSV-Website**: Noch nicht für neue Saison verfügbar

---

*Einfach und funktional! 🎉*
