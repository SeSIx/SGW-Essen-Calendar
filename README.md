# SGW Essen Wasserball Kalender

🏊‍♂️ Automatischer Kalender für SG Wasserball Essen Termine

## 🚀 Schnellstart

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Termine scrapen
```bash
python sgw_essen_scraper.py
```

### 3. Automatischer Upload (optional)
```bash
python github_uploader.py
```

## 📱 Kalender verwenden

Nach dem Setup haben Sie eine `sgw_termine.ics` Datei, die Sie in jede Kalender-App importieren können:

- **Android**: Google Calendar → Einstellungen → Kalender hinzufügen → Über URL
- **iOS**: Kalender → Kalender hinzufügen → Abonnement
- **Desktop**: Outlook, Thunderbird, etc.

## 🔧 Kommandos

| Kommando | Beschreibung |
|----------|-------------|
| `python sgw_essen_scraper.py` | Alle Termine scrapen und ICS erstellen |
| `python sgw_essen_scraper.py --list` | Termine anzeigen |
| `python sgw_essen_scraper.py -new` | Manuell Termine hinzufügen |
| `python github_uploader.py` | GitHub Pages Setup für automatischen Upload |

## ✨ Features

- ✅ **18 Spiele automatisch** von DSV-Website
- ✅ **Alle Plattformen** (Android, iOS, Desktop)
- ✅ **Automatische Updates** möglich
- ✅ **Keine Duplikate** bei Re-Import
- ✅ **Einfach zu verwenden**

## 🏗️ Dateien

- `sgw_essen_scraper.py` - Hauptprogramm
- `github_uploader.py` - Automatischer Upload zu GitHub Pages
- `sgw_termine.ics` - Generierte Kalenderdatei
- `sgw_termine.db` - Lokale Datenbank

Das war's! Einfach und sauber. 🎉