# SGW Essen Wasserball Kalender

🏊‍♂️ Automatischer Kalender für SG Wasserball Essen Termine

## 📱 Kalender abonnieren

**Direkt in Ihrer Kalender-App hinzufügen:**

```
https://raw.githubusercontent.com/SeSIx/SGW-Essen-Calendar/main/sgw_termine.ics
```

### Android (Google Calendar):
1. Google Calendar öffnen
2. ☰ Menü → Einstellungen
3. "Kalender hinzufügen" → "Über URL"
4. URL einfügen → "Kalender hinzufügen"

### iOS (Apple Calendar):
1. Einstellungen → Kalender → Accounts
2. "Account hinzufügen" → "Andere"
3. "Kalenderabonnement hinzufügen"
4. URL einfügen

### Desktop:
- **Outlook**: Datei → Kontoeinstellungen → Internetkalender
- **Thunderbird**: Datei → Neu → Kalender → Im Netzwerk

## 🚀 Verwendung

### Termine scrapen:
```bash
python sgw_essen_scraper.py
```

### Termine anzeigen:
```bash
python sgw_essen_scraper.py --list
```

### Manuell Termine hinzufügen:
```bash
python sgw_essen_scraper.py -new
```

## 🔄 Updates

1. Script ausführen: `python sgw_essen_scraper.py`
2. Änderungen committen: `git add . && git commit -m "Update calendar"`
3. Push zu GitHub: `git push`
4. **Fertig!** Kalender wird automatisch aktualisiert

## ⚙️ Installation

```bash
pip install -r requirements.txt
```

## 📋 Features

- ✅ **18 Spiele automatisch** von DSV-Website scrapen
- ✅ **Alle Plattformen** (Android, iOS, Desktop)
- ✅ **Automatische Updates** via Git Push
- ✅ **Keine Duplikate** bei Re-Import
- ✅ **Einfach zu verwenden**

## 🗂️ Dateien

- `sgw_essen_scraper.py` - Hauptprogramm
- `sgw_termine.ics` - Generierte Kalenderdatei
- `sgw_termine.db` - Lokale Datenbank
- `requirements.txt` - Python Abhängigkeiten

---

*Automatisch generiert vom SGW Termine Scraper*