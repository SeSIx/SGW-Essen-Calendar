# SGW Essen Wasserball Kalender

Automatischer Kalender für alle SGW Essen Mannschaften — Spiele, Ergebnisse, Schiedsrichter und Spielorte direkt aus dem DSV-Portal.

## Kalender abonnieren

**Herren I + Herren II (alle Pflichtspiele):**
```
https://github.com/SeSIx/SGW-Essen-Calendar/raw/sgw-scraper-rewrite/sgw_termine.ics
```

### So geht's:
- **Android**: Google Calendar → ☰ → Einstellungen → Kalender hinzufügen → Über URL
- **iOS**: Einstellungen → Kalender → Accounts → Account hinzufügen → Andere → Kalenderabo
- **Outlook**: Kalender → Kalender hinzufügen → Aus dem Internet

## Benutzung

### Spiele scrapen (DSV-Daten aktualisieren)
```bash
pip install -r requirements.txt
python main.py
```

Erstellt/aktualisiert eine SQLite-DB pro Mannschaft in `output/` und schreibt anschließend automatisch `sgw_termine.ics`.

### Nur den kombinierten Kalender neu bauen
```bash
python combine.py
```

### Eigene Termine hinzufügen (Weihnachtsfeier, Trainingsausfall, …)
```bash
# Termin hinzufügen (interaktiv)
python combine.py --add-event

# Alle eigenen Termine anzeigen
python combine.py --list-events
```

Nach dem Hinzufügen einmal `python combine.py` ausführen, dann committen und pushen.

## Projektstruktur

| Datei | Zweck |
|---|---|
| `main.py` | Orchestrator: scrapt DSV, befüllt DBs, ruft combine auf |
| `scraper.py` | HTTP-Requests + HTML-Parsing (DSV-Portal) |
| `db.py` | SQLite-Schema + Upsert-Helfer |
| `ics.py` | RFC-5545-VCALENDAR-Generator (pro Mannschaft) |
| `combine.py` | Herren I + II + eigene Termine → `sgw_termine.db` + `sgw_termine.ics` |
| `config.py` | URLs, Club-ID, Season-Konstanten |

**Output (gitignored):**

| Datei | Inhalt |
|---|---|
| `output/sgw_essen_herren_1.db` | Herren I (NRW Verbandsliga + NRW Pokal) |
| `output/sgw_essen_herren_2.db` | Herren II (Ruhrgebietsliga) |
| `output/sgw_essen_damen.db` | Damen |
| `output/sgw_essen_u12.db` | U12 |
| `output/sgw_essen_u14.db` | U14 |
| `output/sgw_essen_u16.db` | U16 |
| `output/custom_events.db` | Eigene Termine (persistent, manuell gepflegt) |

**Committet (damit Abonnements aktuell bleiben):**

| Datei | Inhalt |
|---|---|
| `sgw_termine.ics` | Kombinierter Kalender (Herren I + II + eigene Termine) |

## Kalender aktualisieren

```bash
python main.py               # scrapen + alle ICS aktualisieren
git add sgw_termine.ics
git commit -m "Update Spielplan"
git push
```

Der abonnierte Kalender aktualisiert sich beim nächsten Sync der Kalender-App automatisch.
