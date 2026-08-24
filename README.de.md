# SGW Essen Wasserball-Kalender

Alle Spiele, Ergebnisse, Spielorte und Schiedsrichter der SG Wasserball Essen —
automatisch aus dem DSV-Portal geholt und als Kalender-Abo bereitgestellt.

**[English README](README.md)**

## Kalender abonnieren

Die URL als **Abo** eintragen (nicht als Import) — dann aktualisiert sich der
Kalender von selbst, sobald sich Ansetzungen ändern oder Ergebnisse feststehen.

| Kalender | URL |
|---|---|
| **Herren I + II** (Standard) | `https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_termine.ics` |
| Herren I | `https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_essen_herren_1.ics` |
| Herren II | `https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_essen_herren_2.ics` |
| Damen | `https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_essen_damen.ics` |
| U16 | `https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_essen_u16.ics` |
| U14 | `https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_essen_u14.ics` |
| U12 | `https://github.com/SeSIx/SGW-Essen-Calendar/raw/main/sgw_essen_u12.ics` |

Im Standard-Kalender stehen zusätzlich die Vereinstermine (Mannschaftsbesprechung,
Trainingsstart, Kampfrichter-Ausbildung), die es in den DSV-Daten nicht gibt.

### So geht's

- **Android:** Google Kalender → ☰ → Einstellungen → Kalender hinzufügen → Über URL
- **iPhone/iPad:** Einstellungen → Apps → Kalender → Accounts → Account hinzufügen →
  Andere → Kalenderabo hinzufügen
- **Outlook:** Kalender → Kalender hinzufügen → Aus dem Internet
- **Apple Kalender (Mac):** Ablage → Neues Kalenderabo

Jeder Eintrag enthält das Ergebnis, sobald gespielt wurde, die vollständige Adresse
der Schwimmhalle (damit die Navigation direkt losfahren kann), die Schiedsrichter
und einen Link zur DSV-Spielseite.

## Vereinstermine eintragen

Termine, die nicht vom DSV kommen, werden lokal gepflegt und landen im
Standard-Kalender:

```bash
python combine.py --add-event    # interaktiv anlegen (schreibt custom_events.json)
python combine.py --list-events  # alle anzeigen
python combine.py                # Kalender neu bauen
```

Danach die geänderte `sgw_termine.ics` committen und pushen — beim nächsten Sync
sehen alle Abonnenten den neuen Termin.

## Wenn der Kalender leer bleibt

Der Scraper meldet sich selbst: Findet ein Lauf für keine Mannschaft ein
kommendes Spiel, legt die Automatisierung ein GitHub-Issue an. Das passiert
typischerweise beim Saisonwechsel, wenn der DSV die neue Spielzeit noch nicht
veröffentlicht hat.

## Lizenz

[MIT](LICENSE) © Julius Gerecke
