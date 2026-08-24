# SGW Essen Wasserball-Kalender

Alle Spiele, Ergebnisse, Spielorte und Schiedsrichter der SG Wasserball Essen —
automatisch aus dem DSV-Portal geholt und als Kalender-Abo bereitgestellt.

**[English README](README.md)**

## Kalender abonnieren

Die URL als **Abo** eintragen (nicht als Import) — dann aktualisiert sich der
Kalender von selbst, sobald sich Ansetzungen ändern oder Ergebnisse feststehen.

> Es sind bewusst GitHub-Pages-Adressen. Die Dateien direkt aus dem Repository
> zu laden funktioniert nicht: `github.com/…/raw/…` antwortet mit einer
> Weiterleitung, der Kalender-Apps nicht folgen, und `raw.githubusercontent.com`
> liefert `text/plain` plus `nosniff` — damit darf keine App die Datei als
> Kalender interpretieren. Google Calendar lehnt beides ab. Pages liefert
> `.ics` korrekt als `text/calendar` aus.

| Kalender | URL |
|---|---|
| **Herren I + II** (Standard) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_termine.ics` |
| Herren I | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_1.ics` |
| Herren II | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_2.ics` |
| Damen | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_damen.ics` |
| U16 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u16.ics` |
| U14 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u14.ics` |
| U12 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u12.ics` |

Im Standard-Kalender stehen zusätzlich die Vereinstermine (Mannschaftsbesprechung,
Trainingsstart, Kampfrichter-Ausbildung), die es in den DSV-Daten nicht gibt.

### So geht's

- **Google Kalender:** nur am Rechner im Browser — in der Android-App gibt es
  diese Funktion nicht. [calendar.google.com](https://calendar.google.com) →
  Zahnrad → Einstellungen → *Kalender hinzufügen* → *Per URL* → URL einfügen →
  *Kalender hinzufügen*. Auf dem Handy taucht er danach von selbst auf; falls
  nicht, in der App unter Einstellungen den Haken bei dem Kalender setzen.
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
