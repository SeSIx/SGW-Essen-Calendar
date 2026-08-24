# SGW Essen Wasserball-Kalender

Alle Spiele, Ergebnisse, Spielorte und Schiedsrichter der SG Wasserball Essen —
automatisch aus dem DSV-Portal geholt und als Kalender-Abo bereitgestellt.

**[English README](README.md)**

## Kalender abonnieren

Als **Abo** eintragen, nicht als Import — dann aktualisiert sich der Kalender von
selbst, sobald sich Ansetzungen ändern oder Ergebnisse feststehen.

| Mannschaft | Google Kalender | Andere Apps (Apple, Outlook, Thunderbird) |
|---|---|---|
| **Herren I + II** (Standard) | [abonnieren](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_termine.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_termine.ics` |
| Herren I | [abonnieren](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_herren_1.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_1.ics` |
| Herren II | [abonnieren](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_herren_2.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_2.ics` |
| Damen | [abonnieren](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_damen.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_damen.ics` |
| U16 | [abonnieren](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_u16.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u16.ics` |
| U14 | [abonnieren](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_u14.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u14.ics` |
| U12 | [abonnieren](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_u12.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u12.ics` |

Im Standard-Kalender stehen zusätzlich die Vereinstermine (Mannschaftsbesprechung,
Trainingsstart, Kampfrichter-Ausbildung), die es in den DSV-Daten nicht gibt.

### Warum zwei verschiedene Adressen?

Google Kalender nimmt einen Feed nur an, wenn der Server ihn als
`text/calendar; charset=utf-8` ausliefert. GitHub Pages sendet `text/calendar`
ohne Zeichensatz, deshalb läuft der Google-Link über jsDelivr, das den Header
vollständig setzt. Dafür cacht jsDelivr bis zu zwölf Stunden — für Google
egal, weil es ohnehin nur etwa einmal täglich nachsieht. Alle anderen Apps
fragen häufiger nach und bekommen darum die Pages-Adresse, die sofort aktuell ist.

Nicht funktionieren: `github.com/…/raw/…` (antwortet mit einer Weiterleitung)
und `raw.githubusercontent.com/…` (liefert `text/plain` plus `nosniff`).

### So geht's

- **Google Kalender:** oben auf *abonnieren* klicken. Manuell geht es nur am
  Rechner im Browser — die Android-App hat diese Funktion nicht:
  [calendar.google.com](https://calendar.google.com) → Zahnrad → Einstellungen →
  *Kalender hinzufügen* → *Per URL*.
- **iPhone/iPad:** Einstellungen → Apps → Kalender → Accounts → Account hinzufügen →
  Andere → Kalenderabo hinzufügen
- **Outlook:** Kalender → Kalender hinzufügen → Aus dem Internet
- **Apple Kalender (Mac):** Ablage → Neues Kalenderabo

Jeder Eintrag enthält das Ergebnis, sobald gespielt wurde, die vollständige Adresse
der Schwimmhalle, die Schiedsrichter und einen Link zur DSV-Spielseite.

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
