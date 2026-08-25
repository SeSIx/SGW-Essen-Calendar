# SGW Essen Wasserball-Kalender

Alle Spiele, Ergebnisse, Spielorte und Schiedsrichter der SG Wasserball Essen —
automatisch aus dem DSV-Portal geholt und als Kalender-Abo bereitgestellt.

**[English README](README.md)**

## Kalender abonnieren

Als **Abo** eintragen, nicht als Import — dann aktualisiert sich der Kalender von
selbst, sobald sich Ansetzungen ändern oder Ergebnisse feststehen.

| Mannschaft | Google Kalender | Apple, Outlook, Thunderbird |
|---|---|---|
| **Herren I + II** (Standard) | `https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/sgw_termine.ics` | `https://sesix.github.io/SGW-Essen-Calendar/sgw_termine.ics` |
| Herren I | `https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/sgw_essen_herren_1.ics` | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_1.ics` |
| Herren II | `https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/sgw_essen_herren_2.ics` | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_2.ics` |
| Damen | `https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/sgw_essen_damen.ics` | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_damen.ics` |
| U16 | `https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/sgw_essen_u16.ics` | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u16.ics` |
| U14 | `https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/sgw_essen_u14.ics` | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u14.ics` |
| U12 | `https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/sgw_essen_u12.ics` | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u12.ics` |

Im Standard-Kalender stehen zusätzlich die Vereinstermine (Mannschaftsbesprechung,
Trainingsstart, Kampfrichter-Ausbildung), die es in den DSV-Daten nicht gibt.

### So geht's

**Google Kalender — nur am Rechner im Browser.** Die Android-App kann keine
Kalender per URL hinzufügen; einmal am Rechner eingetragen, erscheint das Abo
automatisch auf dem Handy.

1. [calendar.google.com](https://calendar.google.com) öffnen
2. Zahnrad → *Einstellungen* → *Kalender hinzufügen* → *Per URL*
3. URL einfügen — **ohne Leerzeichen am Ende** — und *Kalender hinzufügen*

**Andere Apps:** iPhone/iPad → Einstellungen → Apps → Kalender → Accounts →
Account hinzufügen → Andere → Kalenderabo. Outlook → Kalender hinzufügen → Aus
dem Internet. Apple Kalender am Mac → Ablage → Neues Kalenderabo.

### Google Kalender nimmt derzeit keine neuen Abos an

**Stand 25.08.2026:** Googles Endpunkt `addcalendarfromurl` antwortet auf jeden
Abo-Versuch mit `HTTP 200 OK` und dem leeren Rumpf
`[["addcalendarfromurlaction.acfur"]]` — er meldet Erfolg und legt nichts an.
Im Browser erscheint „Hoppla, dieser Kalender konnte nicht hinzugefügt werden".

Das betrifft **jeden** neuen Feed, nicht nur diesen: In Tests wurde auch ein
fremder, unbeteiligter Kalender abgelehnt, während bereits bekannte Feeds
(Googles Feiertagskalender, officeholidays.com) angenommen wurden. Bestehende
Abos laufen weiter. Gleiches ist am 23./24.08.2026 unabhängig dokumentiert
worden.

Bis Google das behebt:

- **Apple Kalender, Outlook und Thunderbird** abonnieren den Feed problemlos.
- Für Google Kalender: [GAS-ICS-Sync](https://github.com/derekantrican/GAS-ICS-Sync)
  — ein Apps Script im eigenen Konto, das den Feed selbst abholt und die Termine
  über die Kalender-API einträgt. Umgeht die defekte Abo-Funktion vollständig und
  synchronisiert häufiger als Googles 24-Stunden-Takt.

### Warum zwei verschiedene Adressen?

Google nimmt einen Feed nur an, wenn der Server ihn als
`text/calendar; charset=utf-8` ausliefert. GitHub Pages sendet `text/calendar`
ohne Zeichensatz, deshalb die jsDelivr-Adresse für Google; dafür cacht jsDelivr
bis zu zwölf Stunden, was bei Googles täglichem Abrufrhythmus nicht auffällt.
Alle anderen Apps fragen häufiger nach und bekommen die Pages-Adresse, die
sofort aktuell ist.

Nicht funktionieren: `github.com/…/raw/…` (Weiterleitung) und
`raw.githubusercontent.com/…` (`text/plain` plus `nosniff`).

Jeder Eintrag enthält das Ergebnis, sobald gespielt wurde, die vollständige
Adresse der Schwimmhalle, die Schiedsrichter und einen Link zur DSV-Spielseite.

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
