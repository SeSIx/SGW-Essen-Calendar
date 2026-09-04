# SGW Essen Wasserball-Kalender

Alle Spiele, Ergebnisse, Spielorte und Schiedsrichter der SG Wasserball Essen —
automatisch aus dem DSV-Portal geholt und als Kalender-Abo bereitgestellt.

**[English README](README.md)**

## Kalender abonnieren

Als **Abo** eintragen, nicht als Import — dann aktualisiert sich der Kalender von
selbst, sobald sich Ansetzungen ändern oder Ergebnisse feststehen.

| Mannschaft | Abo-URL |
|---|---|
| **Herren I + II** (Standard) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_termine.ics` |
| Herren I | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_1.ics` |
| Herren II | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_2.ics` |
| Damen | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_damen.ics` |
| U16 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u16.ics` |
| U14 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u14.ics` |
| U12 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u12.ics` |
| Nur Vereinstermine | `https://sesix.github.io/SGW-Essen-Calendar/sgw_vereinstermine.ics` |

Im Standard-Kalender stehen zusätzlich die Vereinstermine (Mannschaftsbesprechung,
Trainingsstart, Kampfrichter-Ausbildung), die es in den DSV-Daten nicht gibt. Den
letzten Eintrag braucht nur, wer eine einzelne Mannschaft abonniert hat und die
Vereinstermine trotzdem sehen will.

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

GitHub Pages liefert den Feed als `text/calendar` mit `Cache-Control: max-age=600`
aus — diese URL ist für alle Clients die richtige.
`https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/<datei>.ics` spiegelt
dieselben Bytes und ergänzt `charset=utf-8`, sendet aber `max-age=604800`: Wer
sich daran hält, sieht Änderungen einmal pro Woche statt zweimal täglich. Nur als
Ausweichadresse verwenden. Weder `github.com/…/raw/…` (302) noch
`raw.githubusercontent.com/…` (`text/plain` plus `nosniff`) funktioniert
irgendwo.

### Wenn Google Kalender das Abo ablehnt

Google antwortet auf einen fehlgeschlagenen Versuch mit `HTTP 200 OK`, dem leeren
Rumpf `[["addcalendarfromurlaction.acfur"]]` und der Meldung „Hoppla, dieser
Kalender konnte nicht hinzugefügt werden" — einen Grund nennt es nie. Gegen den
Live-Feed geprüft am 04.09.2026 erklärt nichts auf dieser Seite den Fehler:

- Jede URL antwortet mit `200` und `text/calendar`, ohne Redirect, auch gegenüber
  dem User-Agent `Google-Calendar-Importer` und über IPv4 wie IPv6.
- Der Feed ist gültiges RFC 5545: durchgehend CRLF, Zeilen innerhalb von 75
  Oktetts, Faltung auf Zeichengrenzen, eindeutige UIDs, jedes `VEVENT` mit
  `DTSTART`, kein BOM, kein unescaptes `,` oder `;` in einem TEXT-Wert.

Die Ursache liegt also im Konto oder im Browser, nicht im Feed. In dieser
Reihenfolge eingrenzen:

1. Dieselbe URL in einem **anderen Google-Konto** eintragen, oder in einem
   frischen Browser-Profil ohne Erweiterungen. Ein Adblocker, der den
   `batchexecute`-Aufruf blockiert, erzeugt genau diesen Fehler.
2. Prüfen, ob das Konto Googles Obergrenze für abonnierte Kalender erreicht hat
   und ob ein früherer Versuch die URL nicht schon in der Liste hinterlassen hat
   — eine im selben Konto bereits bekannte URL lehnt Google ab.
3. Bei mehreren gleichzeitig angemeldeten Konten darauf achten, dass das
   `/u/<n>/` in der URL zum gemeinten Konto gehört.

Wenn das nichts bringt:

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
