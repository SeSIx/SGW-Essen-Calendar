# SGW Essen Water Polo Calendar

[![Tests](https://github.com/SeSIx/SGW-Essen-Calendar/actions/workflows/tests.yml/badge.svg)](https://github.com/SeSIx/SGW-Essen-Calendar/actions/workflows/tests.yml)
[![Update calendar](https://github.com/SeSIx/SGW-Essen-Calendar/actions/workflows/update-calendar.yml/badge.svg)](https://github.com/SeSIx/SGW-Essen-Calendar/actions/workflows/update-calendar.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Fixtures, results, venues and referees for every SG Wasserball Essen team, scraped
from the German Swimming Federation (DSV) portal and published as subscribable
iCalendar feeds. Runs unattended on GitHub Actions twice a day.

**🇩🇪 [Abo-Anleitung auf Deutsch](README.de.md)**

## Subscribe

Add these as a *subscription*, not an import, and new fixtures, venue changes and
results appear automatically.

| Team | Google Calendar | Other apps (Apple, Outlook, Thunderbird) |
|---|---|---|
| **Men I + II** (default) | [subscribe](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_termine.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_termine.ics` |
| Men I | [subscribe](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_herren_1.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_1.ics` |
| Men II | [subscribe](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_herren_2.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_2.ics` |
| Women | [subscribe](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_damen.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_damen.ics` |
| U16 | [subscribe](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_u16.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u16.ics` |
| U14 | [subscribe](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_u14.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u14.ics` |
| U12 | [subscribe](https://www.google.com/calendar/render?cid=webcal://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar@main/sgw_essen_u12.ics) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u12.ics` |

The default feed also carries club dates (team meetings, training start, referee
courses) that exist nowhere in the DSV data.

Two hosts, because Google Calendar only accepts a feed served as
`text/calendar; charset=utf-8`. GitHub Pages omits the charset, so the Google
links go through jsDelivr, which sets the full header; jsDelivr caches for up to
twelve hours, which costs nothing because Google polls roughly once a day anyway.
Clients that poll more often get the Pages URL, which is current the moment a run
finishes. Neither `github.com/…/raw/…` (302 redirect) nor
`raw.githubusercontent.com/…` (`text/plain` plus `nosniff`) works with any of them.

Each entry carries the result once played, the full venue address so navigation
apps can route to it, the referees, and a link back to the DSV match page.

## What it does

The DSV publishes fixtures as paginated ASP.NET pages with no API. This project:

1. fetches the club's season page for every relevant DSV season key,
2. parses fixtures, results, venues, referees and player statistics out of the HTML,
3. stores them in one SQLite database per team, upserting so reruns are idempotent,
4. renders RFC 5545 calendars, and
5. commits the changed `.ics` files so subscribers pick them up on their next sync.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the data flow and the design
decisions behind it.

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

python main.py                 # scrape, rebuild every calendar, print a run report
python main.py --no-details    # fast pass: fixtures only, no detail pages
python main.py --summary       # row counts from the existing databases
python combine.py --add-event  # add a club date to the default calendar
pytest                         # 56 offline tests, no network access
ruff check .
```

`main.py` exits `2` when no team has a single upcoming fixture — the failure mode
that would otherwise publish an empty calendar unnoticed. The scheduled workflow
turns that exit code into a GitHub issue.

## Tech stack

Python 3.12 · requests · BeautifulSoup/lxml · SQLite · pytest · ruff · GitHub Actions

## Repository layout

| Path | Purpose |
|---|---|
| `main.py` | Orchestrator: seasons, scraping, run report, exit codes |
| `scraper.py` | HTTP + HTML parsing for the DSV portal |
| `db.py` | SQLite schema, non-destructive upserts, health queries |
| `ics.py` | RFC 5545 calendar generation per team |
| `combine.py` | Default calendar (Men I + II) plus club dates from `custom_events.json` |
| `config.py` | URLs, club id, season arithmetic |
| `tests/` | Offline suite driven by captured DSV HTML |
| `.github/workflows/` | Test, scrape and live-smoke automation |

## Data source and fair use

Data belongs to the [Deutscher Schwimm-Verband](https://dsvdaten.dsv.de). Requests
are serialised with a four-second delay, rate-limit responses are retried with
backoff, and detail pages are only refetched when something is actually missing —
a full rerun costs a handful of requests instead of seventy-four. This is a
non-commercial tool for one club's members.

## License

[MIT](LICENSE) © Julius Gerecke
