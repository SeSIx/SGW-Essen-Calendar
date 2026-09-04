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

| Team | Subscription URL |
|---|---|
| **Men I + II** (default) | `https://sesix.github.io/SGW-Essen-Calendar/sgw_termine.ics` |
| Men I | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_1.ics` |
| Men II | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_herren_2.ics` |
| Women | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_damen.ics` |
| U16 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u16.ics` |
| U14 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u14.ics` |
| U12 | `https://sesix.github.io/SGW-Essen-Calendar/sgw_essen_u12.ics` |
| Club dates only | `https://sesix.github.io/SGW-Essen-Calendar/sgw_vereinstermine.ics` |

The default feed also carries club dates (team meetings, training start, referee
courses) that exist nowhere in the DSV data. The last entry is only for anyone
subscribed to a single team who still wants those dates alongside it.

Google Calendar only accepts subscriptions added from the web interface — its
Android app cannot add a calendar by URL at all, and a
`calendar/render?cid=webcal://…` link, the usual one-click trick, is decoded as
base64 by that app and produces a garbage calendar name. Add it once at
[calendar.google.com](https://calendar.google.com) under Settings → Add calendar
→ From URL, and it appears on the phone by itself.

GitHub Pages serves the feed as `text/calendar` with
`Cache-Control: max-age=600`, which is what every client should use.
`https://cdn.jsdelivr.net/gh/SeSIx/SGW-Essen-Calendar/<file>.ics` mirrors the
same bytes and adds `charset=utf-8`, but sends `max-age=604800`: a client that
honours it sees updates once a week instead of twice a day. Treat it as a
fallback, not the default. Neither `github.com/…/raw/…` (302) nor
`raw.githubusercontent.com/…` (`text/plain` plus `nosniff`) works anywhere.

Each entry carries the result once played, the full venue address, the referees,
and a link back to the DSV match page.

### If Google Calendar refuses the subscription

Google answers a failed attempt with `HTTP 200 OK`, the empty payload
`[["addcalendarfromurlaction.acfur"]]` and the generic message "Oops, this
calendar could not be added" — it never says why. Checked against the live feed
on 2026-09-04, nothing on this side explains it:

- every URL answers `200` with `text/calendar` and no redirect, including to the
  `Google-Calendar-Importer` user agent, over both IPv4 and IPv6,
- the feed is valid RFC 5545: CRLF throughout, lines within 75 octets, folds on
  character boundaries, unique UIDs, every `VEVENT` with a `DTSTART`, no BOM and
  no unescaped `,` or `;` in any TEXT value.

So the fault is in the account or the browser, not the feed. Isolate it in this
order:

1. Add the same URL from a **different Google account**, or from a clean browser
   profile with extensions disabled. An ad blocker breaking the `batchexecute`
   RPC produces exactly this failure.
2. Check that the account is not at Google's cap on subscribed calendars, and
   that a previous attempt did not already leave the URL in the list — Google
   refuses a URL it has seen before in the same account.
3. If you are signed into several accounts, make sure the `/u/<n>/` in the URL
   matches the account you mean to add the calendar to.

Failing that, Apple Calendar, Outlook and Thunderbird subscribe to the feed
without trouble, and
[GAS-ICS-Sync](https://github.com/derekantrican/GAS-ICS-Sync) writes the events
into a Google calendar via the API, bypassing the endpoint entirely.

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
