"""Shared test data for the admin app tests."""

import re
from datetime import datetime
from zoneinfo import ZoneInfo

from flask.testing import FlaskClient

from admin import auth, db

EVENT_TIMED = {
    "id": "0b6f3f9e-8a8e-4a57-9a3c-1c2f5e8f0a11", "title": "Kampfrichter-Lehrgang",
    "start_date": "2026-11-21", "start_time": "09:00", "end_date": None, "end_time": "15:00",
    "location": "Sportbad Thurmfeld", "description": None,
}
EVENT_MULTI = {
    "id": "5d1e2c3b-7f40-4c1a-9e2b-3a4b5c6d7e8f", "title": "Trainingslager Duisburg",
    "start_date": "2026-11-27", "start_time": None, "end_date": "2026-11-29", "end_time": None,
    "location": "Sportpark Wedau", "description": None,
}

# (uid, summary, DTSTART, DTEND); a DTSTART without "T" is an all-day date.
TEAM_GAMES = {
    "herren_1": [
        ("2025_1_A_0", "SG Wasserball Essen 9:7 ASC Duisburg (Oberliga)", "20260920T120000Z", "20260920T133000Z"),
        ("2025_1_A_1", "SG Wasserball Essen : Iserlohn Schleddenhofer SV (Oberliga)", "20261116T193000Z", "20261116T210000Z"),
    ],
    "herren_2": [("2025_2_A_1", "TPSK 1925 : SG Wasserball Essen II (Verbandsliga)", "20261120T193000Z", "20261120T210000Z")],
    "damen": [("2025_3_A_1", "SG Wasserball Essen : Duisburg 98 (Ruhrgebietsliga weiblich)", "20261122T130000Z", "20261122T143000Z")],
    "u16": [("2025_4_A_1", "Turnier U16 (Bezirk)", "20261205", "20261206")],
    "u14": [],
    "u12": [],
}


def team_ics(games) -> str:
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//SGW Essen//test//DE"]
    for uid, summary, start, end in games:
        if "T" in start:
            when = [f"DTSTART:{start}", f"DTEND:{end}"]
        else:
            when = [f"DTSTART;VALUE=DATE:{start}", f"DTEND;VALUE=DATE:{end}"]
        lines += ["BEGIN:VEVENT", f"UID:{uid}@sgw-essen.local", "DTSTAMP:20261001T000000Z",
                  f"SUMMARY:{summary}", *when, "LOCATION:Sportbad Thurmfeld\\, Essen", "END:VEVENT"]
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


HOST = "sgw-admin.srv1136792.hstgr.cloud"
BASE = f"https://{HOST}"
NOW = int(datetime(2026, 10, 9, 12, 0, tzinfo=ZoneInfo("Europe/Berlin")).timestamp())
PW = "Wasserball-Essen-2026!"


class Clock:
    def __init__(self, t: int = NOW):
        self.t = t

    def __call__(self) -> int:
        return self.t


class HttpsClient(FlaskClient):
    """Talks to the app like a browser on the real host (the cookies are Secure)."""

    def open(self, *args, **kwargs):
        if args and isinstance(args[0], str):
            kwargs.setdefault("base_url", BASE)
        return super().open(*args, **kwargs)


def csrf_of(resp) -> str:
    match = re.search(r'name="csrf_token" value="([^"]+)"', resp.get_data(as_text=True))
    assert match, "page has no CSRF field"
    return match.group(1)


def post_form(client, path, data, *, origin=BASE, page=None):
    """POST like a same-origin browser form; CSRF comes from `page` or a GET of `path`."""
    token = csrf_of(page if page is not None else client.get(path))
    return client.post(path, data={**data, "csrf_token": token},
                       headers={"Origin": origin, "Sec-Fetch-Site": "same-origin"})


def make_user(app, login="julius", name="Julius", password=PW) -> str:
    """Create an account with a password; returns a raw session id."""
    svc = app.extensions["sgw"]
    conn = db.connect(svc.settings.db_path)
    try:
        auth.create_user(conn, login, name, svc.now())
        return auth.redeem_token(conn, auth.issue_token(conn, login, "invite", svc.now()), password, svc.now())
    finally:
        conn.close()
