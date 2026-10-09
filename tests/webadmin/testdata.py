"""Shared test data for the admin app tests."""

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
