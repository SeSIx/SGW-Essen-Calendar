"""Pools the club plays and trains in, and the Maps link for a location."""

from urllib.parse import quote

POOLS = (
    ("Hesse", 'Freibad Dellwig "Hesse", Scheppmannskamp 6, 45357 Essen'),
    ("Thurmfeld", "Sportbad Thurmfeld, Reckhammerweg 84, 45141 Essen"),
    ("LZ Rüttenscheid", "Schwimmleistungszentrum Essen, von-Einem-Straße 77, 45130 Essen"),
)


def maps_url(location: str) -> str:
    return "https://www.google.com/maps/search/?api=1&query=" + quote(location, safe="")
