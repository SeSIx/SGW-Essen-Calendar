"""PWA manifest: one white splash, and a maskable icon that survives cropping."""

import json
import struct
import zlib
from pathlib import Path

STATIC = Path(__file__).resolve().parents[2] / "admin" / "static"
WHITE = (255, 255, 255)


def manifest():
    return json.loads((STATIC / "manifest.webmanifest").read_text(encoding="utf-8"))


def read_png(path):
    """(width, height, rows of RGB tuples) of an 8-bit, non-interlaced RGB/RGBA PNG (no Pillow in the venv)."""
    raw = path.read_bytes()
    assert raw[:8] == b"\x89PNG\r\n\x1a\n"
    pos, idat, header = 8, b"", None
    while pos < len(raw):
        size, kind = struct.unpack(">I4s", raw[pos:pos + 8])
        body = raw[pos + 8:pos + 8 + size]
        pos += 12 + size
        if kind == b"IHDR":
            header = struct.unpack(">IIBBBBB", body)
        elif kind == b"IDAT":
            idat += body
    w, h, depth, ctype, _, _, interlace = header
    assert depth == 8 and ctype in (2, 6) and interlace == 0
    bpp = 3 if ctype == 2 else 4
    data, stride = zlib.decompress(idat), w * bpp
    rows, prev = [], bytearray(stride)
    for y in range(h):
        ft, line = data[y * (stride + 1)], bytearray(data[y * (stride + 1) + 1:(y + 1) * (stride + 1)])
        for i in range(stride):
            a = line[i - bpp] if i >= bpp else 0
            b, c = prev[i], (prev[i - bpp] if i >= bpp else 0)
            if ft == 1:
                line[i] = (line[i] + a) & 255
            elif ft == 2:
                line[i] = (line[i] + b) & 255
            elif ft == 3:
                line[i] = (line[i] + (a + b) // 2) & 255
            elif ft == 4:
                p = a + b - c
                pa, pb, pc = abs(p - a), abs(p - b), abs(p - c)
                line[i] = (line[i] + (a if pa <= pb and pa <= pc else b if pb <= pc else c)) & 255
        rows.append([tuple(line[x * bpp:x * bpp + 3]) for x in range(w)])
        prev = line
    return w, h, rows


def corners(rows):
    return [rows[0][0], rows[0][-1], rows[-1][0], rows[-1][-1]]


def test_splash_background_matches_icon_tile():
    data = manifest()
    assert data["background_color"] == "#ffffff"
    assert data["theme_color"] == "#0c1f52"
    _, _, rows = read_png(STATIC / "icon-192.png")
    assert all(c == WHITE for c in corners(rows))


def test_maskable_icon():
    entries = [i for i in manifest()["icons"] if i.get("purpose") == "maskable"]
    assert entries == [{"src": "/static/icon-maskable-512.png", "sizes": "512x512",
                        "type": "image/png", "purpose": "maskable"}]
    w, h, rows = read_png(STATIC / "icon-maskable-512.png")
    assert (w, h) == (512, 512)
    assert all(c == WHITE for c in corners(rows))
    ink = [(x, y) for y, row in enumerate(rows) for x, px in enumerate(row) if px != WHITE]
    assert ink, "the logo is missing"
    xs, ys = [p[0] for p in ink], [p[1] for p in ink]
    margin = 51  # the safe zone is the central 80 %
    assert min(xs) >= margin and min(ys) >= margin
    assert max(xs) < 512 - margin and max(ys) < 512 - margin


def test_existing_icons_kept():
    srcs = {i["src"] for i in manifest()["icons"]}
    assert {"/static/icon-192.png", "/static/apple-touch-icon.png"} <= srcs
