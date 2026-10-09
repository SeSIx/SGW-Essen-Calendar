"""sgw-admin: accounts and token on the server, e.g. `docker exec sgw-admin sgw-admin invite trainer --name Trainer`."""

import argparse
import os
import sqlite3
import sys
import time
from collections.abc import Mapping
from datetime import datetime
from typing import TextIO

from admin import auth, db
from admin.games import BERLIN
from admin.github_store import StoreError, Unauthorized
from admin.services import build_store
from admin.settings import Settings, from_env


def _fmt(ts: int | None) -> str:
    return "–" if ts is None else datetime.fromtimestamp(ts, BERLIN).strftime("%d.%m.%Y %H:%M")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sgw-admin", description="Konten der SGW-Admin-App verwalten")
    sub = parser.add_subparsers(dest="cmd", required=True)
    invite = sub.add_parser("invite", help="Konto anlegen (falls neu) und Einladungslink ausgeben")
    invite.add_argument("login")
    invite.add_argument("--name", help="Anzeigename, nur für neue Konten")
    sub.add_parser("reset", help="Passwort zurücksetzen: Sitzungen beenden, neuen Link ausgeben").add_argument("login")
    sub.add_parser("logout-all", help="Alle Sitzungen eines Kontos beenden").add_argument("login")
    sub.add_parser("users", help="Konten auflisten")
    sub.add_parser("token-status", help="Ablaufdatum des GitHub-Tokens anzeigen")
    return parser


def main(argv: list[str] | None = None, environ: Mapping[str, str] = os.environ,
         out: TextIO = sys.stdout, now: int | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        settings = from_env(environ)
    except RuntimeError as exc:
        print(f"Konfiguration: {exc}", file=out)
        return 1
    now = int(time.time()) if now is None else now
    conn = db.connect(settings.db_path)
    try:
        db.init_schema(conn)
        return _run(args, settings, conn, now, out)
    except auth.AuthError as exc:
        print(f"Fehler: {exc}", file=out)
        return 1
    finally:
        conn.close()


def _run(args: argparse.Namespace, settings: Settings, conn: sqlite3.Connection, now: int, out: TextIO) -> int:
    if args.cmd in ("invite", "reset"):
        if args.cmd == "invite" and auth.get_user(conn, args.login) is None:
            if not args.name:
                raise auth.AuthError('Neues Konto: bitte --name "Anzeigename" angeben')
            auth.create_user(conn, args.login, args.name, now)
        raw = auth.issue_token(conn, args.login, args.cmd, now)
        what = "Einladungslink" if args.cmd == "invite" else "Link zum Neusetzen des Passworts"
        print(f"{what} für {args.login} (48 h gültig, einmal nutzbar):", file=out)
        print(f"{settings.origin}/einladung/{raw}", file=out)
        return 0
    if args.cmd == "logout-all":
        user = auth.get_user(conn, args.login)
        if user is None:
            raise auth.AuthError(f"Kein Konto „{args.login}“")
        print(f"{auth.logout_all(conn, user['id'])} Sitzung(en) beendet.", file=out)
        return 0
    if args.cmd == "users":
        for row in auth.list_users(conn, now):
            state = "Passwort gesetzt" if row["has_password"] else "Einladung offen"
            print(f"{row['login']:<12} {row['display_name']:<20} {state:<16} "
                  f"letzter Login {_fmt(row['last_login_at'])}  Sitzungen {row['sessions']}", file=out)
        return 0
    return _token_status(settings, now, out)


def _token_status(settings: Settings, now: int, out: TextIO) -> int:
    try:
        expiry = build_store(settings).check_token()
    except Unauthorized:
        print("GitHub-Token ungültig oder abgelaufen.", file=out)
        return 2
    except StoreError as exc:
        print(f"GitHub nicht erreichbar: {exc}", file=out)
        return 3
    if expiry is None:
        print("GitHub-Token gültig, kein Ablaufdatum gemeldet.", file=out)
        return 0
    days = (expiry - datetime.fromtimestamp(now, expiry.tzinfo)).days
    print(f"GitHub-Token gültig bis {expiry.astimezone(BERLIN):%d.%m.%Y %H:%M} (noch {days} Tage).", file=out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
