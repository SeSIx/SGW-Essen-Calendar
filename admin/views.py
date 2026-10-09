"""Routes."""

import functools
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from flask import Blueprint, abort, g, make_response, redirect, render_template, request, url_for

import custom_events
from admin import agenda, auth, changes, forms, games, security
from admin.github_store import CorruptFile, RateLimited, StoreError, Unauthorized
from admin.security import SESSION_COOKIE
from admin.services import get_db, services

bp = Blueprint("main", __name__)

TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{20,100}")
DEAD_LINK = "Dieser Link ist ungültig oder abgelaufen. Bitte Julius um einen neuen."


def load_user() -> None:
    g.user = None
    if request.path.startswith("/static/"):
        return
    raw = request.cookies.get(SESSION_COOKIE, "")
    if 20 <= len(raw) <= 100:
        g.user = auth.load_session(get_db(), raw, services().now())


def login_required(view):
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        if g.user is None:
            return redirect(url_for("main.login"))
        return view(*args, **kwargs)
    return wrapped


@bp.get("/healthz")
def healthz():
    return "ok", 200, {"Content-Type": "text/plain; charset=utf-8"}


NOTICES = {
    "gespeichert": "Gespeichert ✓ – Kalender in ca. 1 Min. aktuell",
    "geloescht": "Gelöscht ✓ – Kalender in ca. 1 Min. aktuell",
}
TEAMS_COOKIE = "teams"
WARN_BEFORE = timedelta(days=14)
RED_BANNER = ("Zugang zu GitHub abgelaufen oder ungültig – Speichern ist gesperrt. "
              "Bitte Julius Bescheid geben.")


def _selected_teams() -> tuple[str, ...]:
    for raw in (request.args.get("teams"), request.cookies.get(TEAMS_COOKIE)):
        chosen = games.parse_teams(raw)
        if chosen:
            return chosen
    return games.DEFAULT_TEAMS


def _chips(selected: tuple[str, ...]) -> list[SimpleNamespace]:
    chips = []
    for slug, label in games.TEAMS:
        toggled = [s for s, _ in games.TEAMS if (s in selected) != (s == slug)]
        href = url_for("main.index", teams=",".join(toggled)) if toggled else None
        chips.append(SimpleNamespace(label=label, on=slug in selected, href=href))
    return chips


@bp.app_context_processor
def _banner():
    if g.get("user") is None:
        return {}
    store = services().store
    if store.unauthorized:
        return {"banner": SimpleNamespace(level="red", text=RED_BANNER)}
    expiry = store.token_expiry
    if expiry is not None and expiry - datetime.fromtimestamp(services().clock(), UTC) <= WARN_BEFORE:
        return {"banner": SimpleNamespace(
            level="yellow",
            text=f"Zugang zu GitHub läuft am {expiry.astimezone(games.BERLIN):%d.%m.%Y} ab – "
                 "bitte Julius Bescheid geben.")}
    return {}


@bp.get("/")
@login_required
def index():
    svc = services()
    selected = _selected_teams()
    past = request.args.get("frueher") == "1"
    problem, events, broken = None, [], []
    try:
        snapshot = svc.store.load()
        events = snapshot.result.valid
        broken = agenda.broken_items(snapshot.result.invalid)
    except StoreError as exc:
        problem = store_problem(exc)
    found, stale = svc.games.games(selected)
    today = datetime.fromtimestamp(svc.clock(), games.BERLIN).date()
    resp = make_response(render_template(
        "agenda.html", months=agenda.build(events, found, today, past=past), broken=broken,
        chips=_chips(selected), past=past, problem=problem, games_stale=stale,
        notice=NOTICES.get(request.args.get("ok", "")),
        today_label=f"Heute: {today.day}. {agenda.MONTHS[today.month - 1][:3]}"))
    if request.args.get("teams") is not None:
        resp.set_cookie(TEAMS_COOKIE, ",".join(selected), max_age=365 * 24 * 3600,
                        secure=True, httponly=True, samesite="Lax", path="/")
    return resp


@bp.get("/spiel/<uid>")
@login_required
def game(uid):
    if not games.GAME_UID_RE.fullmatch(uid):
        abort(404)
    found = services().games.find(uid)
    if found is None:
        abort(404)
    day = found.start_date
    when = f"{agenda.WEEKDAYS[day.weekday()].capitalize()} {day:%d.%m.%Y}"
    if not found.all_day:
        when += f", {found.start:%H:%M} Uhr"
    return render_template("game.html", game=found, when=when)


@bp.route("/login", methods=["GET", "POST"])
def login():
    if g.user is not None:
        return redirect(url_for("main.index"))
    if request.method != "POST":  # GET or HEAD
        return render_template("login.html", error=None, login_name="")
    name = request.form.get("login", "")[:64]
    result = auth.login(get_db(), name, request.form.get("password", "")[:256],
                        request.remote_addr or "unknown", services().now())
    if result.error:
        return render_template("login.html", error=result.error, login_name=name), 401
    resp = redirect(url_for("main.index"))
    security.set_session_cookie(resp, result.session)
    return resp


@bp.route("/einladung/<token>", methods=["GET", "POST"])
def invite(token):
    conn, now = get_db(), services().now()
    link = auth.peek_token(conn, token, now) if TOKEN_RE.fullmatch(token) else None
    if link is None:
        return render_template("message.html", title="Link ungültig", text=DEAD_LINK), 404
    if request.method != "POST":  # GET and HEAD (link previews) never consume the link
        return render_template("invite.html", link=link, error=None)
    password = request.form.get("password", "")[:256]
    if password != request.form.get("password2", "")[:256]:
        return render_template("invite.html", link=link,
                               error="Die beiden Passwörter stimmen nicht überein"), 400
    try:
        session = auth.redeem_token(conn, token, password, now)
    except auth.PasswordRejected as exc:
        return render_template("invite.html", link=link, error=str(exc)), 400
    except auth.InvalidLink:
        return render_template("message.html", title="Link ungültig", text=DEAD_LINK), 404
    resp = redirect(url_for("main.index"))
    security.set_session_cookie(resp, session)
    return resp


@bp.post("/abmelden")
@login_required
def logout():
    auth.logout(get_db(), request.cookies.get(security.SESSION_COOKIE, ""))
    resp = redirect(url_for("main.login"))
    security.clear_session_cookie(resp)
    return resp


@bp.post("/abmelden/ueberall")
@login_required
def logout_everywhere():
    auth.logout_all(get_db(), g.user.user_id)
    resp = redirect(url_for("main.login"))
    security.clear_session_cookie(resp)
    return resp


ALREADY_SAVED = "Dieser Termin war bereits gespeichert. Du kannst ihn hier noch korrigieren."
BUSY = "GitHub war gerade beschäftigt – bitte nochmal speichern."


def store_problem(exc: StoreError) -> str:
    if isinstance(exc, Unauthorized):
        return "Zugang zu GitHub abgelaufen oder ungültig – bitte Julius Bescheid geben."
    if isinstance(exc, RateLimited):
        return "Zu viele Anfragen, bitte kurz warten."
    if isinstance(exc, CorruptFile):
        return "custom_events.json ist beschädigt – bitte Julius Bescheid geben."
    return "GitHub gerade nicht erreichbar."


def _author():
    return changes.author_for(g.user.login, g.user.display_name)


def _form(mode, data, event_id, rev, *, error=None, field_errors=None, notice=None, status=200):
    return render_template(
        "event_form.html", mode=mode, data=data, event_id=event_id, rev=rev, error=error, notice=notice,
        field_errors=field_errors or {}, save_blocked=services().store.unauthorized), status


def _invalid(mode, data, event_id, rev, err):
    if err.field in forms.FORM_FIELDS:
        return _form(mode, data, event_id, rev, field_errors={err.field: err.message}, status=400)
    return _form(mode, data, event_id, rev, error=err.message, status=400)


def _who() -> str:
    try:
        return services().store.last_author() or "jemand anderem"
    except StoreError:
        return "jemand anderem"


def _conflict(kind, event_id, mine, current):
    theirs = forms.from_event(current) if isinstance(current, dict) else None
    rev = custom_events.event_rev(current) if isinstance(current, dict) else ""
    who = _who()  # may hit GitHub, so read the token state afterwards
    return render_template("conflict.html", kind=kind, event_id=event_id, mine=mine, theirs=theirs,
                           rev=rev, who=who, save_blocked=services().store.unauthorized), 409


def _load_entry(event_id):
    """The stored entry for the form, or an error response."""
    if not changes.EVENT_ID_RE.fullmatch(event_id):
        abort(404)
    try:
        snapshot = services().store.load()
    except StoreError as exc:
        return None, (render_template("message.html", title="Nicht erreichbar", text=store_problem(exc)), 503)
    entry = changes.find(snapshot, event_id)
    if not isinstance(entry, dict):
        abort(404)
    return entry, None


def _commit(mode, data, event_id, rev, mutate, title, verb):
    """Run a change; returns an error response, or None on success."""
    try:
        changes.commit(services().store, mutate, changes.message(g.user.display_name, title, verb), _author())
    except changes.EventConflict as conflict:
        return _conflict("edit", event_id, data, conflict.current)
    except changes.SaveConflict:
        return _form(mode, data, event_id, rev, error=BUSY, status=409)
    except changes.DuplicateId:
        return redirect(url_for("main.edit_event", event_id=event_id, hinweis="schon-gespeichert"), 303)
    except StoreError as exc:
        return _form(mode, data, event_id, rev,
                     error=f"{store_problem(exc)} Deine Eingaben sind noch da.", status=503)
    return None


@bp.route("/termin/neu", methods=["GET", "POST"])
@login_required
def new_event():
    if request.method != "POST":  # GET or HEAD
        return _form("new", forms.FormData(), custom_events.new_id(), "")
    event_id = request.form.get("id", "")
    if not changes.EVENT_ID_RE.fullmatch(event_id):
        abort(400)
    data = forms.from_request(request.form)
    try:
        event = forms.to_event(data, event_id)
    except custom_events.ValidationError as err:
        return _invalid("new", data, event_id, "", err)
    failed = _commit("new", data, event_id, "", lambda s: changes.create(s, event), event["title"], "angelegt")
    return failed or redirect(url_for("main.index", ok="gespeichert"))


@bp.route("/termin/<event_id>", methods=["GET", "POST"])
@login_required
def edit_event(event_id):
    if request.method != "POST":  # GET or HEAD
        entry, failed = _load_entry(event_id)
        if failed:
            return failed
        notice = ALREADY_SAVED if request.args.get("hinweis") == "schon-gespeichert" else None
        return _form("edit", forms.from_event(entry), event_id, custom_events.event_rev(entry), notice=notice)
    if not changes.EVENT_ID_RE.fullmatch(event_id):
        abort(404)
    rev = request.form.get("rev", "")[:64]
    data = forms.from_request(request.form)
    try:
        event = forms.to_event(data, event_id)
    except custom_events.ValidationError as err:
        return _invalid("edit", data, event_id, rev, err)
    failed = _commit("edit", data, event_id, rev, lambda s: changes.update(s, event_id, rev, event),
                     event["title"], "geändert")
    return failed or redirect(url_for("main.index", ok="gespeichert"))


@bp.route("/termin/<event_id>/loeschen", methods=["GET", "POST"])
@login_required
def delete_event(event_id):
    if request.method != "POST":  # GET or HEAD
        blocked = services().store.unauthorized  # read before the load below resets it
        entry, failed = _load_entry(event_id)
        if failed:
            return failed
        return render_template("delete_confirm.html", event_id=event_id, save_blocked=blocked,
                               title=forms.from_event(entry).title or "(ohne Titel)",
                               rev=custom_events.event_rev(entry))
    if not changes.EVENT_ID_RE.fullmatch(event_id):
        abort(404)
    if services().store.unauthorized:
        return render_template("message.html", title="Nicht gelöscht", text=store_problem(Unauthorized())), 503
    rev = request.form.get("rev", "")[:64]
    titles = []

    def mutate(snapshot):
        # Take the commit-message title from the snapshot commit() loaded.
        entry = changes.find(snapshot, event_id)
        if isinstance(entry, dict):
            titles.append(forms.from_event(entry).title)
        return changes.delete(snapshot, event_id, rev)

    try:
        changes.commit(services().store, mutate, lambda: changes.message(
            g.user.display_name, titles[-1] if titles and titles[-1] else event_id, "gelöscht"), _author())
    except changes.EventConflict as conflict:
        return _conflict("delete", event_id, forms.from_event(conflict.current), conflict.current)
    except changes.SaveConflict:
        return render_template("message.html", title="Bitte nochmal", text=BUSY), 409
    except StoreError as exc:
        return render_template("message.html", title="Nicht gelöscht", text=store_problem(exc)), 503
    return redirect(url_for("main.index", ok="geloescht"))
