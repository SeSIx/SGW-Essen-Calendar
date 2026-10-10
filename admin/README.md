# SGW-Admin

Web-App, mit der die drei Admins die Vereinstermine in `custom_events.json` pflegen.
Design: `docs/superpowers/specs/2026-10-09-sgw-admin-design.md` (nur lokal).

## Einmalig einrichten

### 1. GitHub-Token (Julius)

1. github.com → Settings → Developer settings → Personal access tokens → **Fine-grained tokens** → *Generate new token*.
2. Name: `SGW-Admin`. Expiration: *Custom*, heute + 1 Jahr (höchstens 366 Tage).
3. Resource owner: `SeSIx`. Repository access: **Only select repositories** → `SGW-Essen-Calendar`.
4. Permissions → Repository permissions → **Contents: Read and write** (Metadata: read-only kommt automatisch dazu). Sonst nichts.
5. *Generate token* und kopieren (wird nur einmal angezeigt). Nie in einen Chat einfügen.

### 2. Server

```bash
git clone https://github.com/SeSIx/SGW-Essen-Calendar.git /root/sgw-admin
cd /root/sgw-admin
( umask 077; python3 -c 'import secrets; print("SECRET_KEY=" + secrets.token_urlsafe(48))' > admin/.env )
nano admin/.env            # Zeile ergänzen: GITHUB_TOKEN=github_pat_...
docker compose -f admin/compose.yml up -d --build
docker ps --filter name=sgw-admin      # STATUS zeigt nach ~30 s "(healthy)"
docker exec sgw-admin sgw-admin token-status
```

### 3. Admins einladen

```bash
docker exec sgw-admin sgw-admin invite julius --name Julius
docker exec sgw-admin sgw-admin invite trainer --name Trainer
docker exec sgw-admin sgw-admin invite kapitaen --name Kapitän
```
Jeder Befehl gibt einen Link aus, der einmal und 48 Stunden lang funktioniert. Schick ihn der Person selbst, z.B. per WhatsApp. Sie wählt ein Passwort; das Handy schlägt eins vor und speichert es.

## Im Alltag

| Aufgabe | Befehl |
|---|---|
| Passwort vergessen / Handy verloren | `docker exec sgw-admin sgw-admin reset trainer` → neuen Link schicken |
| Jemanden überall abmelden | `docker exec sgw-admin sgw-admin logout-all trainer` |
| Konten anzeigen | `docker exec sgw-admin sgw-admin users` |
| Ablauf des Tokens | `docker exec sgw-admin sgw-admin token-status` |
| App aktualisieren | `git -C /root/sgw-admin pull && docker compose -f /root/sgw-admin/admin/compose.yml up -d --build` |
| Logs | `docker logs --tail 100 sgw-admin` |

## GitHub-Token erneuern (ca. 2 Minuten)

1. github.com → Settings → Developer settings → Fine-grained tokens → `SGW-Admin` → **Regenerate token** (366 Tage).
2. `nano /root/sgw-admin/admin/.env` → Zeile `GITHUB_TOKEN=` ersetzen.
3. `cd /root/sgw-admin && docker compose -f admin/compose.yml up -d`
4. `docker exec sgw-admin sgw-admin token-status`

## Betrieb: Hinweise zu Traefik

- Traefik leitet erst weiter, wenn der Container „healthy“ ist, also ca. 30 s nach `up -d`. Bis dahin liefert die Adresse 404; das ist normal.
- Traefik darf `X-Forwarded-*`-Header von Clients nicht übernehmen (Standard `forwardedHeaders.insecure=false`, keine `trustedIPs`). Der Einstiegspunkt `websecure` in `/root/docker-compose.yml` setzt dazu nichts und nutzt damit den sicheren Standard. Bei Änderungen daran nachprüfen.
- Die Einladungslinks stehen im Pfad (`/einladung/<token>`) und dürfen in keinem Traefik-Access-Log landen. In `/root/docker-compose.yml` ist kein Access-Log aktiviert (kein `--accesslog`); wer es einschaltet, muss `/einladung/` ausfiltern. Gunicorn loggt Zugriffe ebenfalls nicht.

## Entwickeln

```bash
export PATH=/root/sgw-calendar/.venv/bin:$PATH
pip install -r requirements-dev.txt
pytest tests/webadmin && ruff check .
admin/e2e/run.sh          # Handy-Browser gegen den Container; Screenshots in admin/e2e/screenshots/
```

## Token-Wächter & Backup

Zwei systemd-Timer auf dem Host (nicht im Container):

| Timer | Wann | Was |
|---|---|---|
| `sgw-token-watch.timer` | täglich 09:00 (Berlin) | prüft das Ablaufdatum des GitHub-Tokens und warnt per WhatsApp 14, 7, 3, 1 und 0 Tage vorher; bei ungültigem Token einmal pro Tag |
| `sgw-admin-backup.timer` | täglich 03:30 (Berlin) | Snapshot der App-SQLite nach `/root/backups/sgw-admin/`, die neuesten 14 bleiben |

Der Wächter liest den GitHub-Token aus `admin/.env` und den Evolution-Schlüssel aus
`/root/evolution/docker-compose.yml`. Beides wird nicht kopiert. Die Empfängernummer
steht nur in `/etc/sgw-token-watch.conf` (Rechte 600):

```
RECIPIENT=49…            # internationale Nummer ohne +
# optional: GITHUB_ENV_FILE, EVOLUTION_COMPOSE, EVOLUTION_URL, EVOLUTION_INSTANCE, STATE_FILE
```

Installieren (einmalig, als root):

```bash
install -m 600 /dev/null /etc/sgw-token-watch.conf && nano /etc/sgw-token-watch.conf
cp /root/sgw-admin/admin/ops/sgw-token-watch.{service,timer} \
   /root/sgw-admin/admin/ops/sgw-admin-backup.{service,timer} /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now sgw-token-watch.timer sgw-admin-backup.timer
```

Prüfen:

```bash
python3 -I /root/sgw-admin/admin/ops/token_watch.py --status    # Ablaufdatum, nächste Stufe
python3 -I /root/sgw-admin/admin/ops/token_watch.py --dry-run   # fällige Nachricht anzeigen
python3 -I /root/sgw-admin/admin/ops/token_watch.py --test-message
systemctl list-timers sgw-token-watch.timer sgw-admin-backup.timer
journalctl -u sgw-token-watch -u sgw-admin-backup -n 50 --no-pager
```

Token erneuern: wie oben unter „GitHub-Token erneuern“ (die WhatsApp-Warnung enthält dieselben
Schritte). Danach `docker exec sgw-admin sgw-admin token-status` prüfen; der Wächter liest
das neue Ablaufdatum am nächsten Morgen selbst. Wird der Evolution-Schlüssel rotiert, liest
der Wächter automatisch den neuen Wert aus der Compose-Datei.

Die Backup-Unit hat bewusst kein `ProtectHome`, weil sie nach `/root/backups` schreibt.

Backup wiederherstellen (`<datei>` = Name aus `ls /root/backups/sgw-admin/`; die Backups
gehören root mit Rechten 600, daher läuft der Wiederherstellungs-Container als root und
übergibt die Datei danach an `sgw`. Der Container erbt `cap_drop: ALL` und ein
schreibgeschütztes Root-Dateisystem, deshalb braucht er `DAC_OVERRIDE` (Schreiben in `/data`,
Lesen der 600-Datei) und `CHOWN`):

```bash
cd /root/sgw-admin
docker compose -f admin/compose.yml stop sgw-admin
docker compose -f admin/compose.yml run --rm --no-deps --user root \
  --cap-add DAC_OVERRIDE --cap-add CHOWN --entrypoint sh \
  -v /root/backups/sgw-admin:/backup:ro sgw-admin \
  -c 'db="${SGW_ADMIN_DB:-/data/sgw-admin.db}"; rm -f "$db-wal" "$db-shm" && cp /backup/<datei> "$db" && chmod 600 "$db" && chown sgw:sgw "$db"'
docker compose -f admin/compose.yml start sgw-admin
docker ps --filter name=sgw-admin     # nach ca. 30 s „healthy“
docker exec sgw-admin sgw-admin users
```
