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
