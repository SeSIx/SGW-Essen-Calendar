#!/usr/bin/env bash
# Builds the image, starts it with the fake GitHub store on 127.0.0.1:8099,
# and walks through the app with a phone-sized Chromium.
set -euo pipefail
cd "$(dirname "$0")/../.."

fake_dir="$(mktemp -d)"
export SGW_E2E_FAKE_DIR="$fake_dir"
cp custom_events.json sgw_essen_*.ics "$fake_dir/"
chmod -R a+rwX "$fake_dir"

compose=(docker compose -f admin/e2e/compose.e2e.yml)
cleanup() { "${compose[@]}" down -v >/dev/null 2>&1 || true; rm -rf "$fake_dir"; }
trap cleanup EXIT

"${compose[@]}" up -d --build
for _ in $(seq 60); do
  curl -fsS -H 'Host: localhost:8099' http://127.0.0.1:8099/healthz >/dev/null 2>&1 && break
  sleep 1
done

SGW_E2E_INVITE="$("${compose[@]}" exec -T sgw-admin sgw-admin invite julius --name Julius | tail -n 1)"
export SGW_E2E_INVITE
export SGW_E2E_SHOTS="${SGW_E2E_SHOTS:-$PWD/admin/e2e/screenshots}"
mkdir -p "$SGW_E2E_SHOTS"

pip install -q -r admin/e2e/requirements.txt
python -m playwright install chromium >/dev/null
python -m pytest admin/e2e -q -p no:cacheprovider
