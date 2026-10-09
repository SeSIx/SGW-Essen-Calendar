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
cleanup() {
  status=$?
  if [ "$status" -ne 0 ]; then
    "${compose[@]}" ps || true
    "${compose[@]}" logs --no-color || true
  fi
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$fake_dir"
}
trap cleanup EXIT

# Reclaim a stack left behind by a killed run, then start fresh.
"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${compose[@]}" up -d --build --renew-anon-volumes

healthy=0
for _ in $(seq 60); do
  if curl -fsS -H 'Host: localhost:8099' http://127.0.0.1:8099/healthz >/dev/null 2>&1; then
    healthy=1
    break
  fi
  sleep 1
done
if [ "$healthy" -ne 1 ]; then
  echo "sgw-admin did not become healthy within 60 s" >&2
  exit 1
fi

SGW_E2E_INVITE="$("${compose[@]}" exec -T sgw-admin sgw-admin invite julius --name Julius | tail -n 1)"
export SGW_E2E_INVITE
export SGW_E2E_SHOTS="${SGW_E2E_SHOTS:-$PWD/admin/e2e/screenshots}"
mkdir -p "$SGW_E2E_SHOTS"

python -m pip install -q -r admin/e2e/requirements.txt
python -m playwright install chromium >/dev/null
python -m pytest admin/e2e -o addopts= -rA -p no:cacheprovider
