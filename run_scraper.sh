#!/usr/bin/env bash
# run_scraper.sh — scrape DSV, rebuild sgw_termine.ics, push if changed.
# Designed to be called from cron; logs go to stdout (redirect in crontab).

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$REPO_DIR/.venv"
PYTHON="$VENV/bin/python"
# Fall back to the branch actually checked out; the old rewrite branch is gone.
GIT_BRANCH="${GIT_BRANCH:-$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)}"

echo "=== $(date '+%Y-%m-%d %H:%M:%S') START ==="

# Activate venv (create + install if first run)
if [ ! -f "$PYTHON" ]; then
    echo "[Setup] Creating virtual environment..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install --quiet -r "$REPO_DIR/requirements.txt"
fi

cd "$REPO_DIR"

# Pull latest code before running (avoid conflicts on push)
echo "[Git] Pulling latest..."
git pull --ff-only origin "$GIT_BRANCH" || {
    echo "[Git] WARNING: pull failed — continuing with local version"
}

# Run the scraper. Exit code 2 means "no upcoming fixtures" — worth shouting
# about, but the calendars it did produce should still be committed, so this
# must not abort the script under `set -e`.
echo "[Scraper] Running main.py..."
SCRAPE_STATUS=0
"$PYTHON" main.py || SCRAPE_STATUS=$?
if [ "$SCRAPE_STATUS" -ne 0 ]; then
    echo "[Scraper] WARNING: main.py exited with $SCRAPE_STATUS"
fi

# Stage all ICS files at repo root (per-team + combined)
git add -- *.ics 2>/dev/null || true

if ! git diff --cached --quiet; then
    echo "[Git] ICS files changed — committing and pushing..."
    git -c user.name="SGW Bot" -c user.email="bot@sgw-essen.local" \
        commit -m "Auto-update: calendars $(date '+%Y-%m-%d %H:%M')"
    git push origin "$GIT_BRANCH"
    echo "[Git] Push successful."
else
    echo "[Git] No changes to ICS files — nothing to push."
fi

echo "=== $(date '+%Y-%m-%d %H:%M:%S') DONE (scraper exit $SCRAPE_STATUS) ==="
exit "$SCRAPE_STATUS"
