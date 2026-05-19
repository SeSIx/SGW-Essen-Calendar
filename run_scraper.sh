#!/usr/bin/env bash
# run_scraper.sh — scrape DSV, rebuild sgw_termine.ics, push if changed.
# Designed to be called from cron; logs go to stdout (redirect in crontab).

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$REPO_DIR/.venv"
PYTHON="$VENV/bin/python"
GIT_BRANCH="sgw-scraper-rewrite"

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

# Run the scraper
echo "[Scraper] Running main.py..."
"$PYTHON" main.py

# Check if the committed calendar file changed
if git diff --quiet HEAD -- sgw_termine.ics && \
   git ls-files --others --exclude-standard sgw_termine.ics | grep -q .; then
    # File is untracked (first run)
    CHANGED=true
elif ! git diff --quiet HEAD -- sgw_termine.ics; then
    CHANGED=true
else
    CHANGED=false
fi

if [ "$CHANGED" = "true" ]; then
    echo "[Git] sgw_termine.ics changed — committing and pushing..."
    git add sgw_termine.ics
    git -c user.name="SGW Bot" -c user.email="bot@sgw-essen.local" \
        commit -m "Auto-update: sgw_termine.ics $(date '+%Y-%m-%d %H:%M')"
    git push origin "$GIT_BRANCH"
    echo "[Git] Push successful."
else
    echo "[Git] No changes to sgw_termine.ics — nothing to push."
fi

echo "=== $(date '+%Y-%m-%d %H:%M:%S') DONE ==="
