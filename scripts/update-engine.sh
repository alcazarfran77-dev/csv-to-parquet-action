#!/usr/bin/env bash
# Copies the conversion engine from the main "csv-to-parquet" repository into src/.
# Usage: scripts/update-engine.sh ../csv-to-parquet
set -euo pipefail
APP="${1:?path to the csv-to-parquet repository}"
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cp "$APP/desktop/engine.py" "$APP/desktop/i18n.py" "$HERE/src/"
cp "$APP/locales/en.json" "$APP/locales/es.json" "$HERE/src/locales/"
echo "Engine updated from $APP ($(cd "$APP" && git rev-parse --short HEAD 2>/dev/null || echo 'not a git checkout'))"
