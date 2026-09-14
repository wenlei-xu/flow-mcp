#!/usr/bin/env bash
set -euo pipefail

BACKUP_ROOT="${GFLOW_STUDIO_BACKUP_DIR:-$HOME/gflow-backups}"
STUDIO_STATE="${GFLOW_STUDIO_STATE_DIR:-$PWD}"
GFLOW_HOME="${GFLOW_CLI_HOME:-$HOME/.local/share/gflow-cli}"
STUDIO_DB="${GFLOW_STUDIO_DB_PATH:-$STUDIO_STATE/studio.db}"
STUDIO_UPLOADS="${GFLOW_STUDIO_UPLOAD_DIR:-$STUDIO_STATE/uploads}"
GFLOW_OUTPUT="${GFLOW_CLI_OUTPUT_DIR:-$HOME/Downloads/gflow-cli}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="$BACKUP_ROOT/$STAMP"
mkdir -p "$DEST"

if [[ -f "$STUDIO_DB" ]]; then
  if command -v sqlite3 >/dev/null 2>&1; then
    sqlite3 "$STUDIO_DB" ".backup '$DEST/studio.db'"
  else
    python3 - "$STUDIO_DB" "$DEST/studio.db" <<'PY'
import sqlite3
import sys

source, target = sys.argv[1:]
with sqlite3.connect(source) as src, sqlite3.connect(target) as dst:
    src.backup(dst)
PY
  fi
fi
tar -czf "$DEST/gflow-cli-home.tgz" -C "$(dirname "$GFLOW_HOME")" "$(basename "$GFLOW_HOME")"
if [[ -d "$STUDIO_UPLOADS" ]]; then
  tar -czf "$DEST/studio-uploads.tgz" -C "$(dirname "$STUDIO_UPLOADS")" "$(basename "$STUDIO_UPLOADS")"
fi
# Local generated output is included only when it exists. Deployments using
# GFLOW_CLI_STORAGE_URI keep the authoritative result in the configured bucket.
if [[ -d "$GFLOW_OUTPUT" && -z "${GFLOW_CLI_STORAGE_URI:-}" ]]; then
  tar -czf "$DEST/gflow-output.tgz" -C "$(dirname "$GFLOW_OUTPUT")" "$(basename "$GFLOW_OUTPUT")"
fi
{
  echo "format=gflow-studio-backup-v2"
  echo "created_at=$STAMP"
  echo "studio_db=$STUDIO_DB"
  echo "studio_uploads=$STUDIO_UPLOADS"
  echo "gflow_home=$GFLOW_HOME"
  echo "gflow_output=$GFLOW_OUTPUT"
  for artifact in "$DEST"/*; do
    [[ -f "$artifact" ]] || continue
    sha256sum "$artifact"
  done
} > "$DEST/manifest.txt"
find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -mtime +14 -exec rm -rf -- {} +
echo "backup created: $DEST"
