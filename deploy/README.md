# gflow studio deployment

The control plane and gflow MCP server are deliberately separate local-only
processes. Run them as one systemd unit pair and expose only the studio port
through Caddy or another HTTPS reverse proxy.

Docker Compose is also provided. It starts the same three-process topology
(`gflow-mcp`, `gflow-studio`, `gflow-worker`) and persists Chrome profiles,
generated output and studio state in named volumes. Complete the first Google
login inside the container environment before sending generation tasks. For
reCAPTCHA-sensitive accounts, the host/systemd setup with a real headed Chrome
session is preferred over Xvfb.

1. Copy `.env.example` to `.env` and replace every `replace-with-*` value.
2. Set `GFLOW_STUDIO_PUBLIC_BASE_URL` to the HTTPS origin users will access.
3. Install `gflow-mcp.service`, `gflow-studio.service` and `gflow-worker.service`.
4. Enable all three units; keep Uvicorn at `--workers 1`. The queue worker is a
   separate process, while the API process only reads the shared SQLite state.
5. Put `Caddyfile.example` in the Caddy configuration and reload Caddy.

If the web UI is hosted on another origin (for example a local DSH at
`http://127.0.0.1:3080`), add that exact origin to
`GFLOW_STUDIO_CORS_ORIGINS`; do not use `*` together with credentialed browser
requests.

Generated files are served through signed HTTPS URLs. For multi-host or large
video deployments, set `GFLOW_CLI_STORAGE_URI` to an S3-compatible/R2 or GCS
prefix. The image includes the S3 extra, and the studio proxy streams only
objects under that configured prefix; it never accepts an arbitrary bucket URL.
Keep the provider credentials in the service environment (for example,
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL` for R2), not in
the browser or task payload.

For Docker, copy `.env.example` to the deployment directory, fill the required
secrets and run `docker compose -f deploy/docker-compose.yml up -d --build`.

For monitoring, import `prometheus.yml.example` and
`gflow-alerts.yml.example` into Prometheus. The endpoint is protected by the
same Bearer API key. Install and enable `gflow-backup.service` and
`gflow-backup.timer` after setting `GFLOW_STUDIO_BACKUP_DIR`; backups contain
the SQLite state, authenticated Chrome profiles, uploaded input assets, and
local generated output (when cloud storage is not configured), so protect that
directory as secrets. Each backup also contains `manifest.txt` with SHA-256
checksums; verify it before migrating or restoring a host. Cloud-backed output
is not duplicated locally and must be retained in the configured bucket.

The public endpoints are `/`, `/mcp`, `/v1/models`, `/v1/responses`,
`/v1/images/generations`, and `/v1/videos/generations`. All require the configured API key or the web
admin session, except the health and signed media URLs. Do the first Google
login on the same host that runs the worker so its Chrome profiles are present.
