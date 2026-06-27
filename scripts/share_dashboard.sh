#!/usr/bin/env bash
#
# share_dashboard.sh — run the dashboard locally and expose it through an
# authenticated ngrok tunnel so a non-technical person can view it in a browser.
#
# Usage:
#   ./scripts/share_dashboard.sh                 # prompts for username + password
#   SHARE_USER=anna SHARE_PASS=secret ./scripts/share_dashboard.sh   # non-interactive
#
# Stop everything with Ctrl+C — both the dashboard and the tunnel are cleaned up.
#
# Prerequisites (one-time):
#   - ngrok installed and on PATH      (https://ngrok.com/download)
#   - ngrok authtoken configured        (ngrok config add-authtoken <token>)

set -euo pipefail

PORT=8050
TP_FILE="${HOME}/.config/ngrok/expense-tracker-tp.yml"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# --- preflight ---------------------------------------------------------------
command -v ngrok >/dev/null 2>&1 || { echo "❌ ngrok not found on PATH. Install it: https://ngrok.com/download"; exit 1; }
command -v poetry >/dev/null 2>&1 || { echo "❌ poetry not found on PATH."; exit 1; }
grep -q "authtoken" "${HOME}/.config/ngrok/ngrok.yml" 2>/dev/null || {
  echo "❌ No ngrok authtoken configured. Run once:"
  echo "   ngrok config add-authtoken <your-token>   (free at https://dashboard.ngrok.com)"
  exit 1
}

# --- credentials -------------------------------------------------------------
USER_NAME="${SHARE_USER:-}"
PASS_WORD="${SHARE_PASS:-}"
if [ -z "$USER_NAME" ]; then
  read -r -p "Username to share: " USER_NAME
fi
if [ -z "$PASS_WORD" ]; then
  read -r -s -p "Password to share: " PASS_WORD; echo
fi
[ -n "$USER_NAME" ] && [ -n "$PASS_WORD" ] || { echo "❌ Username and password must both be non-empty."; exit 1; }

# Write the ngrok traffic-policy file (basic auth) — kept outside the git repo,
# readable only by you.
mkdir -p "$(dirname "$TP_FILE")"
umask 077
cat > "$TP_FILE" <<YAML
on_http_request:
  - actions:
      - type: basic-auth
        config:
          realm: "Swiss Expense Tracker"
          credentials:
            - "${USER_NAME}:${PASS_WORD}"
YAML

# --- cleanup on exit ---------------------------------------------------------
# kill_tree kills a PID and all its descendants. Needed because the dashboard
# runs as `poetry → python`: a plain `kill` on the parent leaves python (and the
# bound port 8050) alive.
DASH_PID=""
NGROK_PID=""
kill_tree() {
  local pid="$1"; [ -n "$pid" ] || return 0
  local child
  for child in $(pgrep -P "$pid" 2>/dev/null); do kill_tree "$child"; done
  kill "$pid" 2>/dev/null || true
}
cleanup() {
  trap - INT TERM EXIT   # disarm so cleanup runs only once
  echo; echo "Shutting down…"
  kill_tree "$NGROK_PID"
  kill_tree "$DASH_PID"
  wait 2>/dev/null || true
  echo "Stopped."
}
trap cleanup INT TERM EXIT

# --- start dashboard (localhost only, debugger OFF) --------------------------
echo "Starting dashboard on 127.0.0.1:${PORT} …"
( cd "$PROJECT_DIR" && poetry run python -c \
    "from swiss_exp_tracker.app.app import app; app.run(host='127.0.0.1', port=${PORT}, debug=False)" \
    > /tmp/dashboard.log 2>&1 ) &
DASH_PID=$!

# wait until it actually serves
for _ in $(seq 1 90); do
  if curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${PORT}/" 2>/dev/null | grep -q "200"; then
    READY=1; break
  fi
  sleep 2
done
[ "${READY:-}" = "1" ] || { echo "❌ Dashboard failed to start. Last log:"; tail -20 /tmp/dashboard.log; exit 1; }
echo "✅ Dashboard up."

# --- start tunnel ------------------------------------------------------------
echo "Opening authenticated tunnel …"
ngrok http "$PORT" --traffic-policy-file "$TP_FILE" --log stdout --log-format json > /tmp/ngrok.log 2>&1 &
NGROK_PID=$!

# fetch the public URL from ngrok's local API
URL=""
for _ in $(seq 1 20); do
  # `|| true` is required: until ngrok's local API is up, curl/grep fail, and
  # under `set -euo pipefail` that would otherwise abort the whole script.
  URL=$(curl -s http://127.0.0.1:4040/api/tunnels 2>/dev/null \
        | grep -o '"public_url":"https://[^"]*"' | head -1 | sed 's/"public_url":"//;s/"//') || true
  [ -n "$URL" ] && break
  sleep 1
done
[ -n "$URL" ] || { echo "❌ Could not get tunnel URL. Last log:"; tail -15 /tmp/ngrok.log; exit 1; }

# --- done --------------------------------------------------------------------
cat <<INFO

────────────────────────────────────────────
  ✅ Dashboard is live — send these to your friend:

    URL:       ${URL}
    Username:  ${USER_NAME}
    Password:  (the one you just entered)

  First visit shows an ngrok "Visit Site" page — that's normal.
  Leave this terminal open. Press Ctrl+C to stop sharing.
────────────────────────────────────────────
INFO

# keep running until Ctrl+C
wait "$NGROK_PID"
