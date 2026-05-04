#!/bin/bash
set -e

# start_all.sh polls netstat for each service before returning,
# so when it exits the full desktop stack (Xvfb, Mutter, x11vnc) is confirmed ready.
./start_all.sh
./novnc_startup.sh

echo "Agent container desktop ready — starting agent server on port 8001"
exec uvicorn agent_server:app --host 0.0.0.0 --port 8001
