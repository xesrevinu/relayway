#!/bin/sh
set -eu

if [ -z "${RELAYWAY_PROVIDER_IDS:-}" ]; then
  exec /relayway-proxy "$@"
fi

pids=""
for id in $(printf '%s' "$RELAYWAY_PROVIDER_IDS" | tr ',' ' '); do
  upper="$(printf '%s' "$id" | tr '[:lower:]-' '[:upper:]_')"
  eval "base_url=\${RELAYWAY_PROVIDER_${upper}_BASE_URL:-}"
  eval "admin_password=\${RELAYWAY_PROVIDER_${upper}_ADMIN_PASSWORD:-}"
  eval "token=\${RELAYWAY_PROVIDER_${upper}_TOKEN:-}"
  eval "token_file=\${RELAYWAY_PROVIDER_${upper}_TOKEN_FILE:-/data/$id/token}"
  eval "mode=\${RELAYWAY_PROVIDER_${upper}_MODE:-global}"
  eval "airport_id=\${RELAYWAY_PROVIDER_${upper}_AIRPORT_ID:-}"
  eval "socks_addr=\${RELAYWAY_PROVIDER_${upper}_SOCKS_ADDR:-}"
  eval "api_addr=\${RELAYWAY_PROVIDER_${upper}_API_ADDR:-}"
  eval "provider_server=\${RELAYWAY_PROVIDER_${upper}_PROVIDER_SERVER:-127.0.0.1}"
  eval "provider_port=\${RELAYWAY_PROVIDER_${upper}_PROVIDER_PORT:-}"
  eval "provider_password=\${RELAYWAY_PROVIDER_${upper}_PROVIDER_PASSWORD:-$id}"
  eval "provider_prefix=\${RELAYWAY_PROVIDER_${upper}_PROVIDER_PREFIX:-$upper}"
  eval "relayway_socks_addr=\${RELAYWAY_PROVIDER_${upper}_RELAYWAY_SOCKS_ADDR:-}"
  eval "health_check_urls=\${RELAYWAY_PROVIDER_${upper}_HEALTH_CHECK_URLS:-${RELAYWAY_HEALTH_CHECK_URLS:-}}"

  if [ -z "$base_url" ] || [ -z "$socks_addr" ] || [ -z "$api_addr" ] || [ -z "$provider_port" ] || [ -z "$relayway_socks_addr" ]; then
    echo "relayway provider '$id' is missing required config" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$token_file")"
  (
    export RELAYWAY_PROVIDER_ID="$id"
    export RELAYWAY_BASE_URL="$base_url"
    export RELAYWAY_ADMIN_PASSWORD="$admin_password"
    export RELAYWAY_TOKEN="$token"
    export RELAYWAY_TOKEN_FILE="$token_file"
    export RELAYWAY_MODE="$mode"
    export RELAYWAY_CORED_AIRPORT_ID="$airport_id"
    export RELAYWAY_SOCKS_ADDR="$socks_addr"
    export RELAYWAY_API_ADDR="$api_addr"
    export RELAYWAY_PROVIDER_SERVER="$provider_server"
    export RELAYWAY_PROVIDER_PORT="$provider_port"
    export RELAYWAY_PROVIDER_PASSWORD="$provider_password"
    export RELAYWAY_PROVIDER_NAME_PREFIX="$provider_prefix"
    export RELAYWAY_RELAYWAY_SOCKS_ADDR="$relayway_socks_addr"
    if [ -n "$health_check_urls" ]; then
      export RELAYWAY_HEALTH_CHECK_URLS="$health_check_urls"
    fi
    /relayway-proxy "$@" 2>&1 | while IFS= read -r line; do
      printf '[%s] %s\n' "$id" "$line"
    done
  ) &
  pids="$pids $!"
done

trap 'kill $pids 2>/dev/null || true' INT TERM EXIT
wait
