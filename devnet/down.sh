#!/usr/bin/env bash
# devnet/down.sh — tear down the CC-2Jd self-devnet; leave no containers or networks.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT}"

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required" >&2
  exit 1
fi

# Same project name (cc-devnet). Down both files so a two-process up is not left
# behind, and so the shared cc-devnet-el volume is removed with -v.
echo "==> docker compose down -v --remove-orphans"
docker compose -f "${ROOT}/devnet/compose.yml" down -v --remove-orphans 2>/dev/null || true
docker compose -f "${ROOT}/devnet/compose.beacon-core.yml" down -v --remove-orphans 2>/dev/null || true

# Named network from compose.yml `networks.ccdev.name`
if docker network inspect cc-devnet >/dev/null 2>&1; then
  echo "==> removing leftover network cc-devnet"
  docker network rm cc-devnet 2>/dev/null || true
fi

# Residual containers with our project label / name prefix
leftover="$(docker ps -aq --filter "name=cc-devnet" 2>/dev/null || true)"
if [[ -n "${leftover}" ]]; then
  echo "==> removing leftover containers"
  # shellcheck disable=SC2086
  docker rm -f ${leftover} 2>/dev/null || true
fi

echo "devnet down: no cc-devnet containers or networks remain"
