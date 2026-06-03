#!/usr/bin/env bash
# Downloads and stamps the Hydra devnet config for a single-node private Cardano network.
# Run once before `docker compose up`. Re-run to reset (wipes devnet/db).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEVNET="$REPO_ROOT/devnet"
HYDRA_BASE="https://raw.githubusercontent.com/cardano-scaling/hydra/master/hydra-cluster/config/devnet"

REQUIRED_FILES=(
  "cardano-node.json"
  "genesis-byron.json"
  "genesis-shelley.json"
  "genesis-alonzo.json"
  "genesis-conway.json"
  "kes.skey"
  "vrf.skey"
  "opcert.cert"
  "byron-delegate.key"
  "byron-delegation.cert"
)

echo "==> Preparing devnet in $DEVNET"
mkdir -p "$DEVNET" "$DEVNET/db" "$DEVNET/ipc"

for f in "${REQUIRED_FILES[@]}"; do
  dest="$DEVNET/$f"
  if [ ! -f "$dest" ]; then
    echo "    Downloading $f"
    curl -fsSL "$HYDRA_BASE/$f" -o "$dest"
  else
    echo "    Skipping $f (already present)"
  fi
done

# genesis-dijkstra.json is optional — present in newer Hydra configs
if curl -fsSL --head "$HYDRA_BASE/genesis-dijkstra.json" &>/dev/null; then
  if [ ! -f "$DEVNET/genesis-dijkstra.json" ]; then
    echo "    Downloading genesis-dijkstra.json"
    curl -fsSL "$HYDRA_BASE/genesis-dijkstra.json" -o "$DEVNET/genesis-dijkstra.json"
  fi
fi

echo "==> Stamping genesis start time"
UNIX_NOW=$(date +%s)
ISO_NOW=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
echo "    Byron startTime  : $UNIX_NOW"
echo "    Shelley systemStart: $ISO_NOW"

# BSD sed (macOS) requires a space between -i and the empty suffix
sed -i '' "s/\"startTime\": [0-9]*/\"startTime\": $UNIX_NOW/" "$DEVNET/genesis-byron.json"
sed -i '' "s/\"systemStart\": \"[^\"]*\"/\"systemStart\": \"$ISO_NOW\"/" "$DEVNET/genesis-shelley.json"

echo "==> Writing topology (isolated, no peers)"
cat > "$DEVNET/topology.json" <<'JSON'
{"localRoots": [], "publicRoots": [], "useLedgerAfterSlot": -1}
JSON

echo "==> Setting key file permissions"
chmod 0400 "$DEVNET"/*.skey "$DEVNET"/*.key 2>/dev/null || true

echo ""
echo "Done. Run:  docker compose up"
echo "Then:       cargo run -- --addr localhost:3001 --magic 42 --trace trace.jsonl"
