#!/usr/bin/env bash
#
# Generates the "[YT Flow]" Grafana dashboards from the yt_dashboards
# definitions into this directory. Run this once before "docker compose up".
#
# It renders every flow dashboard registered with a Grafana backend through the
# Grafana backend, pointing the panels at the Prometheus datasource provisioned
# in datasource.yml.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! command -v ya >/dev/null 2>&1; then
    echo "'ya' not found in PATH." >&2
    exit 1
fi

# The dashboard definitions live in the same source tree; reference the generator
# relative to this script, so no repository root lookup is needed. The generator
# writes its output to ./generated.
cd "$script_dir"
generator=../../../../../../admin/dashboards/yt_dashboards/bin

# Every flow dashboard that has a grafana backend (the generator's `list` prints
# one "<slug> <backend>" row per pair), so no explicit list is maintained here.
mapfile -t dashboards < <(ya run "$generator" -- list | awk '$2 == "grafana" && $1 ~ /^flow-/ { print $1 }')

ya run "$generator" -- \
    --grafana-datasource '{"type": "prometheus", "uid": "ytflow-prometheus"}' \
    json "${dashboards[@]}" --backend grafana -f

# The generator offers no output-path option: it always writes to
# ./generated/grafana/<dashboard uid>.json. Move the artifacts into this
# directory, which is what docker-compose mounts into Grafana's dashboard
# provisioning path.
for f in generated/grafana/ytsaurus-flow-*.json; do
    out="ytflow-$(basename "$f" | sed 's/^ytsaurus-flow-//')"
    mv "$f" "$script_dir/$out"
    echo "Wrote $script_dir/$out"
done
rm -rf generated
