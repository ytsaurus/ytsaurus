#!/usr/bin/env python3
# Local stand-in for the monitoring aggregation layer.
#
# The flow dashboards read host="Aggr" series that the production monitoring backend computes by
# aggregating the per-host series. Each sensor declares how it aggregates across hosts via its
# yt_aggr label (sum / max / min / avg); Prometheus cannot express "aggregate every metric, keeping
# its name" in one rule, so this sidecar polls the metric names with their yt_aggr and generates one
# recording rule per name, using the matching operator:
#
#     record: <name>
#     expr: <op> without(host, instance) ({__name__="<name>", host!="Aggr"})
#     labels: {host: Aggr}
#
# Runs inside a plain python image next to Prometheus. Configuration is taken from the environment so
# the same script works against the test's dynamically chosen Prometheus port.

import json
import os
import time
import urllib.parse
import urllib.request

PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
RULES_PATH = os.environ.get("AGGR_RULES_PATH", "/rules/host-aggr.yml")
# Short period so the host="Aggr" series (which every dashboard queries) appear quickly after start.
# Matches Prometheus' scrape/evaluation interval (5s) -- polling faster would not surface data any
# sooner (that floor gates it) and would only churn Prometheus rule reloads.
POLL_PERIOD_SECONDS = 5

# Prometheus aggregation operator per yt_aggr value; an unknown/absent value falls back to sum.
AGGR_OPERATORS = {"sum": "sum", "max": "max", "min": "min", "avg": "avg"}


def fetch_metric_aggr():
    # Map each metric name to its yt_aggr (the sensor's cross-host aggregation). Excludes the
    # leader-only yt.flow.controller.* sensors, which carry no host dimension.
    query = urllib.parse.urlencode({"query": 'count by (__name__, yt_aggr) ({__name__!="", host!="Aggr"})'})
    with urllib.request.urlopen(f"{PROMETHEUS}/api/v1/query?{query}") as rsp:
        result = json.load(rsp)["data"]["result"]
    metrics = {}
    for series in result:
        name = series["metric"].get("__name__")
        if not name or name.startswith("yt_flow_controller_"):
            continue
        metrics[name] = series["metric"].get("yt_aggr", "sum")
    return metrics


def render_rules(metrics):
    lines = [
        "groups:",
        "  - name: host_aggr",
        "    interval: 5s",
        "    rules:",
    ]
    for name in sorted(metrics):
        op = AGGR_OPERATORS.get(metrics[name], "sum")
        lines += [
            f"      - record: {name}",
            f'        expr: {op} without(host, instance) ({{__name__="{name}", host!="Aggr"}})',
            "        labels:",
            "          host: Aggr",
        ]
    return "\n".join(lines) + "\n"


def main():
    previous = None
    while True:
        try:
            rules = render_rules(fetch_metric_aggr())
            if rules != previous:
                with open(RULES_PATH, "w") as f:
                    f.write(rules)
                urllib.request.urlopen(
                    urllib.request.Request(f"{PROMETHEUS}/-/reload", method="POST"))
                previous = rules
                print(f"updated {RULES_PATH}, reloaded prometheus", flush=True)
        except Exception as error:
            print(f"aggr-rules iteration failed: {error}", flush=True)
        time.sleep(POLL_PERIOD_SECONDS)


if __name__ == "__main__":
    main()
