"""Optional Prometheus + Grafana monitoring stack for the integration tests.

Enabled with ``--test-param MONITORING_STACK=1``. When on, ``start_flow_process_federation`` brings up
four host-networked Docker containers (via docker-compose) alongside the running pipeline:

  * Prometheus -- scrapes every flow controller/worker at ``/solomon_proxy/sensors`` (the combined
    node + companion endpoint; targets supplied via file-based service discovery once ports are known);
  * aggr-rules -- generates the ``host="Aggr"`` recording rules the flow dashboards query;
  * Grafana    -- pre-provisioned Prometheus datasource + the flow dashboards;
  * reaper     -- force-removes the whole stack once the test process exits, however it exits
    (clean finish, Ctrl-C, or SIGKILL); see the compose file.

The stack config is shared with the docker example (``yt/yt/flow/docker/monitoring``, embedded here via
that module's ``RESOURCE_FILES``). The grafana dashboards are NOT bundled: the user renders them once
with ``docker/monitoring/grafana/dashboards/generate.sh`` and passes the output directory via
``--test-param MONITORING_DASHBOARDS_DIR=<dir>``.

Local debugging aid only: it needs docker on the host, and the federation teardown holds until Ctrl-C
so the metrics stay visible. Off by default; never runs under autocheck.
"""

import json
import logging
import os
import shutil
import socket
import subprocess
import sys

import yatest.common

import yt.wrapper as yt

log = logging.getLogger("monitoring_stack")

MONITORING_STACK_ENABLED = yatest.common.get_param("MONITORING_STACK", "0") == "1"

# Directory with the pre-rendered flow grafana dashboards (``*.json``), produced by the user with
# ``docker/monitoring/grafana/dashboards/generate.sh``. Required when the stack is enabled.
MONITORING_DASHBOARDS_DIR = yatest.common.get_param("MONITORING_DASHBOARDS_DIR", None)

# Well-known defaults so a pre-opened firewall hole / SSH tunnel matches without extra configuration.
DEFAULT_PROMETHEUS_PORT = 9090
DEFAULT_GRAFANA_PORT = 3000

# Direct link to the general flow dashboard (uid/slug produced by the generator), on the laptop-side
# port 3000 the ssh forward maps to.
GENERAL_DASHBOARD_URL = "http://localhost:3000/d/ytsaurus-flow-general/yt-flow-general"

AGGR_RULES_IMAGE = "python:3-alpine"

# Reaper image: needs the docker CLI + a POSIX shell to watch the owner PID and remove the stack.
REAPER_IMAGE = "docker:cli"

_RESOURCE_PREFIX = "yt/yt/flow/docker/monitoring/"


# ANSI colors for the startup banner.
_BOLD = "\033[1m"
_GREEN = "\033[32m"
_CYAN = "\033[36m"
_YELLOW = "\033[33m"
_RESET = "\033[0m"


def _print_banner(text):
    # Write straight to the controlling terminal (like FlowTestBase.try_print_tty) so the banner shows
    # without --test-stderr: ya captures the test's stdout/stderr, but /dev/tty bypasses that. Fall
    # back to stderr where there is no tty.
    tty = "CONOUT$" if os.name == "nt" else "/dev/tty"
    try:
        if os.path.exists(tty):
            with open(tty, "w") as term:
                term.write(text + "\n")
                return
    except Exception:
        pass
    print(text, file=sys.stderr, flush=True)


class MonitoringStack:
    """Lifecycle of the Prometheus/Grafana/aggr-rules docker-compose stack for one federation."""

    def __init__(self, logs_dir, port_manager):
        self._port_manager = port_manager
        self._dir = os.path.join(logs_dir, "monitoring")
        self._compose_file = os.path.join(self._dir, "docker-compose.yml")
        self._started = False
        self._prometheus_port = None
        self._grafana_port = None
        self._project = None

    @property
    def started(self):
        return self._started

    def start(self, federation):
        self._start(federation)
        self._started = True
        self.log_urls()

    def log_urls(self):
        if not self._started:
            return
        host = socket.getfqdn()
        line = "=" * 78
        banner = "\n".join(
            [
                "",
                f"{_BOLD}{_GREEN}{line}{_RESET}",
                f"{_BOLD}{_GREEN}  YT Flow monitoring stack is UP{_RESET}",
                f"    Grafana:    {_CYAN}http://localhost:{self._grafana_port}{_RESET}  (anonymous Admin)",
                f"    Prometheus: {_CYAN}http://localhost:{self._prometheus_port}{_RESET}",
                "",
                f"{_BOLD}  Forward Grafana to port 3000 on your laptop:{_RESET}",
                f"    {_YELLOW}ssh -N -L 3000:localhost:{self._grafana_port} {host}{_RESET}",
                f"  then open the general dashboard: {_CYAN}{GENERAL_DASHBOARD_URL}{_RESET}",
                f"{_BOLD}{_GREEN}{line}{_RESET}",
                "",
            ]
        )
        _print_banner(banner)

    def notify_hold(self):
        # Printed once the test body has finished and the federation teardown is holding, so it is
        # clear the run is done and it is safe to Ctrl-C.
        if not self._started:
            return
        line = "=" * 78
        banner = "\n".join(
            [
                "",
                f"{_BOLD}{_GREEN}{line}{_RESET}",
                f"{_BOLD}{_GREEN}  Test finished -- monitoring stack still running for inspection.{_RESET}",
                f"    General dashboard (after the ssh forward above): {_CYAN}{GENERAL_DASHBOARD_URL}{_RESET}",
                f"{_BOLD}{_YELLOW}  Press Ctrl-C to stop the test and tear the stack down.{_RESET}",
                f"{_BOLD}{_GREEN}{line}{_RESET}",
                "",
            ]
        )
        _print_banner(banner)

    def _pick_port(self, preferred, name):
        # Prefer the well-known port so a pre-configured tunnel matches; fall back to any free port.
        if self._port_manager.is_port_free(preferred):
            return preferred
        port = self._port_manager.get_port()
        log.warning(
            "Default %s port %d is busy; using %d instead. A pre-opened firewall hole or SSH "
            "tunnel for port %d will NOT reach %s -- forward %d instead.",
            name,
            preferred,
            port,
            preferred,
            name,
            port,
        )
        return port

    def _start(self, federation):
        self._prometheus_port = self._pick_port(DEFAULT_PROMETHEUS_PORT, "Prometheus")
        self._grafana_port = self._pick_port(DEFAULT_GRAFANA_PORT, "Grafana")
        self._project = f"ytflow-mon-{self._prometheus_port}"

        self._materialize_assets()
        self._write_targets(federation)
        self._install_dashboards()
        os.makedirs(os.path.join(self._dir, "rules"), exist_ok=True)

        # The reaper only comes up under the `test` profile (the example does not use it).
        self._compose("--profile", "test", "up", "-d")

    def _install_dashboards(self):
        # Grafana mounts self._dir/grafana/dashboards; fill it from the user-prepared directory. The
        # dashboards are intentionally not bundled -- rendering them needs the yt_dashboards generator,
        # which we do not want in every flow test's build graph.
        if not MONITORING_DASHBOARDS_DIR:
            raise RuntimeError(
                "MONITORING_STACK=1 needs pre-rendered dashboards: run "
                "yt/yt/flow/docker/monitoring/grafana/dashboards/generate.sh, then pass "
                "--test-param MONITORING_DASHBOARDS_DIR=<absolute path to that directory>"
            )
        # The test runs in a sandbox whose cwd is not your checkout, so a relative path would resolve
        # against the wrong directory -- require an absolute one.
        if not os.path.isabs(MONITORING_DASHBOARDS_DIR):
            raise RuntimeError(
                "MONITORING_DASHBOARDS_DIR must be an absolute path (the test runs in a sandbox, not "
                "your checkout); got {!r}".format(MONITORING_DASHBOARDS_DIR)
            )
        if not os.path.isdir(MONITORING_DASHBOARDS_DIR):
            raise RuntimeError("MONITORING_DASHBOARDS_DIR={!r} is not a directory".format(MONITORING_DASHBOARDS_DIR))
        names = [f for f in os.listdir(MONITORING_DASHBOARDS_DIR) if f.endswith(".json")]
        if not names:
            raise RuntimeError(
                "No *.json dashboards in MONITORING_DASHBOARDS_DIR={!r}; run generate.sh there first".format(
                    MONITORING_DASHBOARDS_DIR
                )
            )
        dst = os.path.join(self._dir, "grafana", "dashboards")
        os.makedirs(dst, exist_ok=True)
        for name in names:
            shutil.copyfile(os.path.join(MONITORING_DASHBOARDS_DIR, name), os.path.join(dst, name))

    def _materialize_assets(self):
        # Keep Arcadia-only resource loading lazy so CMake tests can import the disabled monitoring stack.
        from library.python import resource

        # Write the shared config into the log dir so docker can mount it: the compose file,
        # prometheus.yml, aggr_rules.py and the grafana provisioning (datasource + dashboards provider).
        for key in resource.resfs_files(prefix=_RESOURCE_PREFIX):
            if isinstance(key, bytes):
                key = key.decode("utf-8")
            rel = key[len(_RESOURCE_PREFIX) :]
            out = os.path.join(self._dir, rel)
            os.makedirs(os.path.dirname(out), exist_ok=True)
            with open(out, "wb") as f:
                f.write(resource.resfs_read(key))

    def _write_targets(self, federation):
        # Prometheus file-based service discovery: it reads flow_server.json (see prometheus.yml) to
        # learn what to scrape, so we publish the federation's dynamic monitoring ports here. Both the
        # controller/worker node sensors and the companion are served by the same /solomon_proxy/sensors
        # endpoint, so a single target list covers them.
        targets_dir = os.path.join(self._dir, "targets")
        os.makedirs(targets_dir, exist_ok=True)

        flow_targets = []
        for service, processes in (("controller", federation.controllers), ("worker", federation.workers)):
            for process in processes:
                flow_targets.append(
                    {
                        "targets": [f"localhost:{process.monitoring_port}"],
                        "labels": {"service": service},
                    }
                )

        with open(os.path.join(targets_dir, "flow_server.json"), "w") as f:
            json.dump(flow_targets, f)

    def _compose(self, *args):
        env = dict(os.environ)
        env.update(
            {
                "PROMETHEUS_IMAGE": yt.config["admin"]["prometheus_image"],
                "GRAFANA_IMAGE": yt.config["admin"]["grafana_image"],
                "AGGR_RULES_IMAGE": AGGR_RULES_IMAGE,
                "REAPER_IMAGE": REAPER_IMAGE,
                "PROMETHEUS_PORT": str(self._prometheus_port),
                "GRAFANA_PORT": str(self._grafana_port),
                # Consumed by the reaper service: whom to watch and what to remove when it dies.
                "OWNER_PID": str(os.getpid()),
                "COMPOSE_PROJECT": self._project,
            }
        )
        result = subprocess.run(
            ["docker", "compose", "-p", self._project, "-f", self._compose_file, *args],
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            # Surface docker's own diagnostic (daemon down, image pull failure, port in use); a bare
            # CalledProcessError would drop it.
            raise RuntimeError(
                "`docker compose {}` failed (rc={}): {}".format(
                    " ".join(args), result.returncode, result.stderr.strip()
                )
            )
