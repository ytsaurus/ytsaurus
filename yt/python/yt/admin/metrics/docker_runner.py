import yt.logger as logger
import yt.wrapper as yt
from yt.admin.metrics.config import (
    REPLAY_LABEL_FILTER,
    REPLAY_LABELS,
    REPLAY_PREFIX,
)

from contextlib import contextmanager
from typing import Optional
import hashlib
import os
import shutil
import signal
import time


docker = None
docker_errors = None


def _lazy_import_modules():
    global docker, docker_errors
    if not docker:
        try:
            import docker as _docker
            from docker import errors as _docker_errors
            docker = _docker
            docker_errors = _docker_errors
        except ImportError as e:
            from yt.admin.helpers import install_hint
            raise ImportError(
                "The 'docker' package is required for DockerReplayRunner. "
                f"Install it with: {install_hint('docker')}"
            ) from e


@contextmanager
def ignore_sigint():
    previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        yield
    finally:
        signal.signal(signal.SIGINT, previous)


class DockerReplayRunner:
    def __init__(self, archive_path: str):
        _lazy_import_modules()

        try:
            self._client = docker.from_env()
            self._client.ping()
        except docker_errors.DockerException as e:
            raise SystemExit(f"Cannot talk to Docker daemon: {e}")

        suffix = hashlib.sha1(os.path.abspath(archive_path).encode()).hexdigest()[:8]
        self.network_name = f"{REPLAY_PREFIX}-{suffix}"
        self.prometheus_name = f"{REPLAY_PREFIX}-prometheus-{suffix}"
        self.grafana_name = f"{REPLAY_PREFIX}-grafana-{suffix}"
        self._network = None
        self._prometheus = None
        self._grafana = None
        self.prometheus_host_port: Optional[int] = None
        self.grafana_host_port: Optional[int] = None

    def __enter__(self) -> "DockerReplayRunner":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with ignore_sigint():
            self.stop()

    def cleanup_stale(self) -> None:
        for container in self._client.containers.list(all=True, filters=REPLAY_LABEL_FILTER):
            try:
                container.remove(force=True)
            except docker_errors.NotFound:
                pass
            except docker_errors.APIError as e:
                logger.debug(f"Could not remove stale container {container.name}: {e}")
        for network in self._client.networks.list(filters=REPLAY_LABEL_FILTER):
            try:
                network.remove()
            except docker_errors.APIError as e:
                logger.debug(f"Could not remove stale network {network.name}: {e}")

    def import_openmetrics(self, om_path: str, tsdb_dir: str) -> None:
        if os.path.isdir(tsdb_dir):
            shutil.rmtree(tsdb_dir)
        os.makedirs(tsdb_dir, exist_ok=True)

        logger.info("Converting .om to TSDB blocks via promtool")
        container = self._client.containers.run(
            yt.config["admin"]["prometheus_image"],
            entrypoint=["/bin/promtool"],
            command=["tsdb", "create-blocks-from", "openmetrics", "/import/data.om", "/data"],
            volumes={
                os.path.abspath(os.path.dirname(om_path)): {"bind": "/import", "mode": "ro"},
                os.path.abspath(tsdb_dir): {"bind": "/data", "mode": "rw"},
            },
            user=f"{os.getuid()}:{os.getgid()}",
            labels=REPLAY_LABELS,
            detach=True,
        )
        try:
            result = container.wait()
            output = container.logs().decode(errors="replace")
        finally:
            try:
                container.remove(force=True)
            except docker_errors.NotFound:
                pass
            except docker_errors.APIError as e:
                logger.debug(f"Could not remove promtool importer: {e}")
        if result.get("StatusCode") != 0:
            raise SystemExit(f"promtool failed: {output}")
        logger.debug(output)

    def start_prometheus(self, tsdb_dir: str, host_port: Optional[int]) -> int:
        self._network = self._client.networks.create(
            self.network_name,
            driver="bridge",
            labels=REPLAY_LABELS,
        )
        port_label = host_port if host_port is not None else "auto"
        logger.info(f"Starting Prometheus (host port {port_label})")
        self._prometheus = self._client.containers.run(
            yt.config["admin"]["prometheus_image"],
            command=[
                "--config.file=/etc/prometheus/prometheus.yml",
                "--storage.tsdb.path=/prometheus",
                "--storage.tsdb.retention.time=10y",
                "--web.enable-admin-api",
            ],
            name=self.prometheus_name,
            detach=True,
            auto_remove=True,
            network=self.network_name,
            ports={"9090/tcp": host_port},
            user=f"{os.getuid()}:{os.getgid()}",
            volumes={tsdb_dir: {"bind": "/prometheus", "mode": "rw"}},
            labels=REPLAY_LABELS,
        )
        self._wait_running(self._prometheus, "Prometheus")
        self.prometheus_host_port = self._published_port(self._prometheus, "9090/tcp")
        return self.prometheus_host_port

    def start_grafana(self, provisioning_path: str, dashboards_path: str, host_port: Optional[int]) -> int:
        port_label = host_port if host_port is not None else "auto"
        logger.info(f"Starting Grafana (host port {port_label})")
        self._grafana = self._client.containers.run(
            yt.config["admin"]["grafana_image"],
            name=self.grafana_name,
            detach=True,
            auto_remove=True,
            network=self.network_name,
            ports={"3000/tcp": host_port},
            environment={
                "GF_AUTH_ANONYMOUS_ENABLED": "true",
                "GF_AUTH_ANONYMOUS_ORG_ROLE": "Admin",
                "GF_AUTH_DISABLE_LOGIN_FORM": "true",
                "GF_SECURITY_ADMIN_PASSWORD": "admin",
            },
            volumes={
                provisioning_path: {"bind": "/etc/grafana/provisioning", "mode": "ro"},
                dashboards_path: {"bind": "/var/lib/grafana/dashboards", "mode": "ro"},
            },
            labels=REPLAY_LABELS,
        )
        self._wait_running(self._grafana, "Grafana")
        self.grafana_host_port = self._published_port(self._grafana, "3000/tcp")
        return self.grafana_host_port

    def watch(self, poll_interval: float = 2.0) -> None:
        while True:
            time.sleep(poll_interval)
            exited = []
            for container, name in ((self._prometheus, "Prometheus"), (self._grafana, "Grafana")):
                container.reload()
                if container.status != "running":
                    exited.append((container, name))
            if exited:
                for container, name in exited:
                    logger.error(f"{name} exited:\n{container.logs(tail=80).decode(errors='replace')}")
                return

    def stop(self) -> None:
        for container, name in ((self._grafana, "Grafana"), (self._prometheus, "Prometheus")):
            if container is None:
                continue
            try:
                container.stop(timeout=5)
            except docker_errors.NotFound:
                pass
            except Exception as e:
                logger.debug(f"Could not stop {name}: {e}")
        if self._network is not None:
            for attempt in range(4, -1, -1):
                try:
                    self._network.remove()
                    break
                except docker_errors.NotFound:
                    break
                except Exception as e:
                    if not attempt:
                        logger.debug(f"Could not remove network {self._network.name}: {e}")
                    time.sleep(0.5)

    def _wait_running(self, container, label: str, timeout: float = 30.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            container.reload()
            if container.status == "running":
                return
            if container.status in ("exited", "dead"):
                logs = container.logs().decode(errors="replace")
                raise SystemExit(f"{label} exited unexpectedly:\n{logs}")
            time.sleep(0.5)
        raise SystemExit(f"{label} did not reach 'running' within {timeout} seconds")

    @staticmethod
    def _published_port(container, internal: str) -> int:
        container.reload()
        bindings = container.attrs["NetworkSettings"]["Ports"][internal]
        return int(bindings[0]["HostPort"])
