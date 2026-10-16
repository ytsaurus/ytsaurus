import yt.logger as logger
from yt.admin.metrics.config import MetricsReplayConfig
from yt.admin.metrics.docker_runner import DockerReplayRunner, ignore_sigint
from yt.admin.metrics.grafana import GrafanaProvisioner, build_dashboard_url
from yt.admin.metrics.spec import params_by_dashboard

import json
import os
import shutil
import tempfile
import zipfile
from typing import Any, Dict, Optional, Tuple


class MetricsReplayer:
    def run(self, cfg: MetricsReplayConfig) -> None:
        if not os.path.isfile(cfg.archive):
            raise FileNotFoundError(f"Archive not found: {cfg.archive}")

        workdir = tempfile.mkdtemp(prefix="yt-metrics-replay-")
        try:
            with zipfile.ZipFile(cfg.archive, "r") as zf:
                zf.extractall(workdir)
            logger.info(f"Extracted archive to {workdir}")

            om_path = os.path.join(workdir, "data.om")
            if not os.path.isfile(om_path):
                raise RuntimeError(f"data.om missing in archive {cfg.archive}")

            meta, from_ms, to_ms = self._load_meta(workdir)
            params_by_dash = params_by_dashboard(meta.get("spec", {}))

            with DockerReplayRunner(cfg.archive) as runner:
                runner.cleanup_stale()
                tsdb_dir = os.path.join(workdir, "prometheus-data")
                runner.import_openmetrics(om_path, tsdb_dir)
                prom_port = runner.start_prometheus(tsdb_dir, cfg.prometheus_port)

                provisioner = GrafanaProvisioner(workdir)
                dashboards = provisioner.write(f"http://{runner.prometheus_name}:9090")
                grafana_port = runner.start_grafana(
                    provisioner.grafana_provisioning_path,
                    provisioner.grafana_dashboards_patched_path,
                    cfg.grafana_port,
                )

                logger.info(f"Prometheus: http://localhost:{prom_port}")
                logger.info(f"Grafana:    http://localhost:{grafana_port} (anonymous Admin)")
                host = f"http://localhost:{grafana_port}"
                for d in dashboards:
                    params = params_by_dash.get(d.file, {})
                    logger.info(f"  {d.title}: {build_dashboard_url(host, d, params, from_ms, to_ms)}")
                logger.info("Ctrl+C to stop.")

                try:
                    runner.watch()
                except KeyboardInterrupt:
                    pass
        finally:
            with ignore_sigint():
                shutil.rmtree(workdir, ignore_errors=True)

    @staticmethod
    def _load_meta(workdir: str) -> Tuple[Dict[str, Any], Optional[int], Optional[int]]:
        meta_path = os.path.join(workdir, "meta.json")
        if not os.path.isfile(meta_path):
            return {}, None, None
        with open(meta_path) as f:
            meta = json.load(f)
        from_ms = int(float(meta.get("start_unix", 0)) * 1000) if meta.get("start_unix") else None
        to_ms = int(float(meta.get("end_unix", 0)) * 1000) if meta.get("end_unix") else None
        return meta, from_ms, to_ms
