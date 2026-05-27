import yt.logger as logger
from yt.admin.metrics.config import DashboardInfo, DATASOURCE_NAME, DATASOURCE_UID, GRAFANA_PROVISIONING_FILE

from typing import Dict, List, Optional
import json
import os
import re


def _slugify(title: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", title).strip("-").lower()
    return slug or "dashboard"


def build_dashboard_url(host: str, dashboard: DashboardInfo, params: Dict[str, str], from_ms: Optional[int], to_ms: Optional[int]) -> str:
    qs_parts: List[str] = []
    if from_ms is not None and to_ms is not None:
        qs_parts.extend([f"from={from_ms}", f"to={to_ms}"])
    qs_parts.extend(f"var-{k}={v}" for k, v in sorted(params.items()))
    qs = ("?" + "&".join(qs_parts)) if qs_parts else ""
    return f"{host}/d/{dashboard.uid}/{dashboard.slug}{qs}"


class GrafanaProvisioner:
    def __init__(self, workdir: str):
        self.workdir = workdir
        self.grafana_provisioning_path = os.path.join(workdir, "grafana-provisioning")
        self.grafana_dashboards_patched_path = os.path.join(workdir, "grafana-dashboards-patched")

    def write(self, prometheus_url: str) -> List[DashboardInfo]:
        try:
            import yaml
        except ImportError as e:
            from yt.admin.helpers import install_hint
            raise ImportError(
                "The 'yaml' package is required for GrafanaProvisioner. "
                f"Install it with: {install_hint('yaml')}"
            ) from e

        datasources_path = os.path.join(self.grafana_provisioning_path, "datasources")
        dashboard_providers_path = os.path.join(self.grafana_provisioning_path, "dashboards")
        for d in (datasources_path, dashboard_providers_path, self.grafana_dashboards_patched_path):
            os.makedirs(d, exist_ok=True)

        with open(os.path.join(datasources_path, GRAFANA_PROVISIONING_FILE), "w") as f:
            yaml.safe_dump(
                {
                    "apiVersion": 1,
                    "datasources": [
                        {
                            "name": DATASOURCE_NAME,
                            "uid": DATASOURCE_UID,
                            "type": "prometheus",
                            "access": "proxy",
                            "url": prometheus_url,
                            "isDefault": True,
                            "editable": False,
                            "jsonData": {"timeInterval": "15s"},
                        }
                    ],
                },
                f,
            )

        with open(os.path.join(dashboard_providers_path, GRAFANA_PROVISIONING_FILE), "w") as f:
            yaml.safe_dump(
                {
                    "apiVersion": 1,
                    "providers": [
                        {
                            "name": "yt-metrics-replay",
                            "type": "file",
                            "folder": "yt-metrics-replay",
                            "options": {"path": "/var/lib/grafana/dashboards"},
                            "disableDeletion": True,
                            "updateIntervalSeconds": 10,
                            "allowUiUpdates": False,
                        }
                    ],
                },
                f,
            )
        return self._dump_patched_dashboards()

    def _dump_patched_dashboards(self) -> List[DashboardInfo]:
        dashboards: List[DashboardInfo] = []
        src = os.path.join(self.workdir, "dashboards")
        for name in sorted(os.listdir(src)):
            if not name.endswith(".json"):
                continue
            with open(os.path.join(src, name)) as f:
                text = f.read()
            text = text.replace("${PROMETHEUS_DS_UID}", DATASOURCE_UID)
            with open(os.path.join(self.grafana_dashboards_patched_path, name), "w") as f:
                f.write(text)
            try:
                d = json.loads(text)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid dashboard JSON {name}: {e}")
                continue
            uid = d.get("uid") or os.path.splitext(name)[0]
            title = d.get("title") or os.path.splitext(name)[0]
            dashboards.append(DashboardInfo(file=name, uid=uid, title=title, slug=_slugify(title)))
        return dashboards
