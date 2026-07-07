from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_STEP = "15s"
DEFAULT_MAX_SERIES = 100000

REPLAY_PREFIX = "yt-metrics-replay"
DATASOURCE_UID = "yt-metrics-replay"
DATASOURCE_NAME = "YtMetricsReplay"
GRAFANA_PROVISIONING_FILE = "yt-metrics-replay.yaml"

REPLAY_LABEL_KEY = "yt-metrics-replay"
REPLAY_LABEL_VALUE = "1"
REPLAY_LABELS = {REPLAY_LABEL_KEY: REPLAY_LABEL_VALUE}
REPLAY_LABEL_FILTER = {"label": f"{REPLAY_LABEL_KEY}={REPLAY_LABEL_VALUE}"}


@dataclass
class MetricsDumpConfig:
    spec_path: str
    from_ts: datetime
    to_ts: datetime
    step: Optional[str]
    extra_targets: Optional[List[str]]
    output: str
    max_series: int
    force: bool


@dataclass
class MetricsReplayConfig:
    archive: str
    prometheus_port: Optional[int]
    grafana_port: Optional[int]


@dataclass
class DashboardInfo:
    file: str
    uid: str
    title: str
    slug: str


@dataclass
class MetricsSpec:
    selectors: List[str]
    dashboards: List[Tuple[str, Dict[str, Any]]]
    step: str
    raw: Dict[str, Any]
