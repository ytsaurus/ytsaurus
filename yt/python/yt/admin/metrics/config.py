from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_STEP_MS = 15000
DEFAULT_MAX_SERIES = 100000
DEFAULT_MAX_POINTS_PER_SERIES = 11000

_DURATION_UNITS = {
    "y": 31536000000,
    "w": 604800000,
    "d": 86400000,
    "h": 3600000,
    "m": 60000,
    "s": 1000,
    "ms": 1,
}


def parse_duration_ms(text: str) -> int:
    stripped = text.strip()
    digits = len(stripped) - len(stripped.lstrip("0123456789"))
    value, unit = stripped[:digits], stripped[digits:]
    if not value or unit not in _DURATION_UNITS:
        raise ValueError(f"Invalid duration: {text!r}")
    return int(value) * _DURATION_UNITS[unit]


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
    step_ms: Optional[int]
    extra_targets: Optional[List[str]]
    output: str
    max_series: int
    force: bool
    max_points_per_series: int


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
    step_ms: int
    raw: Dict[str, Any]
