from .common import cli, nested_get

import click
import httpx
import json
import sys

from datetime import datetime
from dataclasses import asdict, dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class MetricType(StrEnum):
    DGAUGE = "DGAUGE"
    IGAUGE = "IGAUGE"


@dataclass
class Metric:
    name: str
    labels: dict[str, str]
    type: MetricType
    value: int | float


class MetricUploader:
    api_client: httpx.Client | None
    monitoring_endpoint: str | None
    iam_token: str

    def __init__(self, monitoring_endpoint: str | None, iam_token: str | None):
        self.monitoring_endpoint = monitoring_endpoint
        if monitoring_endpoint is None:
            return
        if iam_token is None:
            raise ValueError("IAM token is required for uploading metrics")
        self.iam_token = iam_token

    def __enter__(self):
        if self.monitoring_endpoint:
            self.api_client = httpx.Client(
                headers={"Authorization": f"Bearer {self.iam_token}"}, params={"service": "custom"}
            )
        return self

    def __exit__(self, *args):
        if self.api_client:
            self.api_client.close()
            self.api_client = None

    def collect_labels(self, query_index: int, query_info: dict[str, Any], query_type: str | None) -> dict[str, Any]:
        labels = {
            "query_index": query_index,
            "query_title": query_info["annotations"]["title"],
            "query_engine": query_info["engine"],
        }
        if query_type is not None:
            labels["query_type"] = query_type
        return labels

    def collect_metrics(self, query_info: dict[str, Any]) -> list[Metric]:
        metrics: list[Metric] = []

        def add_metric(name: str, value: int | float | None, metric_type: MetricType = MetricType.DGAUGE):
            if value is not None:
                metrics.append(Metric(name=f"query.{name}", labels={}, type=metric_type, value=value))

        def add_metric_from_path(name: str, path: str, metric_type: MetricType, proj=lambda x: x):
            metric_value = nested_get(query_info, path.split("."))
            if metric_value is not None:
                add_metric(name, proj(metric_value), metric_type)

        add_metric_from_path(
            "row_count",
            "progress.yql_statistics.ExecutionStatistics.yt.total.data/input/row_count.sum",
            MetricType.IGAUGE,
        )
        add_metric_from_path("node_count", "progress.yql_plan.Basic.nodes", MetricType.IGAUGE, len)

        query_start = datetime.fromisoformat(query_info["start_time"]) if "start_time" in query_info else None
        query_finish = datetime.fromisoformat(query_info["finish_time"]) if "finish_time" in query_info else None
        query_state = query_info["state"]
        if query_start is not None and query_finish is not None:
            add_metric("duration", (query_finish - query_start).total_seconds(), MetricType.DGAUGE)
        add_metric("error", int(query_state not in ["completing", "completed"]), MetricType.IGAUGE)

        return metrics

    def process_query(self, query_index: int, query_info: dict[str, Any], query_type: str | None):
        if not self.api_client:
            return
        labels = self.collect_labels(query_index, query_info, query_type)
        metrics = self.collect_metrics(query_info)
        dict_metrics = [asdict(metric) for metric in metrics]
        json_data = {"labels": labels, "metrics": dict_metrics, "ts": query_info["start_time"]}
        response = self.api_client.post(self.monitoring_endpoint, json=json_data)
        response.raise_for_status()


@cli.command()
@click.option(
    "--artifact-path",
    type=click.Path(file_okay=False, writable=True),
    help="Path to look for artifacts in.",
)
@click.option(
    "--monitoring-token",
    envvar="MONITORING_TOKEN",
    help="Monitoring IAM token for uploading launch metrics. Fetched from env var MONITORING_TOKEN by default.",
)
@click.option("--monitoring-endpoint", help="Monitoring endpoint for writing metrics. Should contain the namespace.")
def upload_metrics(artifact_path: str, monitoring_endpoint: str, monitoring_token: str):
    """Extract metrics about the launch (or launches) from the artifacts and upload them to Monitoring."""

    with MetricUploader(monitoring_endpoint, monitoring_token) as uploader:
        for query_file in Path(artifact_path).glob("**/queries/**/info.json"):
            print(f"Uploading metrics from {query_file}", file=sys.stderr)
            with query_file.open() as f:
                query_index = int(query_file.parent.name)
                query_info = json.load(f)
                query_type = None
                if query_file.parent.parent != "queries":
                    query_type = query_file.parent.parent.name
                uploader.process_query(query_index, query_info, query_type)
