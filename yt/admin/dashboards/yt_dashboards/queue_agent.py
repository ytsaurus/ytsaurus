from yt_dashboard_generator.backends.monitoring import MonitoringCustomDashboardParameter, MonitoringTag
from yt_dashboard_generator.dashboard import Dashboard, Rowset

from yt_dashboard_generator.sensor import MultiSensor
from yt_dashboard_generator.specific_sensors.monitoring import MonitoringExpr, PlainMonitoringExpr
from yt_dashboards.common.sensors import QueueAgentPorto, QueueAgentCpu
from yt_dashboards.common.queue_agent import build_pass_metrisc_rowsets

from dataclasses import dataclass, field
from functools import partial
import inspect
from typing import Optional


@dataclass
class QueueAgentDashboardConfig:
    stages: list[str] = field(default_factory=lambda: ["production"])
    default_stage: str = "production"

    @classmethod
    def from_dict(cls, value: Optional[dict] = None):
        if value is None:
            return cls()

        return cls(
            stages=value.get("stages"),
            default_stage=value.get("default_stage"),
        )


QUEUE_AGENT_STAGE_PARAMETER_NAME = "queue_agent_stage"


def _build_cpu_rowset(has_porto, config: QueueAgentDashboardConfig):
    rowset = Rowset()

    row = rowset.row()
    if has_porto:
        (
            row.cell(
                "CPU utilization %",
                MultiSensor(
                    (
                        100
                        * (
                            MonitoringExpr(QueueAgentPorto("yt.porto.cpu.total"))
                            .all(MonitoringTag("host"))
                            .value("container_category", "pod")
                        )
                        / (
                            MonitoringExpr(QueueAgentPorto("yt.porto.cpu.limit"))
                            .all(MonitoringTag("host"))
                            .value("container_category", "pod")
                        )
                    ).alias("{{container}}"),
                    PlainMonitoringExpr("constant_line(100)").alias("Max CPU utilization"),
                ),
            )
        )
    (
        row.cell(
            "Top 10 threads by CPU total (host=Aggr)",
            MonitoringExpr(QueueAgentCpu("yt.resource_tracker.total_cpu"))
            .aggr(MonitoringTag("host"))
            .aggr(MonitoringTag("bucket"))
            .all(MonitoringTag("container"))
            .all(MonitoringTag("thread"))
            .series_sum("thread")
            .top_max(10),
        ).cell(
            "Top 10 threads by CPU wait (host=Aggr)",
            MonitoringExpr(QueueAgentCpu("yt.resource_tracker.cpu_wait"))
            .aggr(MonitoringTag("host"))
            .aggr(MonitoringTag("bucket"))
            .all(MonitoringTag("container"))
            .all(MonitoringTag("thread"))
            .series_sum("thread")
            .top_max(10),
        )
    )

    return rowset


def _build_ram_rowset(has_porto, config: QueueAgentDashboardConfig):
    rowset = Rowset()

    row = rowset.row()

    if has_porto:
        (
            row.cell(
                "Porto RAM usage",
                (
                    100
                    * (
                        MonitoringExpr(QueueAgentPorto("yt.porto.memory.memory_usage"))
                        .all(MonitoringTag("host"))
                        .value("container_category", "pod")
                    )
                    / (
                        MonitoringExpr(QueueAgentPorto("yt.porto.memory.memory_limit"))
                        .all(MonitoringTag("host"))
                        .value("container_category", "pod")
                    )
                ).alias("{{container}}"),
            )
        )

    return rowset


def _build_pass_metrics_rowset(prefix: str, legend_prefix: str):
    rowsets = build_pass_metrisc_rowsets(prefix, legend_prefix)
    assert len(rowsets) == 1
    return rowsets[0]


def build_dashboard(has_porto, config: Optional[dict] = None):
    if not has_porto:
        raise NotImplementedError("For now queue agent dashboard provides only porto metrics and is not needed without them")

    structured_config = QueueAgentDashboardConfig.from_dict(config)

    d = Dashboard()
    d.set_title("YT Queue Agent")

    rowset_builders = [
        partial(_build_cpu_rowset, has_porto=has_porto),
        partial(_build_ram_rowset, has_porto=has_porto),
        partial(_build_pass_metrics_rowset, prefix="yt.queue_agent", legend_prefix="Queue Agent "),
        partial(_build_pass_metrics_rowset, prefix="yt.queue_agent.cypress_synchronizer", legend_prefix="Cypress Synchronizer "),
        partial(_build_pass_metrics_rowset, prefix="yt.queue_agent.queue_agent_sharding_manager", legend_prefix="Sharding Manager "),
    ]

    def run_builder(builder):
        signature = inspect.signature(builder)

        for name, param in signature.parameters.items():
            if name == "config":
                builder = partial(builder, config=structured_config)
                break

        return builder()

    for rowset_builder in rowset_builders:
        d.add(run_builder(rowset_builder))

    d.add_parameter(
        QUEUE_AGENT_STAGE_PARAMETER_NAME,
        "Queue Agent Stage",
        MonitoringCustomDashboardParameter(
            values=structured_config.stages,
            default_value=structured_config.default_stage,
        ),
    )

    d.value("cluster", f"queue_agent_{{{{{QUEUE_AGENT_STAGE_PARAMETER_NAME}}}}}")

    return d
