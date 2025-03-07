# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent

from .common.sensors import (
    Master, HttpProxy,
    # Scheduler, SchedulerMemory, SchedulerCpu, SchedulerPools, SchedulerInternal, SchedulerRpc,
    # CA, CAMemory, CACpu, CAInternal, CARpc,
    yt_host,
)

from yt_dashboard_generator.dashboard import Dashboard, Rowset
from yt_dashboard_generator.sensor import Sensor, MultiSensor, Text, Title
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter, MonitoringExpr
from yt_dashboard_generator.taggable import SystemFields, NotEquals

##################################################################


def _build_master_metrics(d):
    def master_rps_sensor(type):
        return Master(f"yt.security.user_{type}_request_count.rate")

    def master_time_sensor(type):
        return Master(f"yt.security.user_{type}_time.rate")

    d.add(Rowset()
        .row(height=2).cell("", Title("Master metrics", size="TITLE_SIZE_L"))
    )
    d.add(Rowset()
        .all("host")
        .row()
            .stack(True)
            .cell("Read RPS", master_rps_sensor("read"))
            .cell("Write RPS", master_rps_sensor("write"))
        .row()
            .stack(True)
            .cell("Read time", master_time_sensor("read"))
            .cell("Write time", master_time_sensor("write"))
        .row()
            .cell(
                "Request queue size",
                Master(f"yt.security.user_request_queue_size.max")
            )
    )


def _build_http_proxy_metrics(d):
    def api_error_count():
        return HttpProxy("yt.http_proxy.api_error_count.rate")

    def bytes_rate(type):
        return HttpProxy(f"yt.http_proxy.bytes_{type}.rate")

    d.add(Rowset()
        .row(height=2).cell("", Title("HTTP proxy metrics", size="TITLE_SIZE_L"))
    )
    d.add(Rowset()
        .aggr("host")
        .row()
            .all("error_code")
            .cell(
                "Failed requests by command and error code",
                api_error_count()
                    .all("command")
                    .aggr("proxy_role")
            )
            .cell(
                "Failed requests by proxy role and error code",
                api_error_count()
                    .aggr("command")
                    .all("proxy_role")
            )
        .row()
            .cell("Request rate",
                HttpProxy("yt.http_proxy.request_count.rate")
                    .all("proxy_role", "command")
            )
        .row()
            .all("proxy_role")
            .aggr("command", "network")
            .cell("Out bytes", bytes_rate("out"))
            .cell("In bytes", bytes_rate("in"))
    )


def build_user_load():
    d = Dashboard()
    _build_master_metrics(d)
    _build_http_proxy_metrics(d)

    d.value("user", TemplateTag("user"))
    d.set_description("User load")

    d.set_monitoring_serializer_options(dict(default_row_height=9))

    d.add_parameter(
        "cluster",
        "YT cluster",
        MonitoringLabelDashboardParameter(
            "yt",
            "cluster",
            "-"))
    d.add_parameter(
        "user",
        "User",
        MonitoringLabelDashboardParameter(
            "yt",
            "user",
            "-"))

    return d
