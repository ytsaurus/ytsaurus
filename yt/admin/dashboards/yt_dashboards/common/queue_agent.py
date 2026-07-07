# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
from yt_dashboard_generator.specific_sensors.monitoring import MonitoringExpr
from yt_dashboard_generator.dashboard import Rowset

from .sensors import *


def build_pass_metrisc_rowsets(prefix: str, legend_prefix: str = ""):
    pass_index = (QueueAgent(f"{prefix}.pass.index")
        .all(MonitoringTag("host"))
        .legend_format(f"{legend_prefix}Pass Index")
        .unit("UNIT_COUNT")
    )
    pass_errors = (QueueAgent(f"{prefix}.pass.errors.rate")
        .all(MonitoringTag("host"))
        .legend_format(f"{legend_prefix}Pass Errors")
        .unit("UNIT_COUNT")
    )
    pass_start_time = ((MonitoringExpr(QueueAgent(f"{prefix}.pass.start_time")) * 1000)
        .all(MonitoringTag("host"))
        .legend_format(f"{legend_prefix}Pass Start Time")
        .unit("UNIT_DATETIME_LOCAL")
    )
    pass_duration = (QueueAgent(f"{prefix}.pass.duration.max")
        .all(MonitoringTag("host"))
        .legend_format(f"{legend_prefix}Pass Duration")
        .unit("UNIT_SECONDS")
    )
    return [
        Rowset()
            .row()
                .cell(f"{legend_prefix}Pass Index", pass_index)
                .cell(f"{legend_prefix}Pass Errors", pass_errors)
            .row()
                .cell(f"{legend_prefix}Pass Start Time", pass_start_time)
                .cell(f"{legend_prefix}Pass Duration", pass_duration),
    ]
