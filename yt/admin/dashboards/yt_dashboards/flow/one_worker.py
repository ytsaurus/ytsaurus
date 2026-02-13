# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from .common import create_dashboard

from yt_dashboard_generator.backends.monitoring import MonitoringLabelDashboardParameter


def build_flow_one_worker():
    def fill(d):
        d.add_parameter("host", "Worker host", MonitoringLabelDashboardParameter("", "host", "-"))

    return create_dashboard("one-worker", fill, worker_host_aggr=False)
