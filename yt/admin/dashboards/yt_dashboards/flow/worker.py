# flake8: noqa
# I'd like to disable only E124 and E128 but flake cannot ignore specific
# warnings for the entire file at the moment.
# [E124] closing bracket does not match visual indentation
# [E128] continuation line under-indented for visual indent
# [E131] continuation line unaligned for hanging indent

from .common import build_versions, build_resource_usage, add_common_dashboard_parameters

from yt_dashboard_generator.dashboard import Dashboard
from yt_dashboard_generator.specific_tags.tags import TemplateTag


def build_flow_worker():
    d = Dashboard()
    d.add(build_versions())
    d.add(build_resource_usage("worker", add_component_to_title=False))

    d.set_title("[YT Flow] Pipeline worker")
    add_common_dashboard_parameters(d)

    return (d
        .value("project", TemplateTag("project"))
        .value("cluster", TemplateTag("cluster")))
