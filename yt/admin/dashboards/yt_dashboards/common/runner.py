from yt_dashboard_generator.backends.grafana.cli import GrafanaFacade
from yt_dashboard_generator.backends.monitoring.cli import MonitoringFacade
from yt_dashboard_generator.cli import Cli

from .postprocessors import YtTagPostprocessor

try:
    from .settings import GRAFANA_BASE_URL, MONITORING_ENDPOINT
except ImportError:
    from .opensource_settings import GRAFANA_BASE_URL, MONITORING_ENDPOINT

import argparse


def expand_dashboards(specs):
    backends = {
        "monitoring": MonitoringFacade,
        "grafana": GrafanaFacade,
    }

    result = []

    for key, desc in specs.items():
        title = "YTsaurus-" + " ".join(map(str.capitalize, key.split("-")))
        uid = "ytsaurus-" + key

        for backend_name, backend in backends.items():
            if backend_name in desc:
                if type(desc[backend_name]) is str:
                    result.append((key, backend(desc[backend_name], desc["func"], uid, title)))
                    continue
                id = desc[backend_name].get("id")
                if "args" in desc[backend_name]:
                    func = lambda args=desc[backend_name]["args"], desc=desc: desc["func"](*args)  # noqa: E731
                else:
                    func = desc["func"]
                facade_instance = backend(id, func, uid, title)
                if "postprocessor" in desc[backend_name]:
                    facade_instance.tag_postprocessor = desc[backend_name]["postprocessor"](facade_instance.tag_postprocessor)
                result.append((key, facade_instance))

    return result


def set_facade_settings():
    GrafanaFacade.tag_postprocessor = YtTagPostprocessor(backend="grafana")
    GrafanaFacade.base_url = GRAFANA_BASE_URL

    MonitoringFacade.tag_postprocessor = YtTagPostprocessor(backend="monitoring")
    MonitoringFacade.endpoint = MONITORING_ENDPOINT


def run(dashboards):
    set_facade_settings()

    parser = argparse.ArgumentParser(conflict_handler="resolve")
    cli = Cli(parser)

    for k, v in expand_dashboards(dashboards):
        cli.add_dashboard(k, v)

    args = parser.parse_args()
    cli.run(args)
