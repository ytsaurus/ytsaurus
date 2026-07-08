from yt_dashboard_generator.backends.monitoring import MonitoringDictSerializer
from yt_dashboard_generator.backends.grafana import GrafanaDictSerializer

from yt_dashboards.common.postprocessors import YtTagPostprocessor

import yatest

import os
import json


def canonize_dashboard(dashboard, serializer, filename):
    filepath = os.path.join(yatest.common.work_path(), filename)
    with open(filepath, "w") as fout:
        json.dump(dashboard.serialize(serializer), fout, sort_keys=True, indent=4)
    return yatest.common.canonical_file(filepath, local=True)


def monitoring_serializer():
    return MonitoringDictSerializer(tag_postprocessor=YtTagPostprocessor(backend="monitoring"))


def grafana_serializer():
    return GrafanaDictSerializer(tag_postprocessor=YtTagPostprocessor(backend="grafana"), datasource=None)
