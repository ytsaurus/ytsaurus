from yt_dashboard_generator.taggable import SystemFields, NotEquals
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.postprocessors import TagPostprocessorBase
from yt_dashboard_generator.backends.grafana import GrafanaSystemTags
from yt_dashboard_generator.specific_tags.tags import SpecificTag, Regex

from . import sensors

from functools import cmp_to_key


class HostAggrTagPostprocessor(TagPostprocessorBase):
    def postprocess(self, tags, sensor_name=None):
        res = []
        for (k, v) in tags:
            if str(k) in ("host", "l.host"):
                if v == SystemFields.Aggr:
                    v = "Aggr"
                elif v == SystemFields.All:
                    v = "!Aggr*"
            res.append((k, v))
        return res, sensor_name


class SortingTagPostprocessor(TagPostprocessorBase):
    """The order in which tags appear in a Monitoring url or query."""
    PRIORITIES = [
        "project",
        "cluster",
        "service",
        "sensor",
        "job",
        "__name__",
        "path",
        "host",
        "tablet_cell_bundle",
        "table_path",
        "table_tag",
        "user",
        "account",
        "location_type",
        "location_id",
        "medium",
        "yt_service",
        "method",
        "proxy_role",
        "category",
        "replica_cluster",
        "graph",
    ]

    def _key(self, x):
        if isinstance(x, SpecificTag):
            x = x.value
        if type(x) is not str:
            return 0
        if x.startswith("l."):
            x = x[2:]
        if x in ("hideNoData", "graph"):
            return (1, x)
        if x == "stack":
            return (2, x)
        if x in self.PRIORITIES:
            return self.PRIORITIES.index(x)
        return (0, x)

    def _cmp(self, x, y):
        x = x[0]
        y = y[0]
        if x == y:
            return 0
        x = self._key(x)
        y = self._key(y)
        if type(x) == type(y):
            return 1 if x > y else -1
        if type(x) is int:
            return -1
        return 1

    def postprocess(self, tags, sensor_name=None):
        tags.sort(key=cmp_to_key(lambda *args: self._cmp(*args)))
        return tags, sensor_name


class GrafanaTagPostprocessor(TagPostprocessorBase):
    def __init__(self, backend):
        self.active = backend == "grafana"

    def postprocess(self, tags, sensor_name=None):
        tags = dict(tags)
        if not self.active:
            return tags, sensor_name
        if sensor_name.endswith(".rate"):
            tags[GrafanaSystemTags.Rate] = True
            sensor_name = sensor_name[:-5]
        sensor_name = sensor_name.replace(".", "_")
        tags["__name__"] = sensor_name
        return tags, sensor_name


class YtTagPostprocessor(TagPostprocessorBase):
    def __init__(self, backend):
        self.backend = backend

    def postprocess(self, tags, sensor_name=None):
        tags = dict(tags)

        if self.backend in ("monitoring",):
            tags.setdefault("project", "yt")
            tags.setdefault("cluster", TemplateTag("cluster"))

            if sensors.YtSystemTags.HostContainer in tags:
                value = tags.pop(sensors.YtSystemTags.HostContainer)
                if value in (SystemFields.All, SystemFields.Aggr):
                    tags["host"] = value
                    tags["container"] = value
                else:
                    raise Exception(
                        '"yt_host" label should be used either with .all() or with .aggr(), '
                        f'got "{value}" of type {type(value)}')

        tags, sensor_name = HostAggrTagPostprocessor().postprocess(
            tags.items(), sensor_name)
        tags = dict(tags)

        tags, sensor_name = GrafanaTagPostprocessor(self.backend).postprocess(
            tags.items(), sensor_name
        )
        tags = dict(tags)

        key_replacements = []
        for k, v in tags.items():
            if isinstance(v, str) and v.startswith("!"):
                if self.backend == "monitoring":
                    tags[k] = NotEquals(v[1:])
                elif self.backend == "grafana":
                    tags[k] = NotEquals(Regex(v[1:] + "|^$"))

        for before, after in key_replacements:
            tags[after] = tags.pop(before)

        tags = list(tags.items())

        tags = self.sort(tags)

        return tags, sensor_name

    def sort(self, tags):
        return SortingTagPostprocessor().postprocess(tags)[0]
