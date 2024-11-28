from yt_dashboard_generator.taggable import SystemFields, NotEquals
from yt_dashboard_generator.specific_tags.tags import TemplateTag
from yt_dashboard_generator.postprocessors import TagPostprocessorBase, SimpleTagPostprocessor
from yt_dashboard_generator.backends.grafana import GrafanaSystemTags
from yt_dashboard_generator.specific_tags.tags import SpecificTag, Regex

from . import sensors

from functools import cmp_to_key

from typing import Iterable

##################################################################


class HostAggrTagPostprocessor(TagPostprocessorBase):
    def postprocess(self, tags, sensor_name=None):
        res = []
        for k, v in tags.items():
            if str(k) in ("host", "l.host"):
                if v == SystemFields.Aggr:
                    v = "Aggr"
                elif v == SystemFields.All:
                    v = "!Aggr*"
            res.append((k, v))
        return dict(res), sensor_name


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
        if type(x) is type(y):
            return 1 if x > y else -1
        if type(x) is int:
            return -1
        return 1

    def postprocess(self, tags, sensor_name=None):
        tags = dict(sorted(tags.items(), key=cmp_to_key(lambda *args: self._cmp(*args))))
        return tags, sensor_name


class GrafanaTagPostprocessor(TagPostprocessorBase):
    def __init__(self, backend):
        self.active = backend == "grafana"

    def postprocess(self, tags, sensor_name=None):
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

        tags, sensor_name = HostAggrTagPostprocessor().postprocess(tags, sensor_name)

        tags, sensor_name = GrafanaTagPostprocessor(self.backend).postprocess(tags, sensor_name)

        key_replacements = []
        for k, v in tags.items():
            if isinstance(v, str) and v.startswith("!"):
                if self.backend == "monitoring":
                    tags[k] = NotEquals(v[1:])
                elif self.backend == "grafana":
                    tags[k] = NotEquals(Regex(v[1:] + "|^$"))

        for before, after in key_replacements:
            tags[after] = tags.pop(before)

        tags = self.sort(tags)

        return tags, sensor_name

    def sort(self, tags):
        return SortingTagPostprocessor().postprocess(tags)[0]


##################################################################


# The stubs below should not be used in dashboard configuration.
class AddTagPostprocessorStub(TagPostprocessorBase):
    def __init__(self, underlying, extra_tags, mode="append"):
        super().__init__()

        self.underlying = underlying
        # This preserves order since version 3.7.
        self.extra_tags = dict(extra_tags)
        self.mode = mode

    def postprocess(self, tags, sensor_name=None):
        tags, sensor_name = self.underlying.postprocess(tags, sensor_name)

        assert not (set(self.extra_tags.keys()) & set(tags.keys())), "Additional tags should not intersect with any existing tags"

        if self.mode == "append":
            return (tags | self.extra_tags), sensor_name
        elif self.mode == "prepend":
            return (self.extra_tags | tags), sensor_name
        else:
            assert False, f'Unknown mode {self.mode}, use either "append" or "prepend"'


class MapTagPostprocessorStub(SimpleTagPostprocessor):
    def __init__(self, underlying, old_key, new_key=None, value_mapping=None):
        super().__init__()

        self.underlying = underlying
        self.old_key = old_key
        self.new_key = new_key or old_key
        self.value_mapping = value_mapping or {}

    def process_tag(self, k, v):
        if k == self.old_key:
            return self.new_key, self.value_mapping.get(v, v)
        return k, v

    def postprocess(self, tags, sensor_name):
        tags, sensor_name = self.underlying.postprocess(tags, sensor_name)
        return super().postprocess(tags, sensor_name)


def define_postprocessor(stub):
    return lambda *args, **kwargs: lambda underlying: stub(underlying, *args, **kwargs)


##################################################################


# These postprocessors can be used in the `postprocessor` parameter in dashboard configuration.
AddTagPostprocessor = define_postprocessor(AddTagPostprocessorStub)
MapTagPostprocessor = define_postprocessor(MapTagPostprocessorStub)


# Combines postprocessors like the ones defined above in the following fashion: P1(P2(...P_k(underlying))).
def combine_postprocessors(*args):
    def combined_postprocessor(underlying):
        current_underlying = underlying
        for postprocessor in args[::-1]:
            current_underlying = postprocessor(current_underlying)
        return current_underlying

    return combined_postprocessor
