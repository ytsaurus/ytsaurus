from ...serializer import SerializerBase
from ...sensor import Sensor, MultiSensor, EmptyCell, Text, Title
from ...specific_tags.tags import BackendTag, SpecificTag, TemplateTag
from ...postprocessors import MustacheTemplateTagPostprocessor
from ...taggable import SystemFields, NotEquals

from .sensors import MonitoringExpr, PlainMonitoringExpr, MonitoringSystemFields

from google.protobuf.json_format import ParseDict, MessageToDict
import grpc

import json
import copy
import sys

##################################################################

MonitoringTag = BackendTag.make_new("MonitoringTag")

##################################################################


class MonitoringLabelDashboardParameter:
    def __init__(self, project_id, label_key, default_value):
        self.dict = {
            "labelValues": {
                "projectId": project_id,
                "labelKey": label_key,
                "defaultValues": [default_value],
            }
        }

##################################################################


class MonitoringProxy():
    def __init__(self, endpoint, token):
        import solomon.protos.api.v3.dashboard_service_pb2 as dashboard_service_pb2
        import solomon.protos.api.v3.dashboard_service_pb2_grpc as dashboard_service_grpc_pb2

        self.endpoint = endpoint
        self.token = token

        self.dashboard_service_pb2 = dashboard_service_pb2
        self.dashboard_service_grpc_pb2 = dashboard_service_grpc_pb2

    def _prepare_channel(self):
        class GrpcAuth(grpc.AuthMetadataPlugin):
            def __init__(self, key):
                self._key = key

            def __call__(self, context, callback):
                callback((('authorization', self._key),), None)
        call_credentials = grpc.metadata_call_credentials(GrpcAuth("OAuth " + self.token))
        credentials = grpc.composite_channel_credentials(
            grpc.ssl_channel_credentials(),
            call_credentials)
        return grpc.secure_channel(self.endpoint, credentials)

    def _do_submit_dashboard(self, dict):
        service = self.dashboard_service_grpc_pb2.DashboardServiceStub(self._prepare_channel())
        message = ParseDict(dict, self.dashboard_service_pb2.UpdateDashboardRequest())
        rsp = service.Update(message)
        if rsp.HasField("error"):
            print(rsp)
            raise Exception("Failed to upload dashboard")
        print("OK!")

    def fetch_dashboard(self, dashboard_id):
        service = self.dashboard_service_grpc_pb2.DashboardServiceStub(self._prepare_channel())
        message = self.dashboard_service_pb2.GetDashboardRequest()
        message.dashboard_id = dashboard_id
        return service.Get(message)

    def submit_dashboard(self, serialized_dashboard, dashboard_id, verbose):
        current = self.fetch_dashboard(dashboard_id)

        print("Current etag:", current.etag)

        request = copy.deepcopy(serialized_dashboard)
        request["dashboard_id"] = dashboard_id
        request["etag"] = current.etag
        request.setdefault("title", current.title)
        request.setdefault("name", current.name)
        request.setdefault("description", current.description)
        request.setdefault("parametrization", MessageToDict(current)["parametrization"])

        if verbose:
            json.dump(request, sys.stderr, indent=4)

        self._do_submit_dashboard(request)


def submit(dict, endpoint, token):
    MonitoringProxy(endpoint, token)._do_submit_dashboard(dict)

##################################################################


class MonitoringSerializerBase(SerializerBase):
    def __init__(self, tag_postprocessor=None):
        super().__init__()
        self.tag_postprocessor = tag_postprocessor

    def _get_sensor_query(self, sensor):
        if issubclass(type(sensor), PlainMonitoringExpr):
            return sensor.serialize()
        elif issubclass(type(sensor), MonitoringExpr):
            def _default_serializer(x):
                if issubclass(type(x), Sensor):
                    return self._get_sensor_query(x)
                return str(x)
            return sensor.serialize(_default_serializer)

        assert issubclass(type(sensor), Sensor)

        tags = list(sensor.get_tags().items())
        if self.tag_postprocessor is not None:
            tags, _ = self.tag_postprocessor.postprocess(tags, None)

        tags, _ = MustacheTemplateTagPostprocessor().postprocess(tags, None)

        string_tags = []
        other_tags = {}
        for k, v in tags:
            if isinstance(k, MonitoringTag):
                k = str(k)
            # TODO: rewrite system tag expansion.
            elif isinstance(k, SpecificTag) and not isinstance(k, TemplateTag):
                continue

            if isinstance(k, str):
                if k.startswith("l."):
                    k = k[2:]
                if v == SystemFields.Aggr:
                    string_tags.append((k, "Aggr" if k == "host" else "-"))
                elif v == SystemFields.All:
                    string_tags.append((k, NotEquals("Aggr*") if k == "host" else "*"))
                else:
                    string_tags.append((k, v))
            else:
                other_tags[k] = v

        def _format_kv_pair(k, v):
            if isinstance(v, NotEquals):
                return f"{k}!='{v.value}'"
            else:
                return f"{k}='{v}'"

        query = "{" + ", ".join(_format_kv_pair(k, v) for k, v in string_tags) + "}"

        if SystemFields.Top in other_tags:
            limit, aggregation = other_tags[SystemFields.Top]
            query = "top({}, '{}', {})".format(limit, aggregation, query)

        if SystemFields.NanAsZero in other_tags:
            query = "replace_nan({}, 0)".format(query)

        if SystemFields.QueryTransformation in other_tags:
            query = other_tags[SystemFields.QueryTransformation].format(query=query)

        if SystemFields.LegendFormat in other_tags:
            query = 'alias({}, "{}")'.format(query, other_tags[SystemFields.LegendFormat])

        return query


class MonitoringDictSerializer(MonitoringSerializerBase):
    BOARD_WIDTH = 36
    DEFAULT_ROW_HEIGHT = 7

    def __init__(self, tag_postprocessor=None):
        super().__init__(tag_postprocessor)
        self.default_row_height = self.DEFAULT_ROW_HEIGHT
        self.next_id = 0
        self.vertical_offset = 0
        self.widgets = []
        self.row_heights = []
        self.halign = "center"

    def on_options(self, options):
        self.__dict__.update(options.get("monitoring", {}))

    def _generate_id(self):
        res = "widget_{:03d}".format(self.next_id)
        self.next_id += 1
        return res

    def on_cell_content(self, content):
        if issubclass(type(content), Text):
            return {"text": {"text": content.text.strip().replace("<EOLN>", "  ")}}

        if issubclass(type(content), Title):
            return {"title": {"text": content.title}}

        sensors = []
        if issubclass(type(content), (Sensor, MonitoringExpr)):
            sensors = [content]
        elif issubclass(type(content), MultiSensor):
            sensors = content.sensors
        else:
            assert False

        stack = None
        axis_to_range = {}
        downsampling_aggregation = None
        targets = []
        for sensor in sensors:
            tags = sensor.get_tags()
            if SystemFields.Stack in tags:
                stack = tags[SystemFields.Stack]
            if SystemFields.Range in tags:
                min, max, axis = tags[SystemFields.Range]
                axis_to_range[axis] = (min, max)
            if MonitoringSystemFields.DownsamplingAggregation in tags:
                downsampling_aggregation = tags[MonitoringSystemFields.DownsamplingAggregation]
            targets.append({"query": self._get_sensor_query(sensor)})

        result = {
            "chart": {
                "queries": {
                    "targets": targets,
                },
                "visualization_settings": {
                },
                "series_overrides": []
            }
        }

        if downsampling_aggregation is not None:
            result["chart"]["queries"]["downsampling"] = {
                "grid_aggregation": downsampling_aggregation,
            }

        settings = result["chart"]["visualization_settings"]
        if stack is not None:
            settings["type"] = "VISUALIZATION_TYPE_STACK" if stack \
                else "VISUALIZATION_TYPE_LINE"

        for axis, range in axis_to_range.items():
            assert axis in (SystemFields.LeftAxis, SystemFields.RightAxis)
            axisKey = "left" if axis == SystemFields.LeftAxis else "right"
            minValue, maxValue = range
            if minValue is not None:
                settings.setdefault("yaxis_settings", {}).setdefault(axisKey, {})["min"] = str(minValue)
            if maxValue is not None:
                settings.setdefault("yaxis_settings", {}).setdefault(axisKey, {})["max"] = str(maxValue)

        if "yaxis_settings" not in settings:
            settings["yaxis_settings"] = {}

        series_overrides = result["chart"]["series_overrides"]
        for index, sensor in enumerate(sensors):
            tags = sensor.get_tags()
            settings = {}

            if SystemFields.SensorStackOverride in tags:
                series_stack = tags[SystemFields.SensorStackOverride]
                settings["type"] = "SERIES_VISUALIZATION_TYPE_STACK" if series_stack else "SERIES_VISUALIZATION_TYPE_LINE"
            if SystemFields.Axis in tags:
                axis = tags[SystemFields.Axis]
                settings["yaxisPosition"] = "YAXIS_POSITION_LEFT" if axis == SystemFields.LeftAxis else "YAXIS_POSITION_RIGHT"

            if not settings:
                continue

            override = {
                "target_index": str(index),
                "settings": settings,
            }
            series_overrides.append(override)

        return result

    def on_cell(self, cell, content):
        if "text" in content or "title" in content:
            return content

        assert "chart" in content
        chart = content["chart"]
        chart["id"] = self._generate_id()
        chart["title"] = cell.title

        if cell.display_legend is not None:
            chart["displayLegend"] = cell.display_legend

        for axis, label in cell.yaxis_to_label.items():
            assert axis in (SystemFields.LeftAxis, SystemFields.RightAxis)
            axisKey = "left" if axis == SystemFields.LeftAxis else "right"

            settings = chart["visualization_settings"]
            settings["yaxis_settings"].setdefault(axisKey, {})["title"] = label

        return content

    def on_row(self, row, cells):
        self.row_heights.append(row.height)

        return cells

    def on_rowset(self, rowset, rows):
        assert len(rows) == len(self.row_heights)

        max_len = max(len(row) for row in rows)
        width = self.BOARD_WIDTH // max_len

        for row, specified_height in zip(rows, self.row_heights):
            row_height = specified_height if specified_height is not None else self.default_row_height

            if self.halign == "center":
                x_shift = (self.BOARD_WIDTH - len(row) * width) // 2
            elif self.halign == "left":
                x_shift = 0
            else:
                raise RuntimeError("Invalid value of halign: {self.halign}")

            for i, cell in enumerate(row):
                cell["position"] = {
                    "x": x_shift + i * width,
                    "y": self.vertical_offset,
                    "w": width,
                    "h": row_height,
                }
                self.widgets.append(cell)

            if self.halign == "left":
                for i in range(len(row), max_len):
                    cell = {
                        "text": {"text": ""},
                        "position": {
                            "x": x_shift + i * width,
                            "y": self.vertical_offset,
                            "w": width,
                            "h": row_height,
                        }
                    }
                    self.widgets.append(cell)

            self.vertical_offset += row_height

        self.row_heights = []

        return rows

    def dashboard_parameters_to_dict(self, parameters):
        result = []
        for parameter in parameters:
            if parameter["backends"] and "monitoring" not in parameter["backends"]:
                continue
            dct = {
                "name": parameter["name"],
                "title": parameter["title"],
            }
            for arg in parameter.get("args", []):
                if type(arg) is MonitoringLabelDashboardParameter:
                    dct.update(arg.dict)
            result.append(dct)
        return result

    def on_dashboard(self, dashboard, rowsets):
        result = {
            "widgets": self.widgets,
        }

        if dashboard.title is not None:
            result["title"] = dashboard.title
        if dashboard.description is not None:
            result["description"] = dashboard.description
        if dashboard.parameters is not None:
            result["parametrization"] = {
                "parameters": self.dashboard_parameters_to_dict(dashboard.parameters)
            }

        return result


class MonitoringDebugSerializer(MonitoringSerializerBase):
    def __init__(self, tag_postprocessor=None):
        super().__init__(tag_postprocessor)

    def on_cell_content(self, content):
        if issubclass(type(content), EmptyCell):
            return "<EMPTY>"
        if issubclass(type(content), Text):
            return content.text
        if issubclass(type(content), Title):
            return content.title

        sensors = []
        if issubclass(type(content), (Sensor, MonitoringExpr)):
            sensors = [content]
        elif issubclass(type(content), MultiSensor):
            sensors = content.sensors
        else:
            raise Exception(f"Cannot serialize cell content of type {type(content)}")

        result = {"queries": []}
        for sensor in sensors:
            tags = sensor.get_tags()
            if SystemFields.Stack in tags:
                result["Stack"] = tags[SystemFields.Stack]
            if SystemFields.Range in tags:
                result["Range"] = tags[SystemFields.Range]
            if MonitoringSystemFields.DownsamplingAggregation in tags:
                result["DownsamplingAggregation"] = tags[MonitoringSystemFields.DownsamplingAggregation]
            result["queries"].append(self._get_sensor_query(sensor))

        return result

    def on_cell(self, cell, content):
        return content

    def on_row(self, row, cells):
        return copy.copy(cells)

    def on_sensor(self, sensor):
        query = self._get_sensor_query(sensor)
        parts = query.split()
        lines = [""]
        for part in parts:
            if lines[-1] and len(lines[-1] + part) > 35:
                lines.append("")
            lines[-1] += (" " if lines[-1] else "") + part

        res = " ".join(lines)
        return res
        if len(res) > 40:
            return res[:20] + "..." + res[-20:]
        else:
            return res
