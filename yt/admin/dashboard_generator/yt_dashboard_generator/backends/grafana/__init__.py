from ...serializer import SerializerBase
from ...sensor import Sensor, MultiSensor, Text, Title
from ...specific_tags.tags import BackendTag, Regex
from ...taggable import SystemFields, NotEquals, ContainerTemplate, SensorTemplate
from ...helpers import break_long_lines_in_multiline_cell, pretty_print_fixed_indent
from ...specific_sensors.monitoring import MonitoringExpr
from ...postprocessors import DollarTemplateTagPostprocessor

import requests
import enum
import lark

##################################################################

GrafanaTag = BackendTag.make_new("GrafanaTag")


class GrafanaSystemTags(enum.Enum):
    Rate = enum.auto()


class GrafanaTextboxDashboardParameter:
    def __init__(self, default_value=None):
        self.dict = {
            "type": "textbox",
        }
        if default_value is not None:
            option = {
                "selected": True,
                "text": default_value,
                "value": default_value,
            }
            self.dict.update({
                "options": [option],
                "current": option,
                "query": default_value,
                "hide": 0,
                "skipUrlSync": False,
                "allFormat": "glob",
            })


##################################################################


class GrafanaProxy():
    def __init__(self, base_url, api_key):
        self.base_url = base_url
        self.api_key = api_key

    def _prepare_headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def fetch_dashboard(self, dashboard_id):
        rsp = requests.get(
            f"{self.base_url}/api/dashboards/uid/{dashboard_id}",
            headers=self._prepare_headers())
        res = rsp.json()
        if "dashboard" not in res:
            pretty_print_fixed_indent(rsp.json())
            exit(1)
        return res

    def submit_dashboard(self, serialized_dashboard, dashboard_id):
        try:
            current = self.fetch_dashboard(dashboard_id)["dashboard"]
            print("Current version:", current["version"])
        except BaseException:
            current = {}

        request = serialized_dashboard
        if dashboard_id is not None:
            request["uid"] = dashboard_id
        request.setdefault("title", current.get("title", "some default title"))
        request.setdefault("title", "arusntarstrst")
        request.setdefault("templating", current.get("templating", {}))
        request["version"] = current.get("version", 1)

        request = {"dashboard": request}

        rsp = requests.post(
            f"{self.base_url}/api/dashboards/db",
            json=request,
            headers=self._prepare_headers())
        pretty_print_fixed_indent(rsp.json())


##################################################################

class ExprFuncSerializer:
    @staticmethod
    def alias_expr_builder(serializer, expression):
        query_parts, other_tags = serializer._prepare_expr_query(expression.args[1])
        other_tags[SystemFields.LegendFormat] = expression.args[2]
        return query_parts, other_tags

    @staticmethod
    def topk_expr_builder(serializer, expression):
        """
        There is this https://www.robustperception.io/graph-top-n-time-series-in-grafana/ trick, but it is a bit complicated for now.
        """
        query_parts, other_tags = serializer._prepare_expr_query(expression.args[2])
        return f"(topk({expression.args[1]}, {query_parts}))", other_tags

    @staticmethod
    def bottomk_expr_builder(serializer, expression):
        """
        There is this https://www.robustperception.io/graph-top-n-time-series-in-grafana/ trick, but it is a bit complicated for now.
        """
        query_parts, other_tags = serializer._prepare_expr_query(expression.args[2])
        return f"(bottomk({expression.args[1]}, {query_parts}))", other_tags

    @staticmethod
    def moving_avg(serializer, expression):
        query_parts, other_tags = serializer._prepare_expr_query(expression.args[1])
        return f"(avg_over_time({query_parts}[{expression.args[1]}]))", other_tags

    @staticmethod
    def series_avg(serializer, expression):
        query_parts, other_tags = serializer._prepare_expr_query(expression.args[1])
        return f"(avg by({expression.args[1]}) {query_parts})", other_tags

    @staticmethod
    def series_max(serializer, expression):
        query_parts, other_tags = serializer._prepare_expr_query(expression.args[1])
        return f"(max by({expression.args[1]}) {query_parts})", other_tags


##################################################################

class GrafanaSerializerBase(SerializerBase):
    def __init__(self, tag_postprocessor=None):
        super().__init__()
        self.tag_postprocessor = tag_postprocessor
        self.template_tag_postprocessor = DollarTemplateTagPostprocessor()

    def _glob_to_regex(self, glob):
        regex = []
        for i, c in enumerate(glob):
            if c == "*" and (i == 0 or glob[i - 1] != "."):
                regex.append(".")
            regex.append(c)
        return "".join(regex)

    def _prepare_bin_op_expr_query(self, expression):
        assert expression.args[0] in "+-*/", f"Binary operation {expression.args[0]} is not supported. Expression: {expression}, expression type: {type(expression)}"
        left_query, left_tags = self._prepare_expr_query(expression.args[1])
        right_query, right_tags = self._prepare_expr_query(expression.args[2])
        left_tags.update(right_tags)
        return f"({left_query}) {expression.args[0]} ({right_query})", left_tags

    def _prepare_func_expr_query(self, expression):
        builders = {
            "alias": lambda expression: ExprFuncSerializer.alias_expr_builder(self, expression),
            "top_max": lambda expression: ExprFuncSerializer.topk_expr_builder(self, expression),
            "top_min": lambda expression: ExprFuncSerializer.topk_expr_builder(self, expression),
            "bottom_min": lambda expression: ExprFuncSerializer.bottomk_expr_builder(self, expression),
            "moving_avg": lambda expression: ExprFuncSerializer.moving_avg(self, expression),
            "series_avg": lambda expression: ExprFuncSerializer.moving_avg(self, expression),
            "series_max": lambda expression: ExprFuncSerializer.moving_avg(self, expression),
        }
        builder = builders.get(expression.args[0])
        if builder is None:
            raise NotImplementedError(f"Function {expression.args[0]} is not supported. Expression: {expression}, expression type: {type(expression)}")
        return builder(expression)

    def _prepare_expr_query(self, expression):
        if issubclass(type(expression), int):
            return expression, {}
        if issubclass(type(expression), str):
            return expression, {}
        if issubclass(type(expression), float):
            return expression, {}
        if issubclass(type(expression), Sensor):
            query_parts, other_tags = self._prepare_sensor_query(expression)
            query = f"{{{', '.join(query_parts)}}}"
            if GrafanaSystemTags.Rate in other_tags:  # cthulhu fhtagn
                query = f'rate({query}[$__rate_interval])'
            return query, other_tags
        assert issubclass(type(expression), MonitoringExpr), f"Expression can be either Sensor or MonitoringExpression. Expression: {expression}, expression type: {type(expression)}"
        if expression.node_type == MonitoringExpr.NodeType.BinaryOp:
            return self._prepare_bin_op_expr_query(expression)
        if expression.node_type == MonitoringExpr.NodeType.Terminal:
            return self._prepare_expr_query(expression.args[0])
        if expression.node_type == MonitoringExpr.NodeType.Func:
            return self._prepare_func_expr_query(expression)
        assert False, f"This should be unreachable. Expression: {expression}, expression type: {type(expression)}"

    def _prepare_sensor_query(self, sensor):
        if not issubclass(type(sensor), Sensor):
            raise Exception(f"Cannot serialize cell content of type {type(sensor)}")

        tags = list(sensor.get_tags().items())
        sensor_name = sensor.sensor

        if self.tag_postprocessor is not None:
            tags, _ = self.tag_postprocessor.postprocess(tags, sensor_name)

        tags, _ = self.template_tag_postprocessor.postprocess(tags, sensor_name)

        string_tags = []
        other_tags = {}
        for k, v in tags:
            if k == sensor.sensor_tag_name:
                continue

            if isinstance(k, str):
                string_tags.append((k, v))
            elif isinstance(k, GrafanaTag):
                string_tags.append((str(k), v))
            else:
                other_tags[k] = v

        query_parts = []
        for k, v in string_tags:
            negate = False
            regex = False
            while not isinstance(v, str):
                if v == SystemFields.All:
                    v = ".+"
                    regex = True
                elif v == SystemFields.Aggr:
                    v = ""
                elif isinstance(v, NotEquals):
                    v = v.value
                    negate = True
                elif isinstance(v, Regex):
                    v = v.value
                    regex = True
                else:
                    assert False, f"Could not dispatch tag `{k}` with value `{v}` of type `{type(v)}`"

            if not regex:
                v = self._glob_to_regex(v)

            if negate:
                query_parts.append(f'{k}!~"{v}"')
            else:
                query_parts.append(f'{k}=~"{v}"')

        return (query_parts, other_tags)


class GrafanaDictSerializer(GrafanaSerializerBase):
    BOARD_WIDTH = 24
    DEFAULT_ROW_HEIGHT = 6

    def __init__(self, datasource, tag_postprocessor=None):
        self.datasource = datasource
        super().__init__(tag_postprocessor)
        self.vertical_offset = 0
        self.panels = []
        self.default_row_height = self.DEFAULT_ROW_HEIGHT
        self.row_heights = []

    def on_options(self, options):
        self.__dict__.update(options.get("grafana", {}))

    def on_cell_content(self, content):
        if issubclass(type(content), Text):
            return {
                "type": "text",
                "options": {
                    "content": content.text.strip().replace("<EOLN>", "<br>"),
                    "mode": "markdown",
                },
            }

        if issubclass(type(content), Title):
            return {
                "type": "text",
                "options": {
                    "content": "<h2>{}</h2>".format(content.title),
                    "mode": "markdown",
                },
            }

        sensors = []
        if issubclass(type(content), Sensor) or issubclass(type(content), MonitoringExpr):
            sensors = [content]
        elif issubclass(type(content), MultiSensor):
            sensors = content.sensors
        else:
            raise Exception(f"Cannot serialize cell content of type {type(content)}")

        try:
            stack = False
            range = (None, None)
            targets = []
            for sensor in sensors:
                query, other_tags = self._prepare_expr_query(sensor)
                if SystemFields.QueryTransformation in other_tags:
                    query = other_tags[SystemFields.QueryTransformation].format(query=query)
                # TODO: Support different stacking options for different sensors.
                if SystemFields.Stack in other_tags:
                    stack = other_tags[SystemFields.Stack]
                if SystemFields.Range in other_tags:
                    minValue, maxValue, axis = other_tags[SystemFields.Range]
                    if axis != SystemFields.LeftAxis:
                        raise Exception("Grafana dashboard generator supports only left axis")
                    range = (minValue, maxValue)

                target = {
                    "datasource": self.datasource,
                    "expr": query,
                }
                if SystemFields.LegendFormat in other_tags:
                    value = other_tags[SystemFields.LegendFormat]
                    value = value.replace(ContainerTemplate, "{{pod}}").replace(SensorTemplate, "{{__name__}}")
                    target["legendFormat"] = value

                targets.append(target)
        except NotImplementedError as e:
            print(e)
            return {
                "type": "text",
                "options": {
                    "content": "<h2>Not Supported</h2>",
                    "mode": "markdown",
                },
            }

        result = {
            "datasource": self.datasource,
            "type": "timeseries",
            "targets": targets,
            "fieldConfig": {"defaults": {"unit": "short", "custom": {}}},
        }

        minValue, maxValue = range

        if minValue is not None:
            result["fieldConfig"]["defaults"]["min"] = minValue
        if maxValue is not None:
            result["fieldConfig"]["defaults"]["max"] = maxValue

        if stack:
            result["fieldConfig"]["defaults"]["custom"] = {
                "stacking": {
                    "group": "A",
                    "mode": "normal",
                },
                "fillOpacity": 100,
            }

        return result

    def on_cell(self, cell, content):
        content["title"] = cell.title

        if content["type"] == "text":
            return content

        custom_settings = content["fieldConfig"]["defaults"]["custom"]
        if cell.yaxis_to_label and SystemFields.LeftAxis in cell.yaxis_to_label:
            custom_settings["axisLabel"] = cell.yaxis_to_label[SystemFields.LeftAxis]
        if cell.display_legend is not None:
            if "hideFrom" not in custom_settings:
                custom_settings["hideFrom"] = {}
            custom_settings["hideFrom"]["legend"] = not cell.display_legend

        return content

    def on_row(self, row, cells):
        self.row_heights.append(row.height)
        return cells

    def on_rowset(self, rowset, rows):
        assert len(rows) == len(self.row_heights)

        max_len = max(len(row) for row in rows)
        width = self.BOARD_WIDTH // max_len

        def _get_grid_pos(i, x_shift, height):
            return dict(x=x_shift + i * width, y=self.vertical_offset, w=width, h=height)

        for row, specified_height in zip(rows, self.row_heights):
            row_height = specified_height if specified_height is not None else self.default_row_height
            x_shift = (self.BOARD_WIDTH - len(row) * width) // 2
            for i, cell in enumerate(row):
                cell["gridPos"] = _get_grid_pos(i, x_shift, row_height)
                self.panels.append(cell)
            self.vertical_offset += row_height

        self.row_heights = []

        return rows

    def dashboard_parameters_to_dict(self, parameters):
        result = []
        for parameter in parameters:
            if parameter["backends"] and "grafana" not in parameter["backends"]:
                continue
            dct = {
                "name": parameter["name"],
                "label": parameter["title"],
            }
            for arg in parameter.get("args", []):
                if isinstance(arg, GrafanaTextboxDashboardParameter):
                    dct.update(arg.dict)
            result.append(dct)
        return result

    def on_dashboard(self, dashboard, rowsets):
        result = {
            "panels": self.panels,
        }
        if dashboard.title is not None:
            result["title"] = dashboard.title
        if dashboard.parameters is not None:
            result["templating"] = {
                "list": self.dashboard_parameters_to_dict(dashboard.parameters)
            }
        elif self.template_tag_postprocessor.template_keys:
            keys = set()
            for k in self.template_tag_postprocessor.template_keys:
                if isinstance(k, str):
                    keys.add(k)
                elif isinstance(k, GrafanaTag):
                    keys.add(k.value)
            keys = sorted(keys)
            result["templating"] = {
                "list": [{
                    "name": k,
                    "label": k.capitalize().replace("_", " "),
                    **GrafanaTextboxDashboardParameter().dict,
                } for k in keys]
            }

        return result


# TODO: Change it to be similar to MonitoringDebugSerializer.
class GrafanaDebugSerializer(GrafanaSerializerBase):
    def __init__(self, tag_postprocessor=None):
        super().__init__(tag_postprocessor)

    def on_cell_content(self, content):
        if issubclass(type(content), Text):
            return break_long_lines_in_multiline_cell(content.text)

        if issubclass(type(content), Title):
            return break_long_lines_in_multiline_cell(content.title)

        query_parts, other_tags = self._prepare_expr_query(content)
        _, name = self.tag_postprocessor.postprocess(content.get_tags().items(), content.sensor)

        if other_tags.get(GrafanaSystemTags.Rate, False):
            query_parts.append("$rate=True")

        minValue, maxValue, axis = other_tags.get(SystemFields.Range, (None, None, None))
        if axis is not None and axis != SystemFields.LeftAxis:
            raise Exception("Grafana dashboard generator supports only left axis")
        if minValue is not None:
            query_parts.append(f"$min={minValue}")
        if maxValue is not None:
            query_parts.append(f"$max={maxValue}")

        if other_tags.get(SystemFields.Stack, False):
            query_parts.append("$stack=True")

        data = "\n".join(query_parts)
        return break_long_lines_in_multiline_cell(data)

    def on_row(self, row, cells):
        return cells


class GrafanaDashboardParser():
    def parse(self, panels, diff_format=False):
        rows = []
        last_y = -1

        for cell in panels:
            current_y = cell["gridPos"]["y"]
            if current_y != last_y:
                rows.append(([], []) if diff_format else [])
                last_y = current_y

            if cell["type"] != "timeseries":
                continue

            title, data = self._cell_from_json(cell)
            if diff_format:
                rows[-1][0].append(title)
                rows[-1][1].append(data)
            else:
                rows[-1].append(break_long_lines_in_multiline_cell(data))
        return rows

    def _cell_from_json(self, json):
        """Returns: (name, cell)"""
        grammar = lark.Lark('''
            %ignore " "

            start: expr | rate
            rate: "rate(" expr "[$__rate_interval])"
            !_op: "=" | "=~" | "!=" | "!~"
            label: /[\w]+/ _op (/"[^"]+"/| /""/)

            expr: "{" label ("," label) * "}"
        ''')  # noqa: W605

        class Visitor(lark.Visitor):
            def __init__(self):
                self.tags = []
                self.name = None

            def label(self, x):
                assert len(x.children) == 3
                lhs, op, rhs = map(str, x.children)
                if lhs == "__name__":
                    self.name = rhs
                self.tags.append(lhs + op + rhs)

            def rate(self, x):
                self.tags.append("$rate=True")

        assert len(json["targets"]) == 1
        expr = json["targets"][0]["expr"]
        visitor = Visitor()
        visitor.visit(grammar.parse(expr))
        tags = visitor.tags
        assert visitor.name is not None

        config = json.get("fieldConfig", {}).get("defaults", {})
        if "min" in config:
            tags.append(f'$min={config["min"]}')
        if "max" in config:
            tags.append(f'$max={config["max"]}')
        if config.get("custom", {}).get("stacking", {}).get("mode") == "normal":
            tags.append("$stack=True")

        return (visitor.name, "\n".join(visitor.tags))
