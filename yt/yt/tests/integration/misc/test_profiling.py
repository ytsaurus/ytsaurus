from yt_env_setup import YTEnvSetup

from yt_commands import (authors, get, raises_yt_error)

from yt.common import YtResponseError

import pytest
import requests
import simplejson
import json

##################################################################


def try_parse_yt_error_headers(rsp):
    if "X-YT-Error" in rsp.headers:
        assert "X-YT-Framing" not in rsp.headers
        raise YtResponseError(json.loads(rsp.headers.get("X-YT-Error")))
    rsp.raise_for_status()


##################################################################


class Sensor:
    def __init__(self, name, value, timestamp, kind=None, labels=None):
        self.name = name
        self.value = value
        self.timestamp = timestamp
        self.kind = kind
        self.labels = labels or {}

    def from_json(json):
        try:
            return Sensor(
                json["labels"]["sensor"],
                float(json["value"]) if "value" in json else json["hist"],
                json["ts"],
                json["kind"],
                json["labels"],
            )
        except KeyError as e:
            raise ValueError(f"Invalid sensor JSON: {json}") from e

    # This might not work for histograms or some other types, but we don't check them (for now).
    def from_prometheus_line(line, kind):
        # yt_http_connections_accepted{yt_aggr="1", another_label="5"} 1 1733941661000
        # yt_tablet_server_tablet_cell_decommissioner_check_orphans_max 0.000003 1733943237000
        try:
            labels = {}
            if "{" in line:
                name, rem = line.split("{")
                labels_raw, rem = rem.split("} ")
                labels = {key: value.strip("\"") for key, value in [label.split("=") for label in labels_raw.split(", ") if label]}
                value, timestamp = rem.split(" ")
                value = float(value)
            else:
                name, value, timestamp = line.split(" ")
            return Sensor(name, value, timestamp, kind=kind, labels=labels)
        except ValueError as e:
            raise ValueError(f"Invalid Prometheus sensor line: {line}") from e

    @classmethod
    def is_prometheus_type_line(cls, line):
        return line.startswith("# TYPE")

    @classmethod
    def infer_prometheus_type(cls, line):
        assert line.startswith("# TYPE")
        prometheus_type = line.split(" ")[3]

        if prometheus_type == "gauge":
            return "GAUGE"
        elif prometheus_type == "counter":
            return "RATE"
        elif prometheus_type == "histogram":
            return "HIST"

        # TODO(achulkov2): Maybe replace with dummy type and dummy sensor.
        raise ValueError(f"Unsupported Prometheus type: {prometheus_type}")

    def __str__(self):
        return f"Sensor(name={self.name}, labels={self.labels}, kind={self.kind}, value={self.value}, ts={self.timestamp})"

    def __repr__(self):
        return str(self)


class SensorSet:
    def __init__(self, sensors):
        self.sensors = sensors

    def match_singular(self, **kwargs):
        matches = self.match(**kwargs)
        assert len(matches) == 1
        return matches[0]

    def match(self, full_name=None, ordered_name_substrings=None, labels={}):
        def match_function(sensor):
            substring_criteria = True
            substring_positions = [sensor.name.lower().find(substring.lower()) for substring in (ordered_name_substrings or [])]
            for i, position in enumerate(substring_positions):
                if position == -1:
                    substring_criteria = False
                    break
                if i > 0 and position <= substring_positions[i - 1]:
                    substring_criteria = False
                    break

            return all((
                full_name is None or sensor.name == full_name,
                ordered_name_substrings is None or substring_criteria,
                all((sensor.labels.get(key, "") == value for key, value in labels.items())),
            ))

        return [sensor for sensor in self.sensors if match_function(sensor)]

    def check_condition(self, condition, **kwargs):
        matches = self.match(**kwargs)
        for match in matches:
            assert condition(match), f"Condition failed for sensor: {match}"


# There are two suites of test for profiling: unit tests in library/profiling/unittests
# and the integration tests in this file. The intent of these test is to at least check
# the ability to override scraping options via scrape parameters, but they are also used
# to check some of the newer functionality. The integration tests are not exhaustive and
# some older functionality is only tested in unit tests. In general, it is up to your
# personal preference where to add new tests.
class TestProfiling(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 0

    ENABLE_RESOURCE_TRACKING = True

    DELTA_MASTER_CONFIG = {
        "solomon_exporter": {
            "force_convert_counters_to_rate_gauge_for_solomon": False,
            "force_convert_counters_to_delta_gauge_for_solomon": False,
            "force_enable_aggregation_workaround_for_solomon": False,
        }
    }

    def _get_endpoint(self):
        monitoring_port = self.Env.configs["master"][0]["monitoring_port"]
        return f"http://localhost:{monitoring_port}/solomon/all"

    def get_sensors_raw(self, **kwargs):
        rsp = requests.get(self._get_endpoint(), **kwargs)
        try_parse_yt_error_headers(rsp)
        return rsp

    def _get_build_version(self):
        master_address = self.Env.configs["master"][0]["primary_master"]["addresses"][0]
        return get(f"//sys/primary_masters/{master_address}/orchid/build_info/binary_version")

    def get_sensors(self, format="json", params=None):
        assert format in ("json", "prometheus")
        # JSON is the default format.
        headers = {"Accept": "text/plain"} if format == "prometheus" else {}

        raw_sensors = self.get_sensors_raw(headers=headers, params=params)
        try_parse_yt_error_headers(raw_sensors)
        parsed_sensors = []

        if format == "json":
            try:
                for sensor in raw_sensors.json()["sensors"]:
                    parsed_sensors.append(Sensor.from_json(sensor))
            except simplejson.JSONDecodeError as e:
                raise ValueError(f"Invalid sensor JSON: {raw_sensors.text}") from e
        elif format == "prometheus":
            type = "GAUGE"
            for line in raw_sensors.text.split("\n"):
                if Sensor.is_prometheus_type_line(line):
                    type = Sensor.infer_prometheus_type(line)
                    continue
                if line:
                    parsed_sensors.append(Sensor.from_prometheus_line(line, type))

        return SensorSet(parsed_sensors)

    @authors("achulkov2")
    @pytest.mark.parametrize("format", ["json", "prometheus"])
    def test_basic(self, format):
        build_version = self._get_build_version()

        build_version_sensor = self.get_sensors(format=format).match_singular(ordered_name_substrings=["yt", "build", "version"])
        assert build_version_sensor.value == 1.0
        assert build_version_sensor.kind == "GAUGE"
        assert build_version_sensor.labels["version"] == build_version
        assert build_version_sensor.labels["yt_aggr"] == "1"

    @authors("achulkov2")
    def test_sensor_name_configuration(self):
        sensors = self.get_sensors(params={
            "convert_sensor_component_names_to_camel_case": r"%true",
            # We need to add quotes, otherwise YSON parser tries to parse an integer due to leading minus sign.
            "sensor_component_delimiter": "\"-_-\""})
        build_version_sensor = sensors.match_singular(
            ordered_name_substrings=["yt", "resource", "tracker", "total", "cpu"],
            labels={"thread": "Automaton"})
        assert build_version_sensor.name == "Yt-_-ResourceTracker-_-TotalCpu"

    @authors("achulkov2")
    @pytest.mark.parametrize("conversion_type", ["rate", "delta"])
    @pytest.mark.parametrize("rename_converted_counters", [True, False])
    def test_counter_to_rate_conversions(self, conversion_type, rename_converted_counters):
        def is_converted_gauge(sensor):
            return sensor.kind == "GAUGE" and not (rename_converted_counters ^ sensor.name.endswith(conversion_type))

        def is_rate(sensor):
            return sensor.kind == "RATE" and not sensor.name.endswith(conversion_type)

        sensors = self.get_sensors(params={
            f"convert_counters_to_{conversion_type}_gauge": "true",
            "rename_converted_counters": "true" if rename_converted_counters else "false",
            "add_metric_type_label": "true"})

        sensors.check_condition(is_converted_gauge, ordered_name_substrings=["yt", "hydra", "mutation", "count"])

        # JSON is considered a Solomon format due to historical reasons.
        sensors = self.get_sensors(params={
            f"convert_counters_to_{conversion_type}_gauge": "false",
            f"force_convert_counters_to_{conversion_type}_gauge_for_solomon": "true",
            "rename_converted_counters": "true" if rename_converted_counters else "false",
            "add_metric_type_label": "true"})

        sensors.check_condition(is_converted_gauge, ordered_name_substrings=["yt", "hydra", "mutation", "count"])

        # But you can turn these conversions off even for Solomon formats.
        sensors = self.get_sensors(params={
            f"convert_counters_to_{conversion_type}_gauge": "false",
            f"force_convert_counters_to_{conversion_type}_gauge_for_solomon": "false",
            "rename_converted_counters": "true" if rename_converted_counters else "false",
            "add_metric_type_label": "true"})

        sensors.check_condition(is_rate, ordered_name_substrings=["yt", "hydra", "mutation", "count"])

        # Prometheus is not a Solomon format.
        sensors = self.get_sensors(format="prometheus", params={
            f"convert_counters_to_{conversion_type}_gauge": "false",
            f"force_convert_counters_to_{conversion_type}_gauge_for_solomon": "true",
            "rename_converted_counters": "true" if rename_converted_counters else "false",
            "add_metric_type_label": "true"})

        sensors.check_condition(is_rate, ordered_name_substrings=["yt", "hydra", "mutation", "count"])

        # But you can get the conversion for Prometheus if you want.
        sensors = self.get_sensors(format="prometheus", params={
            f"convert_counters_to_{conversion_type}_gauge": "true",
            "rename_converted_counters": "true" if rename_converted_counters else "false",
            "add_metric_type_label": "true"})

        sensors.check_condition(is_converted_gauge, ordered_name_substrings=["yt", "hydra", "mutation", "count"])

    @authors("achulkov2")
    def test_yson_parameter_parsing(self):
        with raises_yt_error("Cannot parse string"):
            self.get_sensors(params={
                "convert_sensor_component_names_to_camel_case": r"%true",
                "sensor_component_delimiter": "-_-"})

        with raises_yt_error("Character \"!\" is not allowed in \"sensor_component_delimiter\""):
            self.get_sensors(params={"sensor_component_delimiter": "\"!\""})

        with raises_yt_error("both set"):
            self.get_sensors(params={
                "convert_counters_to_rate_gauge": "true",
                "convert_counters_to_delta_gauge": "true"})

        # Booleans can be parsed without %.
        sensors = self.get_sensors(params={
            "convert_sensor_component_names_to_camel_case": r"true"
        })
        build_version_sensor = sensors.match_singular(ordered_name_substrings=["yt", "build", "version"])
        assert build_version_sensor.name == "Yt.Build.Version"
