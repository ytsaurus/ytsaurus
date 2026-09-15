"""E2E coverage for C++ companion state and metric export."""

import json
import logging
import random
import string
import time

import pytest
import requests

import yatest.common
import yt.yson as yson

from yt.yt.flow.library.python.integration_test_base.yt_flow_cpp_base import (
    FlowTestCppCompanionBase,
)
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

TOTAL_EVENTS = 300

##################################################################


def generate_log(tablet_count):
    expected_counts = {}
    result = []
    for i in range(TOTAL_EVENTS):
        word = "".join(random.choices(string.ascii_lowercase, k=(random.randint(1, 4))))
        expected_counts[word] = expected_counts.get(word, 0) + 1
        result.append(
            {
                "word": word,
                "$tablet_index": i % tablet_count,
            }
        )
    return result, expected_counts


def parse_state_payload(payload):
    return yson.loads(yson.get_bytes(payload))


def fetch_merged_sensors(monitoring_port):
    """Fetch merged node and companion sensors."""
    response = requests.get(
        f"http://localhost:{monitoring_port}/solomon_proxy/sensors",
        headers={"Accept": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return json.loads(response.text)["sensors"]


def find_sensor(sensors, name, labels=None):
    """Find a sensor, accepting raw and rate-converted counter names."""
    names = {name, f"{name}.rate"}
    for sensor in sensors:
        sensor_labels = sensor["labels"]
        if sensor_labels.get("sensor") not in names:
            continue
        if labels and any(sensor_labels.get(key) != value for key, value in labels.items()):
            continue
        return sensor
    return None


def tag_weight(tag):
    # Position-weighted, so a word and its reversed tag get different weights.
    return sum(index * ord(letter) for index, letter in enumerate(tag, start=1))


class Test(FlowTestCppCompanionBase):
    CPP_COMPANION_BINARY = yatest.common.binary_path("yt/yt/flow/tests/companion/all_states_cpp/companion/companion")

    def _prepare_environment(self, input_queue, word_metadata, tag_metadata):
        tablet_count = 2
        run_yt_sync(self.primary_cluster_name, self.work_yt_path, tablet_count)
        logs, expected_counts = generate_log(tablet_count)
        batching_write_rows(logs, lambda batch: self.client.insert_rows(input_queue, batch), 1000)
        self.client.insert_rows(
            word_metadata,
            [{"word": word, "tag": word[::-1]} for word in expected_counts],
        )
        self.client.insert_rows(
            tag_metadata,
            [{"tag": word[::-1], "weight": tag_weight(word[::-1])} for word in expected_counts],
        )
        self._expected_counts = expected_counts
        self._tablet_count = tablet_count

    def _prepare_pipeline_config(
        self,
        input_queue,
        input_consumer,
        source_output_queue,
        output_queue,
        word_metadata,
        word_state,
        tag_metadata,
    ):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        cluster = self.primary_cluster_name
        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster={cluster}>{input_queue}",
                "consumer_path": f"<cluster={cluster}>{input_consumer}",
                "finite": True,
            }
        )
        reader = pipeline_config["spec"]["computations"]["reader"]
        reader["sinks"]["source-output"]["parameters"]["queue_path"] = source_output_queue
        reader["external_state_joiners"]["/word-metadata"]["parameters"]["path"] = word_metadata

        counter = pipeline_config["spec"]["computations"]["counter"]
        counter["sinks"]["queue"]["parameters"]["queue_path"] = output_queue
        counter["external_state_managers"]["/word-state-external"]["parameters"]["path"] = word_state
        counter["external_state_joiners"]["/tag-metadata"]["parameters"]["path"] = tag_metadata

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    def test_all_states(self):
        input_queue = f"{self.work_yt_path}/input_queue"
        input_consumer = f"{self.work_yt_path}/consumer"
        source_output_queue = f"{self.work_yt_path}/source_output_queue"
        unique_words_queue = f"{self.work_yt_path}/unique_words_queue"
        word_metadata = f"{self.work_yt_path}/word_metadata"
        word_state = f"{self.work_yt_path}/word_state"
        tag_metadata = f"{self.work_yt_path}/tag_metadata"

        self._prepare_environment(input_queue, word_metadata, tag_metadata)
        pipeline_config_path = self._prepare_pipeline_config(
            input_queue,
            input_consumer,
            source_output_queue,
            unique_words_queue,
            word_metadata,
            word_state,
            tag_metadata,
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ) as federation:
            self.wait_pipeline_state("completed", timeout=240)
            logging.info("pipeline completed")

            states = list(self.client.select_rows(f"key, name, state FROM [{self.pipeline_path}/states]"))

            # Ordered-source state is keyed by source partition and drives every output row.
            source_counts = sorted(
                parse_state_payload(state["state"]["payload"]) for state in states if state["name"] == "/reader-state"
            )
            expected_partition_count = TOTAL_EVENTS // self._tablet_count
            assert source_counts == [expected_partition_count] * self._tablet_count

            source_rows = list(self.client.select_rows(f"* FROM [{source_output_queue}]"))
            assert len(source_rows) == TOTAL_EVENTS
            assert all(row["tag"] == row["word"][::-1] for row in source_rows)
            assert sorted(row["source_sequence"] for row in source_rows) == sorted(
                list(range(1, expected_partition_count + 1)) * self._tablet_count
            )

            # Internal state: the persisted "/word-state" counters must match.
            got_counts = {}
            for state in states:
                if state["name"] != "/word-state":
                    continue
                got_counts[state["key"][1]] = parse_state_payload(state["state"]["payload"])
            logging.info("Got internal counts: %s", got_counts)
            assert self._expected_counts == got_counts

            # External state: the TSimpleExternalState table mirrors the counts.
            got_external_counts = {}
            for row in self.client.select_rows(f"* FROM [{word_state}]"):
                got_external_counts[row["word"]] = row["count"]
            logging.info("Got external counts: %s", got_external_counts)
            assert self._expected_counts == got_external_counts

            # Output: every word emitted exactly once, carrying the tag-keyed joined weight.
            output_rows = list(self.client.select_rows(f"* from [{unique_words_queue}]"))
            output_words = [row["word"] for row in output_rows]
            assert len(output_words) == len(set(output_words))
            assert self._expected_counts.keys() == set(output_words)
            assert all(row["tag_weight"] == tag_weight(row["word"][::-1]) for row in output_rows)
            logging.info("check completed")

            self._check_companion_sensors(federation)

    def _check_companion_sensors(self, federation):
        """Check that the merged endpoint contains companion and node sensors."""
        worker = federation.workers[0]

        # Wait for the companion exporter's next collection.
        sensors = self._wait_for_sensor(
            worker.monitoring_port,
            "yt.flow.worker.computation.processed_message_count",
            {"computation_id": "counter", "flow_process": "companion"},
        )

        # Node sensors remain present and untagged as companion.
        node_sensor = find_sensor(sensors, "yt.flow.worker.job_count", {"computation_id": "counter"})
        assert node_sensor is not None, "node sensors are missing from the merged endpoint"
        assert node_sensor["labels"].get("flow_process") is None

    def _wait_for_sensor(self, monitoring_port, name, labels, timeout=90, period=2):
        deadline = time.monotonic() + timeout
        while True:
            sensors = fetch_merged_sensors(monitoring_port)
            if find_sensor(sensors, name, labels) is not None:
                return sensors
            if time.monotonic() >= deadline:
                companion_sensors = sorted(
                    {
                        sensor["labels"]["sensor"]
                        for sensor in sensors
                        if sensor["labels"].get("flow_process") == "companion"
                    }
                )
                raise AssertionError(
                    f"sensor {name} with {labels} is missing from the merged endpoint; "
                    f"companion sensors seen: {companion_sensors}"
                )
            time.sleep(period)
