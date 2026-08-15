"""E2E test: the exactly-once state carriers of the C++ companion.

The companion-hosted source enriches rows from internal state and a joined
external table. The downstream transform counts words in internal state, mirrors
the counts into a mutable external table, joins a table keyed by "tag" (not by
the computation's key) and emits every word once.
"""

import logging
import random
import string

import pytest

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
        ):
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
