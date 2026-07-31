"""E2E test: the exactly-once state carriers of the C++ companion.

The "counter" transform is hosted by a C++ companion binary: it counts words in
the internal "word-state" state, mirrors the counts into a TSimpleExternalState
table, and emits every word once. The test asserts the internal states persisted
by the worker, the external state table, and the output uniqueness.
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


class Test(FlowTestCppCompanionBase):
    CPP_COMPANION_BINARY = yatest.common.binary_path("yt/yt/flow/tests/companion/all_states_cpp/companion/companion")

    def _prepare_environment(self, input_queue):
        tablet_count = 2
        run_yt_sync(self.primary_cluster_name, self.work_yt_path, tablet_count)
        logs, expected_counts = generate_log(tablet_count)
        batching_write_rows(logs, lambda batch: self.client.insert_rows(input_queue, batch), 1000)
        self._expected_counts = expected_counts

    def _prepare_pipeline_config(self, input_queue, input_consumer, output_queue, word_state):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        cluster = self.primary_cluster_name
        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster={cluster}>{input_queue}",
                "consumer_path": f"<cluster={cluster}>{input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["counter"]["sinks"]["queue"]["parameters"]["queue_path"] = output_queue
        pipeline_config["spec"]["computations"]["counter"]["external_state_managers"]["/word-state-external"][
            "parameters"
        ]["path"] = word_state

        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["sergeypozdeev"])
    def test_all_states(self):
        input_queue = f"{self.work_yt_path}/input_queue"
        input_consumer = f"{self.work_yt_path}/consumer"
        unique_words_queue = f"{self.work_yt_path}/unique_words_queue"
        word_state = f"{self.work_yt_path}/word_state"

        self._prepare_environment(input_queue)
        pipeline_config_path = self._prepare_pipeline_config(
            input_queue, input_consumer, unique_words_queue, word_state
        )

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=1,
            controllers_count=1,
        ):
            self.wait_pipeline_state("completed", timeout=240)
            logging.info("pipeline completed")

            # Internal state: the persisted "/word-state" counters must match.
            got_counts = {}
            for state in self.client.select_rows(f"key, name, state FROM [{self.pipeline_path}/states]"):
                if state["name"] != "/word-state":
                    continue
                got_counts[state["key"][1]] = yson.loads(state["state"]["payload"].encode())
            logging.info("Got internal counts: %s", got_counts)
            assert self._expected_counts == got_counts

            # External state: the TSimpleExternalState table mirrors the counts.
            got_external_counts = {}
            for row in self.client.select_rows(f"* FROM [{word_state}]"):
                got_external_counts[row["word"]] = row["count"]
            logging.info("Got external counts: %s", got_external_counts)
            assert self._expected_counts == got_external_counts

            # Output: every word emitted exactly once.
            output_rows = list(self.client.select_rows(f"* from [{unique_words_queue}]"))
            output_words = [row["word"] for row in output_rows]
            assert len(output_words) == len(set(output_words))
            assert self._expected_counts.keys() == set(output_words)
            logging.info("check completed")
