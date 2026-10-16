import json

from urllib.request import Request, urlopen

import pytest
import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_SAME_KEY_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_same_key.yson"
)
PIPELINE_OVERRIDE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/pipeline/pipeline_override.yson"
)

USER_AMOUNTS = [
    ("user-0", 10),
    ("user-1", 20),
    ("user-2", 30),
    ("user-3", 40),
]

##################################################################


class Test(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    def setup_method(self, method):
        super(Test, self).setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.consumer = self.work_yt_path + "/consumer"
        self.output_table = self.work_yt_path + "/output_table"
        self.input_data = [{"UserId": user_id, "Amount": amount} for user_id, amount in USER_AMOUNTS]
        self.expected_output = sorted((user_id, amount) for user_id, amount in USER_AMOUNTS)

    def prepare_tables(self):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        self.client.insert_rows(self.input_queue, self.input_data)

    def prepare_pipeline_config(self, config_path, manual_preload, cache_ttl=None, compressed_cache=False, finite=True):
        pipeline_config = get_yson_config(config_path)

        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.consumer}",
                "finite": finite,
            }
        )

        if manual_preload:
            pipeline_config["spec"]["computations"]["joiner"]["state_joiners"]["/user_total"]["auto_preload"] = False

        if cache_ttl is not None:
            pipeline_config["dynamic_spec"]["computations"]["joiner"]["state_joiners"] = {
                "/user_total": {"cache": {"ttl": cache_ttl}}
            }

        if compressed_cache:
            pipeline_config["dynamic_spec"].setdefault("job_tracker", {}).setdefault("state_cache", {}).update(
                {"uncompressed_cache_weight": 0, "compressed_cache_weight": 1024 * 1024}
            )

        pipeline_config["spec"]["computations"]["joiner"]["sinks"]["out"]["parameters"].update(
            {
                "table_path": f"<cluster=primary>{self.output_table}",
            }
        )

        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def get_output(self):
        rows = list(self.client.select_rows(f"UserId, Total FROM [{self.output_table}]"))
        return sorted((row["UserId"], row["Total"]) for row in rows)

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("config_path", "manual_preload", "cache_ttl"),
        [
            pytest.param(PIPELINE_SAME_KEY_CONFIG_PATH, False, None, id="same_key"),
            pytest.param(PIPELINE_OVERRIDE_CONFIG_PATH, False, None, id="key_schema_override"),
            pytest.param(PIPELINE_SAME_KEY_CONFIG_PATH, True, None, id="manual_preload"),
            pytest.param(PIPELINE_SAME_KEY_CONFIG_PATH, False, "60s", id="cached"),
        ],
    )
    def test_join(self, config_path, manual_preload, cache_ttl):
        self.prepare_tables()
        pipeline_config_path = self.prepare_pipeline_config(config_path, manual_preload, cache_ttl)
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ):
            self.wait_pipeline_state("completed", timeout=180)
            assert self.get_output() == self.expected_output

    @pytest.mark.authors(["sergeypozdeev"])
    def test_compressed_cache_hit_across_epochs(self):
        self.prepare_tables()
        pipeline_config_path = self.prepare_pipeline_config(
            PIPELINE_SAME_KEY_CONFIG_PATH, False, "60s", compressed_cache=True, finite=False
        )
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
        ) as federation:
            counters = {}

            def read_cache_counters():
                request = Request(
                    f"http://localhost:{federation.workers[0].monitoring_port}/solomon_proxy/sensors",
                    headers={"X-YT-IsSolomonPull": "0"},
                )
                with urlopen(request, timeout=5) as response:
                    sensors = json.load(response)["sensors"]
                counters.clear()
                for sensor in sensors:
                    labels = sensor["labels"]
                    name = labels["sensor"]
                    prefix = "yt.flow.worker.computation.job_state_cache."
                    if (
                        labels.get("computation_id") == "joiner"
                        and labels.get("name") == "/user_total"
                        and name.startswith(prefix)
                    ):
                        counters[name.removeprefix(prefix)] = sensor["value"]
                    if name == "yt.flow.worker.state_cache.hit_count" and labels.get("cache") == "compressed":
                        counters["compressed_hits"] = sensor["value"]
                return counters

            wait(lambda: self.get_output() == self.expected_output, timeout=180)
            # Reset inserts the joined states and clears the provider's per-epoch maps.
            wait(lambda: read_cache_counters().get("insert", 0) == len(USER_AMOUNTS), timeout=60)
            assert counters.get("extract_hit", 0) == 0

            user_id, _ = USER_AMOUNTS[0]
            # Require a fresh output write after reconstructing the cached state in the next epoch.
            self.client.insert_rows(self.output_table, [{"UserId": user_id, "Total": -1}])
            self.client.insert_rows(self.input_queue, [{"UserId": user_id, "Amount": 0}])
            wait(lambda: self.get_output() == self.expected_output, timeout=60)

            def has_compressed_joiner_hit():
                values = read_cache_counters()
                return values.get("extract_hit_not_expired", 0) == 1 and values.get("compressed_hits", 0) > 0

            wait(
                has_compressed_joiner_hit,
                timeout=60,
                error_message=lambda: f"Unexpected state cache counters: {counters}",
            )
