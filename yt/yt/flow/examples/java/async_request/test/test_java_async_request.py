import pytest
import random
import string

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_java_base import FlowTestJavaBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/../async_request/src/main/resources/pipeline.yson"
)

if yatest.common.context.sanitize is not None:
    EXPECTED_ANSWER = {
        1844674407370955161: 1,
        3689348814741910322: 4,
        5534023222112865483: 3,
        7378697629483820644: 2,
        9223372036854775805: 1,
        11068046444225730966: 5,
        12912720851596686127: 3,
        14757395258967641288: 4,
        16602069666338596449: 5,
        18446744073709551610: 6,
    }
else:
    EXPECTED_ANSWER = {
        1844674407370955161: 554,
        3689348814741910322: 234,
        5534023222112865483: 334,
        7378697629483820644: 651,
        9223372036854775805: 425,
        11068046444225730966: 442,
        12912720851596686127: 323,
        14757395258967641288: 233,
        16602069666338596449: 756,
        18446744073709551610: 524,
    }

#################################################################


def generate_word(length):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


def generate_log(tablet_count):
    result = []
    for key, total_length in EXPECTED_ANSWER.items():
        while total_length > 0:
            if total_length < 5:
                length = total_length
            else:
                length = random.randint(0, total_length)
            total_length -= length
            result.append({"key": key, "data": generate_word(length), "$tablet_index": len(result) % tablet_count})
    random.shuffle(result)
    return result


class TestAsyncRequest(FlowTestJavaBase):
    JAVA_RUNNER_BINARY_DIR = yatest.common.binary_path(f"{yatest.common.context.project_path}/../async_request/")
    JAVA_RUNNER_MAIN_CLASS = "tech.ytsaurus.flow.examples.asyncrequest.RunnerMain"

    def setup_method(self, method):
        super().setup_method(method)

        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.data_state = self.work_yt_path + "/data_state"

    def prepare_environment(self):
        tablet_count = 5
        run_yt_sync("primary", self.work_yt_path, tablet_count)
        batching_write_rows(generate_log(tablet_count), lambda batch: self.client.insert_rows(self.input_queue, batch), 1000)

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)

        pipeline_config["spec"]["computations"]["injector"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
                "finite": True,
            }
        )
        pipeline_config["spec"]["computations"]["state"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = self.data_state
        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize(
        ("workers_count", "controllers_count", "problems"),
        [
            pytest.param(1, 1, False, id="1c_1w_stable"),
            pytest.param(4, 1, False, id="1c_4w_stable"),
        ],
    )
    def test_basic(self, workers_count, controllers_count, problems):
        self.prepare_environment()
        pipeline_config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
            problems=problems,
        ):
            self.wait_pipeline_state("completed", timeout=500 if yatest.common.context.sanitize else 240)

            expr = f"* from [{self.data_state}]"
            rows = list(self.client.select_rows(expr))

            data = dict()
            for row in rows:
                data[row["key"]] = row["total_length"]
            assert data == EXPECTED_ANSWER
