import pytest
import random

import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_java_base import FlowTestJavaBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(
    f"{yatest.common.context.project_path}/../url_downloader/src/main/resources/pipeline.yson"
)

#################################################################


def generate_data(hosts, max_urls):
    rows = []
    for i in range(hosts):
        host = f"https://host_{i}"
        for j in range(max_urls // (i + 1)):
            url = f"{host}/item_{j}"
            url_len = len(url)
            d = sum([1 for c in url if c in "0123456789"])
            data = f"length: {url_len}, digits: {d}"
            rows.append({"host": host, "url": url, "data": data})

    return rows


def generate_answer(data):
    answer = dict()
    for row in data:
        answer[row["url"]] = (row["host"], row["data"])
    return answer


DATA = generate_data(50, 50)
EXPECTED_ANSWER = generate_answer(DATA)


def generate_log(tablet_count):
    result = []
    for row in DATA:
        result.append({"host": row["host"], "url": row["url"], "$tablet_index": len(result) % tablet_count})
    random.shuffle(result)
    return result


class TestUrlDownloader(FlowTestJavaBase):
    JAVA_RUNNER_BINARY_DIR = yatest.common.binary_path(f"{yatest.common.context.project_path}/../url_downloader/")
    JAVA_RUNNER_MAIN_CLASS = "tech.ytsaurus.flow.examples.urldownloader.RunnerMain"

    def setup_method(self, method):
        super().setup_method(method)
        self.input_queue = self.work_yt_path + "/input_queue"
        self.input_consumer = self.work_yt_path + "/consumer"
        self.output_queue = self.work_yt_path + "/output_queue"
        tablet_count = 5
        run_yt_sync("primary", self.work_yt_path, tablet_count)
        batching_write_rows(generate_log(tablet_count), lambda batch: self.client.insert_rows(self.input_queue, batch), 1000)

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        pipeline_config["spec"]["computations"]["url_reader"]["source_streams"]["queue"]["parameters"].update({
            "queue_path": f"<cluster=primary>{self.input_queue}",
            "consumer_path": f"<cluster=primary>{self.input_consumer}",
            "finite": True,
        })
        pipeline_config["spec"]["computations"]["url_downloader"]["sinks"]["queue"]["parameters"].update({
            "queue_path": f"<cluster=primary>{self.output_queue}",
        })
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize(("workers_count", "controllers_count"), [pytest.param(4, 1, id="1c_4w")])
    def test_basic(self, workers_count, controllers_count):
        pipeline_config_path = self.prepare_pipeline_config()
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": pipeline_config_path},
            workers_count=workers_count,
            controllers_count=controllers_count,
        ):
            self.wait_pipeline_state("completed", timeout=500 if yatest.common.context.sanitize else 240)
            rows = list(self.client.select_rows(f"host, url, data from [{self.output_queue}]"))
            data = {row["url"]: (row["host"], row["data"]) for row in rows}
            assert data == EXPECTED_ANSWER
