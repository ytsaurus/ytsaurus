import os
import pytest

import yatest.common
import yt.yson as yson

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_python_base import FlowTestPythonBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.queue import batching_write_rows

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline.yson")

REANIMATE_BINARY = yatest.common.binary_path(
    "yt/yt/flow/tools/reanimate_vanilla_operation/reanimate_vanilla_operation"
)

_TERMINAL_STATES = ("completed", "failed", "aborted")

SECRET_ENV = "YT_MY_SECRET"
SECRET_VALUE = "topsecret"

##################################################################


class TestReanimateVanillaPython(FlowTestPythonBase):
    PYTHON_COMPANION_BINARY = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    @property
    def input_queue(self):
        return f"{self.work_yt_path}/input_queue"

    @property
    def input_consumer(self):
        return f"{self.work_yt_path}/consumer"

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        pipeline_config["spec"]["computations"]["reader"]["source_streams"]["queue"]["parameters"].update(
            {
                "queue_path": f"<cluster=primary>{self.input_queue}",
                "consumer_path": f"<cluster=primary>{self.input_consumer}",
            }
        )
        pipeline_config["spec"]["resources"]["YTClientFactory"]["parameters"]["default_config"][
            "proxy_role"
        ] = self.RPC_PROXY_ROLE
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def _write_words(self, count):
        rows = [{"word": f"w{i}"} for i in range(count)]
        batching_write_rows(rows, lambda batch: self.client.insert_rows(self.input_queue, batch), 1000)

    def _current_operation_id(self):
        # The manifest records only the operation alias; a live operation resolves through the
        # scheduler runtime state.
        alias = self.client.get(f"{self.pipeline_path}/@current_vanilla_operation/alias")
        return self.client.get_operation(operation_alias=alias, attributes=["id"], include_runtime=True)["id"]

    def _reanimate(self):
        cluster_url = self.cluster_name_to_url[self.primary_cluster_name]
        # reanimate rebuilds the secure vault from the environment, so the secret declared in
        # secret_env must be present just like at the original launch.
        yatest.common.execute(
            [
                REANIMATE_BINARY,
                "--cluster",
                cluster_url,
                "--proxy-role",
                self.RPC_PROXY_ROLE,
                "--path",
                self.pipeline_path,
            ],
            env={**os.environ, SECRET_ENV: SECRET_VALUE},
        )

    def _states(self):
        # The states table also holds the reader's internal source bookkeeping (offsets, watermarks)
        # under "/$..." names; only the mapper's own state carries a user payload.
        rows = []
        for state in self.client.select_rows(f"* from [{self.pipeline_path}/states]"):
            s = state.get("state")
            if not isinstance(s, dict) or "payload" not in s:
                continue
            payload = s["payload"]
            if isinstance(payload, (bytes, str)):
                payload = yson.loads(payload.encode() if isinstance(payload, str) else payload)
            rows.append(payload)
        return rows

    def _total_count(self):
        return sum(row.get("count", 0) for row in self._states())

    def _any_secret(self):
        rows = self._states()
        return rows[0]["secret"] if rows else None

    @pytest.mark.authors(["mikari"])
    def test_reanimate(self):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        # Initial input the companion processes before the crash.
        self._write_words(200)

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            use_vanilla_jobs=True,
            vanilla_secret_env=[SECRET_ENV],
            additional_env={SECRET_ENV: SECRET_VALUE},
        ):
            self.wait_pipeline_state("working", timeout=600)

            # The python companion must process messages and the secret must reach it: the mapper
            # writes a growing per-word count and the secret value into its internal state.
            wait(lambda: self._total_count() > 0, timeout=300)
            assert self._any_secret() == SECRET_VALUE
            count_before = self._total_count()

            original_operation = self._current_operation_id()
            self.client.abort_operation(original_operation)
            wait(
                lambda: self.client.get_operation(original_operation, attributes=["state"])["state"]
                in _TERMINAL_STATES,
                timeout=300,
            )

            self._reanimate()
            wait(lambda: self._current_operation_id() != original_operation, timeout=300, ignore_exceptions=True)
            self.wait_pipeline_state("working", timeout=600)

            # Fresh input after reanimate must be processed (count grows) and the secret must still
            # be delivered to the reanimated companion.
            self._write_words(200)
            wait(lambda: self._total_count() > count_before, timeout=300)
            assert self._any_secret() == SECRET_VALUE
