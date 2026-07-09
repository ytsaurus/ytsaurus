import os
import pytest

import yatest.common

from yt.common import wait

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

REANIMATE_BINARY = yatest.common.binary_path(
    "yt/yt/flow/tools/reanimate_vanilla_operation/reanimate_vanilla_operation"
)

_TERMINAL_STATES = ("completed", "failed", "aborted")

SECRET_ENV = "YT_MY_SECRET"
SECRET_VALUE = "topsecret"

##################################################################


class TestReanimateVanillaCpp(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    @property
    def output_table(self):
        return f"{self.work_yt_path}/output"

    def prepare_pipeline_config(self):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        pipeline_config["spec"]["computations"]["sink"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = self.output_table
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, "pipeline.yson")

    def _current_operation_id(self, operation_client=None):
        # The manifest records only the operation alias; a live operation resolves through the
        # scheduler runtime state of the cluster it runs on.
        operation_client = operation_client or self.client
        alias = self.client.get(f"{self.pipeline_path}/@current_vanilla_operation/alias")
        return operation_client.get_operation(operation_alias=alias, attributes=["id"], include_runtime=True)["id"]

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

    def _total_count(self):
        rows = list(self.client.select_rows(f"sum(count) as total from [{self.output_table}] group by 1 as g"))
        return rows[0]["total"] if rows else 0

    def _any_secret(self):
        rows = list(self.client.select_rows(f"secret from [{self.output_table}] limit 1"))
        return rows[0]["secret"] if rows else None

    @pytest.mark.authors(["mikari"])
    def test_reanimate(self):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            use_vanilla_jobs=True,
            vanilla_secret_env=[SECRET_ENV],
            additional_env={SECRET_ENV: SECRET_VALUE},
        ):
            self.wait_pipeline_state("working", timeout=300)

            # @current_vanilla_operation is only a pointer to the operation (by alias); the secret_env
            # names live in the persisted spec, not in the attribute.
            manifest = self.client.get(f"{self.pipeline_path}/@current_vanilla_operation")
            assert set(manifest.keys()) == {"cluster", "alias", "proxy_role"}
            spec = self.client.get(f"{self.pipeline_path}/vanilla/current_spec")
            assert spec["secret_env"] == [SECRET_ENV]
            assert "secure_vault" not in spec

            # The pipeline must actually process messages and the secret must reach the job: the sink
            # writes a growing count and the secret value into the output table.
            wait(lambda: self._total_count() > 0, timeout=300)
            assert self._any_secret() == SECRET_VALUE
            count_before = self._total_count()

            # Simulate a crash: abort the operation. The pipeline keeps its persisted working state
            # but loses its controller, so it cannot make progress until reanimated.
            original_operation = self._current_operation_id()
            self.client.abort_operation(original_operation)
            wait(
                lambda: self.client.get_operation(original_operation, attributes=["state"])["state"]
                in _TERMINAL_STATES,
                timeout=300,
            )

            # Reanimate from the manifest: must bring up a fresh operation of the same version.
            self._reanimate()
            wait(lambda: self._current_operation_id() != original_operation, timeout=300, ignore_exceptions=True)
            self.wait_pipeline_state("working", timeout=300)

            # Processing resumes after reanimate (count keeps growing) and the secret is still
            # delivered to the reanimated operation.
            wait(lambda: self._total_count() > count_before, timeout=300)
            assert self._any_secret() == SECRET_VALUE

    @pytest.mark.authors(["mikari"])
    def test_reanimate_cross_cluster(self):
        # The pipeline node lives on the primary cluster, but the vanilla operation runs on a
        # remote runtime cluster. Reanimate must read the manifest from the pipeline cluster and
        # resubmit the operation on the runtime cluster recorded in it.
        runtime_cluster_name = self.remote_cluster_names[0]
        runtime_cluster_url = self.cluster_name_to_url[runtime_cluster_name]
        runtime_client = self.cluster_name_to_client[runtime_cluster_name]

        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            use_vanilla_jobs=True,
            vanilla_secret_env=[SECRET_ENV],
            additional_env={SECRET_ENV: SECRET_VALUE},
            vanilla_runtime_cluster=runtime_cluster_url,
        ):
            self.wait_pipeline_state("working", timeout=300)
            wait(lambda: self._total_count() > 0, timeout=300)
            count_before = self._total_count()

            # A durable copy of the job resources must live in the pipeline folder, so reanimate can
            # re-stage them onto the runtime cluster without depending on the runtime file cache.
            assert self.client.list(f"{self.pipeline_path}/vanilla/files")

            # The operation lives on the runtime cluster, so it must be aborted there.
            original_operation = self._current_operation_id(runtime_client)
            runtime_client.abort_operation(original_operation)
            wait(
                lambda: runtime_client.get_operation(original_operation, attributes=["state"])["state"]
                in _TERMINAL_STATES,
                timeout=300,
            )

            self._reanimate()
            wait(
                lambda: self._current_operation_id(runtime_client) != original_operation,
                timeout=300,
                ignore_exceptions=True,
            )
            self.wait_pipeline_state("working", timeout=300)

            # Processing resumes after the cross-cluster reanimate and the secret is still delivered.
            wait(lambda: self._total_count() > count_before, timeout=300)
            assert self._any_secret() == SECRET_VALUE
