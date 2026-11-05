import logging
import os
import pytest
import yatest.common

from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config
from yt.yt.flow.library.python.integration_test_base.yt_sync_preset import run_yt_sync

from yt.wrapper.errors import YtResponseError
from yt.wrapper.flow_commands import get_controller_logs
from yt.common import wait

##################################################################

# The local YT of this suite runs with native-protocol TLS (see ya.make): its proxies cannot dial
# the plain-TCP Flow controller, exactly like a Managed YTsaurus cluster.
PIPELINE_CONFIG_PATH = yatest.common.source_path("yt/yt/flow/tests/direct_controller_commands/pipeline/pipeline.yson")
FLOW_BINARY_PATH = yatest.common.binary_path("yt/yt/flow/tests/direct_controller_commands/pipeline/pipeline")

# The cause the clients report when the RPC proxy cannot reach the controller.
TLS_ERROR = "Failed to establish TLS/SSL session"
# The controller refuses the first mutating command of a reader: it was discovered and the caller
# was authenticated, so the denial comes from the per-command check.
WRITE_DENIED_ERROR = 'No "write" permission for pipeline'
# A stranger is stopped earlier, by the cluster, on the flow_control lookup that discovers the leader.
DISCOVERY_DENIED_ERROR = 'Access denied for user "stranger": "read" permission'
# The controller names the cause when it skips the leadership confirmation through the proxy.
DIRECT_ONLY_WARNING = "the cluster requires TLS to connect to this controller"

##################################################################


def _to_str(output):
    return output.decode("utf-8") if isinstance(output, bytes) else output


def _read_file(path):
    with open(path) as file:
        return file.read()


def _make_ace(action, subject, permissions):
    return {
        "action": action,
        "subjects": [subject],
        "permissions": permissions,
        "inheritance_mode": "object_and_descendants",
    }


class TestDirectControllerCommands(FlowTestBase):
    FLOW_BINARY_PATH = FLOW_BINARY_PATH

    def setup_method(self, method):
        super(TestDirectControllerCommands, self).setup_method(method)

        # The proxies dial other services with TLS required; the plain-TCP controller cannot answer.
        assert self.client.get("//sys/@cluster_connection/bus_client/encryption_mode") == "required"

        self._runner_logs = []
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        self.pipeline_config_path = self.prepare_pipeline_config()
        self.direct_pipeline_config_path = self.prepare_pipeline_config(direct=True)

    def prepare_pipeline_config(self, direct=False):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        if direct:
            # The runner sends its pipeline commands to the controller itself.
            pipeline_config["direct_controller_commands"] = {"enabled": True}
        self.patch_config(pipeline_config)

        return self.dump_config_to_log_dir(pipeline_config, "pipeline_direct.yson" if direct else "pipeline.yson")

    # The runner. Each call returns (exit_code, stdout, stderr); `direct` picks the path to the controller.

    def _run(self, command, extra_env=None):
        env = self._runner_env()
        if extra_env:
            env.update(extra_env)
        result = yatest.common.execute(
            command,
            env=env,
            check_exit_code=False,
            timeout=300,
        )
        stdout = _to_str(result.std_out)
        stderr = _to_str(result.std_err)
        logging.info("%s: exit code %s, stderr tail: %s", command[:1], result.exit_code, stderr[-2000:])
        return result.exit_code, stdout, stderr

    def _runner_command(self, direct):
        config_path = self.direct_pipeline_config_path if direct else self.pipeline_config_path
        return [FLOW_BINARY_PATH, "--config", config_path]

    def _runner_env(self, user=None):
        env = dict(os.environ)
        # The runner must return once the pipeline is working; it does not follow the run.
        env["YT_FLOW_WAIT"] = "0"
        if user:
            # The user is set the way the harness does it on the auth-less local YT.
            env["YT_USER"] = user
        return env

    def _submit_spec_with_runner(self, direct, user=None):
        """Runs the runner to completion: it submits the pipeline specs and starts the pipeline."""
        return self._run(self._runner_command(direct), {"YT_USER": user} if user else None)

    def _submit_spec_with_runner_until(self, expected, direct, user=None, timeout=120):
        """Runs the runner until |expected| shows up in its stderr, then stops it.
        The runner retries a failed spec submit forever, so a failing run never exits on its own."""
        stderr_path = yatest.common.output_path(f"runner_{len(self._runner_logs)}.err")
        self._runner_logs.append(stderr_path)
        with open(stderr_path, "w") as stderr_file:
            process = yatest.common.execute(
                self._runner_command(direct),
                env=self._runner_env(user),
                stderr=stderr_file,
                wait=False,
                check_exit_code=False,
            )
            try:
                wait(lambda: expected in _read_file(stderr_path), timeout=timeout)
            finally:
                process.kill()

    def _controller_log_contains(self, substring):
        rows, _ = get_controller_logs(self.pipeline_path, count=5000, client=self.client)
        return any(substring in row["data"] for row in rows)

    def _start_federation(self, node_config=None):
        return self.start_flow_process_federation(
            pipeline_binary_args={"--config": self.pipeline_config_path},
            node_config=node_config,
            run_pipeline=False,
            wait_pipeline=False,
        )

    def _wait_for_direct_leader(self):
        """A leader that kept its leadership through the direct confirmation, the proxy path having failed."""
        wait(lambda: self._controller_log_contains(DIRECT_ONLY_WARNING), timeout=120)

    # Tests.

    @pytest.mark.authors(["timoninmaxim"])
    def test_spec_submit_needs_direct_mode_on_tls_cluster(self):
        """The user's view of the problem and its fix: with a leading controller the proxy path fails,
        and the same spec submit goes through once the runner talks to the controller itself."""
        with self._start_federation():
            self._wait_for_direct_leader()

            # The Python client through the HTTP proxy fails with the TLS cause. The proxy reports it
            # either bare or wrapped into the proxy path error, so only the cause is checked here.
            with pytest.raises(YtResponseError) as excinfo:
                self.client.get_pipeline_state(self.pipeline_path)
            assert excinfo.value.contains_text(TLS_ERROR), str(excinfo.value)
            with pytest.raises(YtResponseError) as excinfo:
                self.client.pause_pipeline(self.pipeline_path)
            assert excinfo.value.contains_text(TLS_ERROR), str(excinfo.value)

            # The runner through the proxy: the same cause before it touches the pipeline specs.
            self._submit_spec_with_runner_until(TLS_ERROR, direct=False)

            # The runner with the direct mode enabled runs the whole submit: set-flow-core-target,
            # set-pipeline-specs, set-target-pipeline-state and the get-pipeline-state polling.
            exit_code, _, stderr = self._submit_spec_with_runner(direct=True)
            assert exit_code == 0, stderr

            # Authorization is enforced by the controller, by the ACL of the pipeline node.
            for user in ("writer", "reader", "stranger"):
                self.client.create("user", attributes={"name": user})
            self.client.set(f"{self.pipeline_path}/@inherit_acl", False)
            self.client.set(
                f"{self.pipeline_path}/@acl",
                [
                    _make_ace("allow", "writer", ["read", "write"]),
                    _make_ace("allow", "reader", ["read"]),
                ],
            )

            # A writer runs the whole submit.
            exit_code, _, stderr = self._submit_spec_with_runner(direct=True, user="writer")
            assert exit_code == 0, stderr
            # A reader may query the pipeline, but not update it.
            self._submit_spec_with_runner_until(WRITE_DENIED_ERROR, direct=True, user="reader")
            # Without read access the controller cannot even be discovered.
            self._submit_spec_with_runner_until(DISCOVERY_DENIED_ERROR, direct=True, user="stranger")
