import os
import time
from contextlib import contextmanager

import pytest

import yatest.common

from yt.common import wait
from yt.wrapper.driver import make_formatted_request

from yt.yt.flow.library.python.bullied_process import ProcessDiedException
from yt.yt.flow.library.python.integration_test_base.yt_flow_base import FlowTestBase
from yt.yt.flow.library.python.integration_test_base.helpers import get_yson_config

from .yt_sync import run_yt_sync

##################################################################

PIPELINE_CONFIG_PATH = yatest.common.source_path(f"{yatest.common.context.project_path}/pipeline/pipeline.yson")

REANIMATE_BINARY = yatest.common.binary_path("yt/yt/flow/tools/reanimate_vanilla_operation/reanimate_vanilla_operation")

_TERMINAL_STATES = ("completed", "failed", "aborted")

SECRET_ENV = "YT_MY_SECRET"
SECRET_VALUE = "topsecret"

ACL_TEST_DEADLINE_SECONDS = 480
ACL_PHASE_TIMEOUT_SECONDS = 300


def _ace(action, subjects, permissions, **extra):
    return {"action": action, "subjects": subjects, "permissions": permissions, **extra}


def _normalized_acl(acl):
    return [
        (
            ace["action"],
            tuple(ace["subjects"]),
            tuple(sorted(ace["permissions"])),
            ace.get("inheritance_mode", "object_and_descendants"),
            str(ace.get("subject_tag_filter")),
        )
        for ace in acl
    ]


class _Deadline:
    def __init__(self, seconds=ACL_TEST_DEADLINE_SECONDS):
        self._end = time.monotonic() + seconds

    def timeout(self, phase_cap=ACL_PHASE_TIMEOUT_SECONDS):
        remaining = self._end - time.monotonic()
        assert remaining > 0, "The test body deadline is exceeded"
        return min(phase_cap, remaining)


##################################################################


class TestReanimateVanillaCpp(FlowTestBase):
    FLOW_BINARY_PATH = yatest.common.binary_path(f"{yatest.common.context.project_path}/pipeline/pipeline")

    @property
    def output_table(self):
        return f"{self.work_yt_path}/output"

    def prepare_pipeline_config(self, name="pipeline.yson"):
        pipeline_config = get_yson_config(PIPELINE_CONFIG_PATH)
        pipeline_config["spec"]["computations"]["sink"]["external_state_managers"]["/state"]["parameters"][
            "path"
        ] = self.output_table
        self.patch_config(pipeline_config)
        return self.dump_config_to_log_dir(pipeline_config, name)

    def _current_operation_id(self, operation_client=None):
        # The manifest records only the operation alias; a live operation resolves through the
        # scheduler runtime state of the cluster it runs on.
        operation_client = operation_client or self.client
        alias = self.client.get(f"{self.pipeline_path}/@current_vanilla_operation/alias")
        return operation_client.get_operation(operation_alias=alias, attributes=["id"], include_runtime=True)["id"]

    def _reanimate(self, timeout=None, check_exit_code=True):
        cluster_url = self.cluster_name_to_url[self.primary_cluster_name]
        # reanimate rebuilds the secure vault from the environment, so the secret declared in
        # secret_env must be present just like at the original launch.
        return yatest.common.execute(
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
            timeout=timeout,
            check_exit_code=check_exit_code,
        )

    def _total_count(self):
        rows = list(self.client.select_rows(f"sum(count) as total from [{self.output_table}] group by 1 as g"))
        return rows[0]["total"] if rows else 0

    def _any_secret(self):
        rows = list(self.client.select_rows(f"secret from [{self.output_table}] limit 1"))
        return rows[0]["secret"] if rows else None

    @pytest.mark.authors(["mikari"])
    @pytest.mark.parametrize("use_job_proxy", [False, True], ids=["rpc_proxy", "job_proxy"])
    def test_reanimate(self, use_job_proxy):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        cache_path = f"{self.work_yt_path}/vanilla_cache/files"
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            use_vanilla_jobs=True,
            vanilla_secret_env=[SECRET_ENV],
            # The runner is not kept attached: the operation is aborted below on purpose, which an
            # attached runner reports as a failed pipeline.
            additional_env={SECRET_ENV: SECRET_VALUE, "YT_FLOW_WAIT": "0"},
            vanilla_config_patch={
                "cache_path": cache_path,
                "worker": {
                    "count": 1,
                    "cpu_limit": 1,
                    "port_count": self.VANILLA_WORKER_PORT_COUNT,
                    "set_container_cpu_limit": True,
                    "enable_rpc_proxy_in_job_proxy": use_job_proxy,
                },
            },
            leader_wait_timeout=300,
        ):
            self.wait_pipeline_state("working", timeout=300)

            # The files went through the custom cache, staged in its parent: the cache holds the
            # blobs, while the staging dir holds only throwaways, all removed after being copied in.
            assert len(self.client.list(cache_path)) > 0
            assert self.client.list(f"{self.work_yt_path}/vanilla_cache") == ["files"]

            # The staged node's expiration timeout must follow the blob neither into the cache nor
            # into the durable copy reanimate depends on.
            for shard in self.client.list(cache_path):
                for blob in self.client.list(f"{cache_path}/{shard}"):
                    assert not self.client.exists(f"{cache_path}/{shard}/{blob}/@expiration_timeout")
            files_dir = f"{self.pipeline_path}/vanilla/files"
            for name in self.client.list(files_dir):
                assert not self.client.exists(f"{files_dir}/{name}/@expiration_timeout")

            # @current_vanilla_operation is only a pointer to the operation (by alias); the secret_env
            # names live in the persisted spec, not in the attribute.
            manifest = self.client.get(f"{self.pipeline_path}/@current_vanilla_operation")
            assert set(manifest.keys()) == {"cluster", "alias", "proxy_role"}
            spec = self.client.get(f"{self.pipeline_path}/vanilla/current_spec")
            assert spec["secret_env"] == [SECRET_ENV]
            assert "secure_vault" not in spec
            assert spec["tasks"]["worker"]["set_container_cpu_limit"]
            assert "set_container_cpu_limit" not in spec["tasks"]["controller"]
            assert spec["tasks"]["worker"].get("enable_rpc_proxy_in_job_proxy", False) == use_job_proxy

            operation_spec = self.client.get_operation(self._current_operation_id(), attributes=["spec"])["spec"]
            assert operation_spec["tasks"]["worker"]["set_container_cpu_limit"]
            assert "set_container_cpu_limit" not in operation_spec["tasks"]["controller"]
            assert operation_spec["tasks"]["worker"].get("enable_rpc_proxy_in_job_proxy", False) == use_job_proxy

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

            operation_spec = self.client.get_operation(self._current_operation_id(), attributes=["spec"])["spec"]
            assert operation_spec["tasks"]["worker"]["set_container_cpu_limit"]
            assert "set_container_cpu_limit" not in operation_spec["tasks"]["controller"]
            assert operation_spec["tasks"]["worker"].get("enable_rpc_proxy_in_job_proxy", False) == use_job_proxy

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
            # The runner is not kept attached: the operation is aborted below on purpose, which an
            # attached runner reports as a failed pipeline.
            additional_env={SECRET_ENV: SECRET_VALUE, "YT_FLOW_WAIT": "0"},
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

    @pytest.mark.authors(["timoninmaxim"])
    def test_runner_fails_fast_on_aborted_operation(self):
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        # The attached runner exits non-zero on its own, which the harness reports as a died process.
        with pytest.raises(ProcessDiedException):
            with self.start_flow_process_federation(
                pipeline_binary_args={"--config": config_path},
                use_vanilla_jobs=True,
                vanilla_secret_env=[SECRET_ENV],
                additional_env={SECRET_ENV: SECRET_VALUE},
            ) as federation:
                # Abort the operation while the runner still waits for the controller: it must
                # notice and stop instead of waiting out the controller-unavailable timeout.
                wait(lambda: self._current_operation_id() is not None, timeout=300, ignore_exceptions=True)
                federation.try_dump_final_state()
                self.client.abort_operation(self._current_operation_id())
                wait(lambda: False, error_message="The runner did not exit after the abort", timeout=120)

        with open(os.path.join(self.path_to_flow_logs, "Runner.err")) as runner_err:
            assert "is aborted" in runner_err.read()

    def _create_users(self, names, clients=None):
        for client in clients or [self.client]:
            for name in names:
                if not client.exists(f"//sys/users/{name}"):
                    client.create("user", attributes={"name": name})

    def _create_group(self, name, members, clients=None):
        for client in clients or [self.client]:
            if not client.exists(f"//sys/groups/{name}"):
                client.create("group", attributes={"name": name})
            for member in members:
                client.add_member(member, name)

    def _add_acl(self, client, path, entries):
        client.set(f"{path}/@acl", list(client.get(f"{path}/@acl")) + entries)

    def _isolate_work_acl(self, entries=()):
        self.client.set(f"{self.work_yt_path}/@inherit_acl", False)
        self.client.set(f"{self.work_yt_path}/@acl", list(entries))

    def _assert_operation_permission(self, client, operation_id, user, permission, expected_action):
        result = client.check_operation_permission(operation_id=operation_id, user=user, permission=permission)
        assert result["action"] == expected_action, (operation_id, user, permission, result)

    def _assert_operation_read(self, client, operation_id, allowed=(), denied=()):
        for user in allowed:
            self._assert_operation_permission(client, operation_id, user, "read", "allow")
        for user in denied:
            self._assert_operation_permission(client, operation_id, user, "read", "deny")

    def _submitted_acl(self, client, operation_id):
        return client.get_operation(operation_id, attributes=["spec"])["spec"].get("acl")

    def _persisted_spec(self):
        return self.client.get(f"{self.pipeline_path}/vanilla/current_spec")

    def _assert_spec_acl(self, operation_client, operation_id, expected_acl):
        persisted_acl = self._persisted_spec().get("acl")
        submitted_acl = self._submitted_acl(operation_client, operation_id)
        assert persisted_acl is not None and submitted_acl is not None, (persisted_acl, submitted_acl)
        assert _normalized_acl(submitted_acl) == _normalized_acl(persisted_acl)
        if expected_acl is not None:
            assert sorted(_normalized_acl(persisted_acl)) == sorted(_normalized_acl(expected_acl)), persisted_acl

    def _assert_no_persisted_secrets(self):
        spec = self._persisted_spec()
        assert "secure_vault" not in spec
        assert SECRET_VALUE not in str(spec)

    def _wait_output_advances(self, deadline, count_before=0):
        wait(lambda: self._total_count() > count_before, timeout=deadline.timeout(), ignore_exceptions=True)

    def _wait_operation_terminal(self, client, operation_id, deadline):
        wait(
            lambda: client.get_operation(operation_id, attributes=["state"])["state"] in _TERMINAL_STATES,
            timeout=deadline.timeout(),
        )

    def _wait_new_operation(self, operation_client, previous_operation_id, deadline):
        wait(
            lambda: self._current_operation_id(operation_client) != previous_operation_id,
            timeout=deadline.timeout(),
            ignore_exceptions=True,
        )
        return self._current_operation_id(operation_client)

    @contextmanager
    def _acl_federation(self, deadline, config_path, runtime_cluster=None, config_patch=None, env=None):
        with self.start_flow_process_federation(
            pipeline_binary_args={"--config": config_path},
            use_vanilla_jobs=True,
            vanilla_secret_env=[SECRET_ENV],
            additional_env={SECRET_ENV: SECRET_VALUE, "YT_FLOW_WAIT": "0", **(env or {})},
            vanilla_runtime_cluster=runtime_cluster,
            vanilla_config_patch=config_patch,
            leader_wait_timeout=deadline.timeout(),
            wait_pipeline=False,
        ) as federation:
            self.wait_pipeline_state(["working", "completed"], timeout=deadline.timeout())
            yield federation

    def _prepare_launch_config(self, name, runtime_cluster=None, config_patch=None):
        config_path = self.prepare_pipeline_config(f"{name}.yson")
        self._inject_vanilla_block({"--config": config_path}, 1, [SECRET_ENV], runtime_cluster, config_patch)
        return config_path

    def _run_runner(self, name, deadline, runtime_cluster=None, config_patch=None, env=None, check_exit_code=True):
        config_path = self._prepare_launch_config(name, runtime_cluster, config_patch)
        stdout_path = os.path.join(self.path_to_flow_logs, f"{name}.out")
        stderr_path = os.path.join(self.path_to_flow_logs, f"{name}.err")
        runner_env = {
            **os.environ,
            SECRET_ENV: SECRET_VALUE,
            "YT_FLOW_WAIT": "0",
            "YT_FLOW_BINARY_CHECKSUM_OVERRIDE": "test",
            **(env or {}),
        }
        with open(stdout_path, "w") as stdout, open(stderr_path, "w") as stderr:
            result = yatest.common.execute(
                [self.FLOW_BINARY_PATH, "--config", config_path],
                cwd=os.path.dirname(self.FLOW_BINARY_PATH),
                env=runner_env,
                stdout=stdout,
                stderr=stderr,
                timeout=deadline.timeout(),
                check_exit_code=check_exit_code,
            )
        with open(stderr_path) as stderr:
            return result.exit_code, stderr.read()

    def _create_scheduler_base_acl_member(self, name, clients=None):
        self._create_users([name], clients)
        for client in clients or [self.client]:
            client.add_member(name, "admins")

    def _provision_launcher(self, name, clients=None):
        clients = clients or [self.client]
        self._create_users([name], clients)
        self._ensure_vanilla_pool()
        pool_path = f"//sys/pool_trees/default/{self.VANILLA_POOL}"
        self._add_acl(self.client, pool_path, [_ace("allow", [name], ["use"])])
        for client in clients:
            assert "superusers" not in client.get(f"//sys/users/{name}/@member_of_closure")
            self._add_acl(client, "//sys/accounts/tmp", [_ace("allow", [name], ["use"])])
            self._add_acl(client, self.work_yt_path, [_ace("allow", [name], ["read", "write", "remove"])])
            assert client.check_permission(name, "write", self.work_yt_path)["action"] == "allow"
            assert client.check_permission(name, "use", "//sys/accounts/tmp")["action"] == "allow"
        assert self.client.check_permission(name, "use", pool_path)["action"] == "allow"

    def _check_permission_by_acl(self, client, user, permission, acl):
        return make_formatted_request(
            "check_permission_by_acl",
            params={"user": user, "permission": permission, "acl": acl},
            format=None,
            client=client,
        )

    def _assert_cache_absent_or_empty(self, client, cache_path):
        if client.exists(cache_path):
            assert list(client.list(cache_path)) == [], cache_path

    def _assert_cache_populated(self, client, cache_path):
        assert client.exists(cache_path), cache_path
        assert len(client.list(cache_path)) > 0, cache_path

    def _acl_cache_path(self, case_name, attempt):
        return f"{self.work_yt_path}/acl_cache/{case_name}/{attempt}/files"

    def _snapshot_deployment(self, operation_client):
        return {
            "operation_id": self._current_operation_id(operation_client),
            "manifest": self.client.get(f"{self.pipeline_path}/@current_vanilla_operation"),
            "current_spec": self._persisted_spec(),
            "output_count": self._total_count(),
        }

    def _assert_prior_operation_unchanged(self, snapshot, operation_client, deadline):
        assert self._current_operation_id(operation_client) == snapshot["operation_id"]
        assert self.client.get(f"{self.pipeline_path}/@current_vanilla_operation") == snapshot["manifest"]
        assert self._persisted_spec() == snapshot["current_spec"]
        state = operation_client.get_operation(snapshot["operation_id"], attributes=["state"])["state"]
        assert state == "running", state
        self._wait_output_advances(deadline, snapshot["output_count"])

    def _run_failed_replacement(
        self,
        deadline,
        name,
        operation_client,
        cache_clients,
        failed_cache_path,
        expected_diagnostic,
        runtime_cluster=None,
        config_patch=None,
        env=None,
    ):
        for client in cache_clients:
            assert not client.exists(failed_cache_path), failed_cache_path
        snapshot = self._snapshot_deployment(operation_client)

        exit_code, stderr = self._run_runner(
            name,
            deadline,
            runtime_cluster=runtime_cluster,
            config_patch={**(config_patch or {}), "cache_path": failed_cache_path},
            env=env,
            check_exit_code=False,
        )
        assert exit_code != 0, stderr[-4000:]
        assert expected_diagnostic in stderr, stderr[-4000:]

        try:
            for client in cache_clients:
                self._assert_cache_absent_or_empty(client, failed_cache_path)
        finally:
            self._assert_prior_operation_unchanged(snapshot, operation_client, deadline)
        for client in cache_clients:
            self._assert_cache_absent_or_empty(client, failed_cache_path)
        return stderr

    def _abort_and_wait(self, operation_client, operation_id, deadline):
        operation_client.abort_operation(operation_id)
        self._wait_operation_terminal(operation_client, operation_id, deadline)

    def _reanimate_successfully(self, operation_client, previous_operation_id, deadline):
        self._reanimate(timeout=deadline.timeout())
        operation_id = self._wait_new_operation(operation_client, previous_operation_id, deadline)
        self.wait_pipeline_state("working", timeout=deadline.timeout())
        return operation_id

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize("config_patch", [None, {"acl": None}], ids=["omitted", "entity"])
    def test_operation_acl_unset_keeps_legacy(self, config_patch):
        deadline = _Deadline()
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(["acl_pipeline_reader"])
        self._create_scheduler_base_acl_member("acl_admins_member")
        self._isolate_work_acl()
        self._add_acl(
            self.client,
            self.pipeline_path,
            [_ace("allow", ["owner"], ["read"]), _ace("allow", ["acl_pipeline_reader"], ["read"])],
        )
        assert self.client.check_permission("acl_pipeline_reader", "read", self.pipeline_path)["action"] == "allow"

        with self._acl_federation(deadline, config_path, config_patch=config_patch):
            operation_id = self._current_operation_id()
            assert "acl" not in self._persisted_spec()
            assert self._submitted_acl(self.client, operation_id) is None
            self._assert_operation_read(
                self.client, operation_id, allowed=["acl_admins_member"], denied=["acl_pipeline_reader"]
            )
            self._assert_no_persisted_secrets()
            self._wait_output_advances(deadline)

    @pytest.mark.authors(["blinkov"])
    def test_operation_read_acl_explicit_tag_filters(self):
        deadline = _Deadline()
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        members = {"acl_trusted": ["trusted"], "acl_untagged": [], "acl_trusted_blocked": ["trusted", "blocked"]}
        self._create_users(list(members))
        for user, tags in members.items():
            self.client.set(f"//sys/users/{user}/@tags", tags)
        self._create_group("acl_tag_readers", list(members))
        self._isolate_work_acl()
        explicit_acl = [
            _ace("allow", ["acl_tag_readers"], ["read"], subject_tag_filter="trusted"),
            _ace("deny", ["acl_tag_readers"], ["read"], subject_tag_filter="blocked"),
        ]

        with self._acl_federation(deadline, config_path, config_patch={"acl": explicit_acl}):
            operation_id = self._current_operation_id()
            self._assert_operation_read(
                self.client,
                operation_id,
                allowed=["acl_trusted"],
                denied=["acl_untagged", "acl_trusted_blocked"],
            )
            self._assert_spec_acl(self.client, operation_id, explicit_acl)
            self._wait_output_advances(deadline)

    @pytest.mark.authors(["blinkov"])
    def test_operation_read_acl_submitter_deny(self):
        deadline = _Deadline()
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(["acl_separate_reader"])
        self._isolate_work_acl()
        self._provision_launcher("acl_launcher")
        assert self.client.check_permission("acl_launcher", "read", self.pipeline_path)["action"] == "allow"
        explicit_acl = [
            _ace("deny", ["acl_launcher"], ["read"]),
            _ace("allow", ["acl_separate_reader"], ["read"]),
        ]

        with self._acl_federation(
            deadline,
            config_path,
            config_patch={"acl": explicit_acl},
            env={"YT_USER": "acl_launcher"},
        ):
            operation_id = self._current_operation_id()
            operation = self.client.get_operation(operation_id, attributes=["authenticated_user"])
            assert operation["authenticated_user"] == "acl_launcher"

            self._assert_operation_permission(self.client, operation_id, "acl_launcher", "read", "deny")
            self._assert_operation_permission(self.client, operation_id, "acl_launcher", "manage", "allow")
            self._assert_operation_read(self.client, operation_id, allowed=["acl_separate_reader"])
            self._assert_spec_acl(self.client, operation_id, explicit_acl)
            self._wait_output_advances(deadline)

    @pytest.mark.authors(["blinkov"])
    @pytest.mark.parametrize(
        "explicit_acl",
        [
            [],
            [
                _ace("allow", ["acl_operator", "acl_policy_blocked"], ["read", "manage"]),
                _ace("allow", ["acl_policy_readers"], ["read"]),
                _ace("deny", ["acl_policy_blocked"], ["read"]),
            ],
        ],
        ids=["empty", "nonempty"],
    )
    def test_operation_read_acl_explicit_policy(self, explicit_acl):
        deadline = _Deadline()
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(
            ["acl_source_reader", "acl_new_source_reader", "acl_operator", "acl_policy_reader", "acl_policy_blocked"]
        )
        self._create_group("acl_policy_readers", ["acl_policy_reader", "acl_policy_blocked"])
        self._create_scheduler_base_acl_member("acl_admins_member")
        self._isolate_work_acl()
        self._add_acl(
            self.client,
            self.pipeline_path,
            [_ace("allow", ["owner"], ["read"]), _ace("allow", ["acl_source_reader"], ["read"])],
        )

        with self._acl_federation(deadline, config_path, config_patch={"acl": explicit_acl}):
            operation_id = self._current_operation_id()
            count_before = 0
            for reanimate in (False, True):
                if reanimate:
                    self._add_acl(self.client, self.pipeline_path, [_ace("allow", ["acl_new_source_reader"], ["read"])])
                    self._abort_and_wait(self.client, operation_id, deadline)
                    count_before = self._total_count()
                    operation_id = self._reanimate_successfully(self.client, operation_id, deadline)

                if explicit_acl:
                    self._assert_operation_read(
                        self.client,
                        operation_id,
                        allowed=["acl_operator", "acl_policy_reader"],
                        denied=["acl_policy_blocked", "acl_source_reader", "acl_new_source_reader"],
                    )
                    self._assert_operation_permission(self.client, operation_id, "acl_operator", "manage", "allow")
                    self._assert_operation_permission(
                        self.client, operation_id, "acl_policy_blocked", "manage", "allow"
                    )
                    self._assert_operation_permission(self.client, operation_id, "acl_policy_reader", "manage", "deny")
                else:
                    assert self._persisted_spec()["acl"] == []
                    assert self._submitted_acl(self.client, operation_id) == []
                    self._assert_operation_read(
                        self.client,
                        operation_id,
                        allowed=["acl_admins_member"],
                        denied=["acl_source_reader", "acl_new_source_reader"],
                    )
                    self._assert_operation_permission(self.client, operation_id, "acl_admins_member", "manage", "allow")

                assert _normalized_acl(self._persisted_spec()["acl"]) == _normalized_acl(explicit_acl)
                self._assert_spec_acl(self.client, operation_id, explicit_acl)
                self._wait_output_advances(deadline, count_before)

    @pytest.mark.authors(["blinkov"])
    def test_operation_read_acl_runtime_subject_failure(self):
        deadline = _Deadline()
        runtime_cluster_name = self.remote_cluster_names[0]
        runtime_cluster_url = self.cluster_name_to_url[runtime_cluster_name]
        runtime_client = self.cluster_name_to_client[runtime_cluster_name]
        all_clients = [self.client, runtime_client]
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(["acl_runtime_reader"], all_clients)
        self._isolate_work_acl()
        initial_acl = [_ace("allow", ["acl_runtime_reader"], ["read"])]
        successful_cache_path = self._acl_cache_path("runtime_subject_failure", "successful")
        failed_cache_path = self._acl_cache_path("runtime_subject_failure", "failed")
        assert not runtime_client.exists(successful_cache_path)

        with self._acl_federation(
            deadline,
            config_path,
            runtime_cluster=runtime_cluster_url,
            config_patch={"acl": initial_acl, "cache_path": successful_cache_path},
        ):
            self._assert_cache_populated(runtime_client, successful_cache_path)
            prior_operation_id = self._current_operation_id(runtime_client)
            self._assert_operation_read(runtime_client, prior_operation_id, allowed=["acl_runtime_reader"])
            self._wait_output_advances(deadline)

            self._create_users(["acl_primary_only_reader"])
            assert not runtime_client.exists("//sys/users/acl_primary_only_reader")
            replacement_acl = [_ace("allow", ["acl_runtime_reader", "acl_primary_only_reader"], ["read"])]
            for client in all_clients:
                client.create("map_node", os.path.dirname(failed_cache_path), recursive=True, ignore_existing=True)

            with pytest.raises(Exception, match="Some subjects mentioned in ACL are missing.*acl_primary_only_reader"):
                self._check_permission_by_acl(runtime_client, "root", "read", replacement_acl)

            stderr = self._run_failed_replacement(
                deadline,
                "runtime_subject_failure_runner",
                operation_client=runtime_client,
                cache_clients=[runtime_client, self.client],
                failed_cache_path=failed_cache_path,
                expected_diagnostic="Cannot use vanilla operation ACL on runtime cluster",
                runtime_cluster=runtime_cluster_url,
                config_patch={"acl": replacement_acl},
            )
            assert "acl_primary_only_reader" in stderr
            assert "do not exist there" in stderr

    @pytest.mark.authors(["blinkov"])
    def test_operation_read_acl_snapshot_refresh(self):
        deadline = _Deadline()
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(["acl_reader_a", "acl_reader_b"])
        self._isolate_work_acl()
        acl_a = [_ace("allow", ["acl_reader_a"], ["read"])]
        acl_b = [_ace("allow", ["acl_reader_b"], ["read"])]

        with self._acl_federation(deadline, config_path, config_patch={"acl": acl_a}):
            first_operation_id = self._current_operation_id()
            self._assert_operation_read(
                self.client, first_operation_id, allowed=["acl_reader_a"], denied=["acl_reader_b"]
            )
            self._wait_output_advances(deadline)

            self._abort_and_wait(self.client, first_operation_id, deadline)
            reanimated_operation_id = self._reanimate_successfully(self.client, first_operation_id, deadline)
            self._assert_operation_read(
                self.client, reanimated_operation_id, allowed=["acl_reader_a"], denied=["acl_reader_b"]
            )
            self._assert_spec_acl(self.client, reanimated_operation_id, acl_a)

            self._run_runner("refresh_runner", deadline, config_patch={"acl": acl_b})
            relaunched_operation_id = self._wait_new_operation(self.client, reanimated_operation_id, deadline)
            self._assert_operation_read(
                self.client, relaunched_operation_id, allowed=["acl_reader_b"], denied=["acl_reader_a"]
            )
            self._assert_spec_acl(self.client, relaunched_operation_id, acl_b)
            self.wait_pipeline_state("working", timeout=deadline.timeout())
            self._wait_output_advances(deadline, self._total_count())

    @pytest.mark.authors(["blinkov"])
    def test_operation_read_acl_legacy_reanimate(self):
        deadline = _Deadline()
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(["acl_reader_a", "acl_reader_b"])
        self._create_scheduler_base_acl_member("acl_admins_member")
        self._isolate_work_acl()

        with self._acl_federation(
            deadline, config_path, config_patch={"acl": [_ace("allow", ["acl_reader_a"], ["read"])]}
        ):
            first_operation_id = self._current_operation_id()
            self._assert_operation_read(self.client, first_operation_id, allowed=["acl_reader_a"])
            self._wait_output_advances(deadline)

            current_spec_path = f"{self.pipeline_path}/vanilla/current_spec"
            legacy_spec = self.client.get(current_spec_path)
            del legacy_spec["acl"]
            self.client.set(current_spec_path, legacy_spec)
            self._add_acl(self.client, self.pipeline_path, [_ace("allow", ["acl_reader_b"], ["read"])])

            self._abort_and_wait(self.client, first_operation_id, deadline)
            count_before = self._total_count()
            operation_id = self._reanimate_successfully(self.client, first_operation_id, deadline)

            assert "acl" not in self._persisted_spec()
            assert self._submitted_acl(self.client, operation_id) is None
            self._assert_operation_read(
                self.client,
                operation_id,
                allowed=["acl_admins_member"],
                denied=["acl_reader_a", "acl_reader_b"],
            )
            self._wait_output_advances(deadline, count_before)

    @pytest.mark.authors(["blinkov"])
    def test_operation_read_acl_cross_cluster_repeat(self):
        deadline = _Deadline()
        runtime_cluster_name = self.remote_cluster_names[0]
        runtime_cluster_url = self.cluster_name_to_url[runtime_cluster_name]
        runtime_client = self.cluster_name_to_client[runtime_cluster_name]
        all_clients = [self.client, runtime_client]
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(["acl_primary_member", "acl_runtime_member"], all_clients)
        self._create_group("acl_cross_readers", ["acl_primary_member"], [self.client])
        self._create_group("acl_cross_readers", ["acl_runtime_member"], [runtime_client])
        self._isolate_work_acl()
        pipeline_acl_before = self.client.get(f"{self.pipeline_path}/@acl")
        explicit_acl = [_ace("allow", ["acl_cross_readers"], ["read"])]

        with self._acl_federation(
            deadline, config_path, runtime_cluster=runtime_cluster_url, config_patch={"acl": explicit_acl}
        ):
            first_operation_id = self._current_operation_id(runtime_client)
            self._assert_operation_read(
                runtime_client, first_operation_id, allowed=["acl_runtime_member"], denied=["acl_primary_member"]
            )
            self._assert_spec_acl(runtime_client, first_operation_id, explicit_acl)
            self._wait_output_advances(deadline)

            self._run_runner(
                "repeat_runner", deadline, runtime_cluster=runtime_cluster_url, config_patch={"acl": explicit_acl}
            )
            operation_id = self._wait_new_operation(runtime_client, first_operation_id, deadline)
            self._assert_operation_read(
                runtime_client, operation_id, allowed=["acl_runtime_member"], denied=["acl_primary_member"]
            )
            self._assert_spec_acl(runtime_client, operation_id, explicit_acl)
            assert self.client.get(f"{self.pipeline_path}/@acl") == pipeline_acl_before
            self.wait_pipeline_state("working", timeout=deadline.timeout())
            self._wait_output_advances(deadline, self._total_count())

    @pytest.mark.authors(["blinkov"])
    def test_operation_read_acl_reanimate_missing_runtime_subject(self):
        deadline = _Deadline()
        runtime_cluster_name = self.remote_cluster_names[0]
        runtime_cluster_url = self.cluster_name_to_url[runtime_cluster_name]
        runtime_client = self.cluster_name_to_client[runtime_cluster_name]
        all_clients = [self.client, runtime_client]
        run_yt_sync(self.primary_cluster_name, self.work_yt_path)
        config_path = self.prepare_pipeline_config()

        self._create_users(["acl_vanishing_reader", "acl_control_reader"], all_clients)
        self._isolate_work_acl()
        explicit_acl = [
            _ace("allow", ["acl_vanishing_reader"], ["read"]),
            _ace("allow", ["acl_control_reader"], ["read"]),
        ]

        with self._acl_federation(
            deadline, config_path, runtime_cluster=runtime_cluster_url, config_patch={"acl": explicit_acl}
        ):
            first_operation_id = self._current_operation_id(runtime_client)
            self._assert_spec_acl(runtime_client, first_operation_id, explicit_acl)
            self._assert_operation_read(
                runtime_client, first_operation_id, allowed=["acl_vanishing_reader", "acl_control_reader"]
            )
            self._wait_output_advances(deadline)

            self._abort_and_wait(runtime_client, first_operation_id, deadline)
            runtime_client.remove("//sys/users/acl_vanishing_reader")
            wait(lambda: not runtime_client.exists("//sys/users/acl_vanishing_reader"), timeout=deadline.timeout(60))
            assert self.client.exists("//sys/users/acl_vanishing_reader")
            manifest_before = self.client.get(f"{self.pipeline_path}/@current_vanilla_operation")
            saved_spec_before = self._persisted_spec()
            count_before = self._total_count()

            result = self._reanimate(timeout=deadline.timeout(), check_exit_code=False)
            assert result.exit_code == 0, result.std_err.decode(errors="replace")[-4000:]
            assert self.client.get(f"{self.pipeline_path}/@current_vanilla_operation") == manifest_before
            assert self._persisted_spec() == saved_spec_before
            operation_id = self._wait_new_operation(runtime_client, first_operation_id, deadline)
            self.wait_pipeline_state("working", timeout=deadline.timeout())
            self._assert_spec_acl(runtime_client, operation_id, explicit_acl)
            self._assert_operation_read(runtime_client, operation_id, allowed=["acl_control_reader"])
            self._wait_output_advances(deadline, count_before)
            assert self._any_secret() == SECRET_VALUE

            self._create_users(["acl_vanishing_reader"], [runtime_client])
            self._assert_operation_read(
                runtime_client, operation_id, allowed=["acl_vanishing_reader", "acl_control_reader"]
            )
            self._assert_spec_acl(runtime_client, operation_id, explicit_acl)
