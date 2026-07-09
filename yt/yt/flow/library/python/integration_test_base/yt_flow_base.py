import getpass
import logging
import os
import socket
import time

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import yatest
from library.python.port_manager import PortManager
import yt.yt.flow.library.python.integration_test_base.flow_process as flow_process
from yt.yt.flow.library.python.bullied_process import ProblemsConfig

import yt.wrapper as yt_wrapper

from yt.recipe.basic.lib import get_yt_clusters
from yt.wrapper import yson
from yt.wrapper.constants import UI_ADDRESS_PATTERN
from yt.wrapper.flow_commands import get_controller_logs
from yt.common import wait, WaitFailed

from . import default_config_parameters
from .helpers import dump_yson_config, get_yson_config

log = logging.getLogger("flow_process")


##################################################################


# --test-param EXTERNAL_YT_CONFIG=<yson>
#
# When set, FlowTestBase routes pipeline runs to external YT clusters instead
# of the local recipe. The local recipe still runs (its overhead is acceptable
# for performance evaluation runs) but its YT is ignored.
#
# Format (see TExternalYtConfig / TExternalYtCluster below for the schema).
# The first entry in `clusters` is the primary cluster.
#
# Example:
#   --test-param 'EXTERNAL_YT_CONFIG={
#       path="//tmp/yt_flow/pechatnov";
#       tablet_cell_bundle=default;
#       proxy_role=default;
#       clusters=[{cluster_name=hahn}; {cluster_name=arnold};];
#   }'
@dataclass
class TExternalYtCluster:
    cluster_name: str
    proxy_url: str = ""  # empty ⇒ use cluster_name

    def __post_init__(self):
        if not self.proxy_url:
            self.proxy_url = self.cluster_name


@dataclass
class TExternalYtConfig:
    path: str
    tablet_cell_bundle: str
    proxy_role: str
    clusters: list  # parsed to list[TExternalYtCluster] in __post_init__
    primary_medium: str = "default"

    def __post_init__(self):
        self.clusters = [TExternalYtCluster(**c) for c in self.clusters]


def _load_external_yt_config():
    raw = yatest.common.get_param("EXTERNAL_YT_CONFIG", None)
    if raw is None:
        return None
    return TExternalYtConfig(**yson.loads(raw.encode("utf-8") if isinstance(raw, str) else raw))


##################################################################


def _derive_test_name(cls, method) -> str:
    test_name = f"{cls.__class__.__name__}::{method.__name__}"

    # If test is parametrized, `method.__name__` will return the same result for all parameters.
    # pytest defines environment variable with current test session state, which contains full parametrized test name.
    # PYTEST_CURRENT_TEST format: test_filename.py::TestClassName::test_method_name[parameter_id] (session state)
    # NB: This might break in future pytest releases.
    # https://docs.pytest.org/en/7.1.x/example/simple.html#pytest-current-test-environment-variable
    env_current_test = os.environ.get("PYTEST_CURRENT_TEST")
    if env_current_test:
        assert (
            test_name in env_current_test
        ), f"Test name from env does not contain expected name. Env: {env_current_test}, expected: {test_name}"
        test_name += env_current_test.split(test_name, 1)[1].split()[0]

    # YPath special symbols (https://ytsaurus.tech/docs/en/user-guide/storage/ypath#simple_ypath_lexis) + some extra symbols.
    special_symbols = ["{", "}", "[", "]", "(", ")", "/", "@", "&", "*", ":"]

    test_name = test_name.translate(str.maketrans({special_symbol: "_" for special_symbol in special_symbols}))
    test_name = "_".join(part for part in test_name.split("_") if part)
    return test_name


def _prepare_proxy_role(role: str, client) -> None:
    client.create("rpc_proxy_role_map", "//sys/rpc_proxy_roles", ignore_existing=True)
    if not client.exists(f"//sys/rpc_proxy_roles/{role}"):
        client.create(
            "proxy_role",
            attributes={"name": role, "proxy_kind": "rpc"},
        )
    for proxy in client.list("//sys/rpc_proxies"):
        client.set(f"//sys/rpc_proxies/{proxy}/@role", role)


_PROXY_KINDS = (
    ("rpc_proxies", "yt-rpc-proxy"),
    ("http_proxies", "yt-http-proxy"),
)


def _enable_proxy_signatures_for_cluster(client, cluster_name: str) -> None:
    """Configure RPC/HTTP proxies of one cluster to sign/validate Flow controller RPCs.

    Sets `flow_proxy_signature_enabled` on three places that drive proxy cluster
    connection config (`//sys/@cluster_connection`, `//sys/clusters/<name>` for
    HTTP proxy's cluster-directory sync, and `//sys/{rpc,http}_proxies/@config`
    for live reconfiguration), and enables signature_components generation +
    validation under those proxies' dynamic config. The flag in static recipe
    config `global_cluster_connection_config={flow_proxy_signature_enabled=%true}`
    primes the same value for processes that are already running.
    """
    cluster_conn = client.get("//sys/@cluster_connection")
    cluster_conn["flow_proxy_signature_enabled"] = True
    client.set("//sys/@cluster_connection", cluster_conn)

    cluster_entry_path = f"//sys/clusters/{cluster_name}"
    cluster_entry = client.get(cluster_entry_path) if client.exists(cluster_entry_path) else dict(cluster_conn)
    cluster_entry["flow_proxy_signature_enabled"] = True
    client.set(cluster_entry_path, cluster_entry)

    for kind, owner_id in _PROXY_KINDS:
        if not client.exists(f"//sys/{kind}"):
            continue
        client.set(
            f"//sys/{kind}/@config",
            {
                "cluster_connection": dict(cluster_conn),
                "signature_components": {
                    "validation": {
                        "cypress_key_reader": {},
                        "validator": {},
                    },
                    "generation": {
                        "cypress_key_writer": {"owner_id": owner_id},
                        "key_rotator": {"key_rotation_options": {"key_expiration_delta": "10m"}},
                        "generator": {},
                    },
                },
            },
        )

    # Wait for proxies to apply the new dynamic config and publish a key.
    wait(
        lambda: client.exists("//sys/public_keys/by_owner") and len(client.list("//sys/public_keys/by_owner")) > 0,
        timeout=60,
    )

    # Wait until proxies actually observe `flow_proxy_signature_enabled=true`
    # via their cluster-directory sync; otherwise initial Flow controller
    # requests would race through unsigned channels.
    def proxy_has_flag(kind: str, proxy: str) -> bool:
        path = f"//sys/{kind}/{proxy}/orchid/cluster_connection/dynamic_config"
        return bool(client.get(path).get("flow_proxy_signature_enabled"))

    wait(
        lambda: all(
            proxy_has_flag(kind, proxy)
            for kind, _ in _PROXY_KINDS
            if client.exists(f"//sys/{kind}")
            for proxy in client.list(f"//sys/{kind}")
        ),
        timeout=60,
        ignore_exceptions=True,
    )


##################################################################


# Use --test-param FLOW_BINARY_PATH=<absolute_path_to_binary> to override FLOW_BINARY_PATH (for test custom build).
class FlowTestBase:
    RPC_PROXY_ROLE = "test"
    VANILLA_POOL = "test"
    # YT-allocated ports a vanilla worker requests: rpc + monitoring. Companion-based pipelines
    # (e.g. python) override this to add a companion port.
    VANILLA_WORKER_PORT_COUNT = 2

    # Set to True in a subclass to enable proxy-signature based auth between
    # YT proxies and the Flow controller (sets up signature_components on
    # //sys/{rpc,http}_proxies and toggles flow_proxy_signature_enabled).
    ENABLE_PROXY_SIGNATURES: bool = False

    FLOW_BINARY_PATH: str

    @classmethod
    def setup_class(cls):
        cls.external_yt_config = _load_external_yt_config()

        if cls.external_yt_config is None:
            clusters = get_yt_clusters()

            cls.primary_cluster_name = clusters.primary_cluster.yt_id
            cls.remote_cluster_names = [c.yt_id for c in clusters.replica_clusters]
            cls.cluster_name_to_client = {c.yt_id: c.get_yt_client() for c in clusters}
            cls.cluster_name_to_url = {c.yt_id: c.get_proxy_address() for c in clusters}
            cls.base_work_yt_path = "//tmp/test"
            cls.tablet_cell_bundle = "default"
            cls.primary_medium = "default"
        else:
            cfg = cls.external_yt_config
            cls.primary_cluster_name = cfg.clusters[0].cluster_name
            cls.remote_cluster_names = [c.cluster_name for c in cfg.clusters[1:]]
            cls.cluster_name_to_client = {c.cluster_name: yt_wrapper.YtClient(proxy=c.proxy_url) for c in cfg.clusters}
            cls.cluster_name_to_url = {c.cluster_name: c.proxy_url for c in cfg.clusters}
            # Append the developer's local username so concurrent runs by different
            # users don't stomp on each other.
            cls.base_work_yt_path = f"{cfg.path}/{getpass.getuser()}"
            cls.tablet_cell_bundle = cfg.tablet_cell_bundle
            cls.primary_medium = cfg.primary_medium
            cls.RPC_PROXY_ROLE = cfg.proxy_role

        cls.client = cls.cluster_name_to_client[cls.primary_cluster_name]

        cls.path_to_run = yatest.common.output_path()

        cls.port_manager = PortManager()
        cls.pm = cls.port_manager  # TODO(pechatnov): Remove this field.

        url_aliasing_config = yson.loads(os.environ.get("YT_PROXY_URL_ALIASING_CONFIG", "{}").encode("utf-8"))
        url_aliasing_config.update(cls.cluster_name_to_url)
        cls.serialized_url_aliasing_config = str(yson.dumps(url_aliasing_config).decode("utf-8"))

        # On a real external cluster we don't have permissions to redefine //sys/rpc_proxy_roles;
        # the role must be configured by the cluster owner instead.
        if cls.external_yt_config is None:
            for client in cls.cluster_name_to_client.values():
                _prepare_proxy_role(cls.RPC_PROXY_ROLE, client)

        cls.rpc_driver_config = {"proxy_role": cls.RPC_PROXY_ROLE}

        cls.rpc_driver_config_path = os.path.join(cls.path_to_run, "rpc_driver_config.yson")
        with open(cls.rpc_driver_config_path, "wb") as f:
            yson.dump(cls.rpc_driver_config, f)

        # On external clusters state persists across runs; wipe the developer's
        # subdirectory once per test class so every run starts from a clean slate.
        if cls.external_yt_config is not None:
            for client in cls.cluster_name_to_client.values():
                client.remove(cls.base_work_yt_path, recursive=True, force=True)
                client.create("map_node", cls.base_work_yt_path, recursive=True)

        cls.log_test_info()

        # Standard environ variables with current local yt setup.

        if cls.external_yt_config is None:
            # Hide the environ variables of the user launching tests.
            os.environ["YT_USER"] = "root"
            # Tokens from env are replaced in output logs, so use some random string.
            os.environ["YT_TOKEN"] = "test986104827492"
        else:
            # The local YT recipe stubs YT_TOKEN/YT_USER for its own cluster; clear them so
            # yt-wrapper falls back to the developer's credentials (~/.yt/token).
            os.environ.pop("YT_USER", None)
            os.environ.pop("YT_TOKEN", None)

        os.environ["YT_DRIVER_CONFIG_PATH"] = cls.rpc_driver_config_path
        os.environ["YT_PROXY_URL_ALIASING_CONFIG"] = cls.serialized_url_aliasing_config
        os.environ["YT_RPC_PROXY_ROLE"] = cls.RPC_PROXY_ROLE

        if cls.ENABLE_PROXY_SIGNATURES:
            for cluster_name, client in cls.cluster_name_to_client.items():
                _enable_proxy_signatures_for_cluster(client, cluster_name)

    @classmethod
    def teardown_class(cls):
        cls.port_manager.release()

    @classmethod
    def log_test_info(cls):
        message = "YT Flow test started:\n"
        message += f"    Work path on disk (logs, configs, etc): {cls.path_to_run}\n"

        if cls.external_yt_config is None:
            hostname = socket.gethostname()
            for name, url in cls.cluster_name_to_url.items():
                http_proxy_port = url.rsplit(":", 1)[1]
                if UI_ADDRESS_PATTERN:
                    ui_address = UI_ADDRESS_PATTERN.format(cluster_name=f"{hostname}:{http_proxy_port}")
                    message += (
                        f"    Local YT url for cluster '{name}': "
                        f"{ui_address}navigation?path={cls.base_work_yt_path}"
                        "\n"
                    )
        else:
            for name, url in cls.cluster_name_to_url.items():
                if UI_ADDRESS_PATTERN:
                    ui_address = UI_ADDRESS_PATTERN.format(cluster_name=name)
                    message += (
                        f"    External YT url for cluster '{name}': "
                        f"{ui_address}navigation?path={cls.base_work_yt_path}"
                        f" (proxy: {url})\n"
                    )
        message += (
            "    About test framework: yt/yt/flow/library/python/integration_test_base/README.md\n"
        )
        logging.info("%s", message)
        cls.try_print_tty(message)  # Ignore if printing failed.

    @classmethod
    def try_print_tty(cls, text):
        file_name = "CONOUT$" if os.name == "nt" else "/dev/tty"
        if not os.path.exists(file_name):
            return False
        try:
            with open(file_name, "w") as term:
                term.write(f"\n\n{text}\n")
            return True
        except Exception:
            logging.exception("Failed to print to tty")
            return False

    def setup_method(self, method):
        test_name = _derive_test_name(self, method)

        self.path_to_flow_logs = os.path.join(self.path_to_run, test_name)
        os.makedirs(self.path_to_flow_logs)

        self.test_name = test_name
        self.work_yt_path = f"{self.base_work_yt_path}/{self.test_name}"
        self.pipeline_path = f"{self.work_yt_path}/pipeline"
        self.pipeline_view_path = f"{self.pipeline_path}/flow_view"

        for client in self.cluster_name_to_client.values():
            client.create("map_node", self.work_yt_path, recursive=True)

    def teardown_method(self, method):
        pass

    def patch_config(self, config: dict):
        base_patch = {
            "cluster_url": self.primary_cluster_name,
            "path": self.pipeline_path,
            "proxy_role": self.RPC_PROXY_ROLE,
        }
        config.update(base_patch)

        # The crutch method to distinguish between node and pipeline configs.
        if "spec" in config:
            default_config_parameters.fill_runner_test_defaults(config)
        else:
            default_config_parameters.fill_flow_node_test_defaults(config)

    def dump_config_to_log_dir(self, config: dict, name: str):
        prepared_path = os.path.join(self.path_to_flow_logs, name)
        dump_yson_config(config, prepared_path)
        return prepared_path

    def _ensure_vanilla_pool(self):
        """The local test YT has no pools out of the box; create the one vanilla ops run in."""
        pool_path = f"//sys/pool_trees/default/{self.VANILLA_POOL}"
        if not self.client.exists(pool_path):
            self.client.create(
                "scheduler_pool",
                attributes={"name": self.VANILLA_POOL, "pool_tree": "default"},
            )

    def _inject_vanilla_block(
        self,
        pipeline_binary_args: dict,
        workers_count: int,
        secret_env: list[str] | None = None,
        runtime_cluster: str | None = None,
    ):
        """Patches the pipeline-config YSON in-place to enable a vanilla-jobs launch."""
        config_path = pipeline_binary_args.get("--config")
        assert config_path is not None, "use_vanilla_jobs requires --config in pipeline_binary_args"

        self._ensure_vanilla_pool()

        node_config = {}
        self.patch_config(node_config)

        pipeline_config = get_yson_config(config_path)
        pipeline_config["vanilla"] = {
            "enable": True,
            "pool": self.VANILLA_POOL,
            # Co-located jobs on the test host share the network, so fixed ports would
            # collide — request YT-allocated ports for both tasks.
            # Local YT in tests has limited CPU; pin cpu_limit on both tasks so they fit
            # (the production defaults are larger and would not be schedulable here).
            "controller": {"count": 1, "cpu_limit": 1, "port_count": 2},
            "worker": {"count": workers_count, "cpu_limit": 1, "port_count": self.VANILLA_WORKER_PORT_COUNT},
            "node_config": node_config,
        }
        if secret_env:
            pipeline_config["vanilla"]["secret_env"] = secret_env
        if runtime_cluster:
            pipeline_config["vanilla"]["runtime_cluster"] = runtime_cluster
            # All clusters of the test federation expose the same proxy role.
            pipeline_config["vanilla"]["runtime_proxy_role"] = self.RPC_PROXY_ROLE
        dump_yson_config(pipeline_config, config_path)

    @contextmanager
    def start_flow_process_federation(
        self,
        *,
        node_config: dict = None,
        pipeline_binary_args: dict[str, str] = {},
        workers_count: int = 1,
        controllers_count: int = 1,
        binary_path: str | None = None,
        runner_binary_path: str | None = None,
        start_watcher_thread: bool = True,
        problems: bool = False,
        controller_problems_config: Optional[ProblemsConfig] = None,
        worker_problems_config: Optional[ProblemsConfig] = None,
        companion_problems_config: Optional[ProblemsConfig] = None,
        run_pipeline: bool = True,
        use_vanilla_jobs: bool = False,
        vanilla_secret_env: list[str] | None = None,
        vanilla_runtime_cluster: str | None = None,
        patch_node_config: bool = True,
        additional_env: dict[str, str] | None = None,
        run_companion_externally: bool = False,
        companion_binary_path: Optional[str] = None,
        companion_binary_args: Optional[list[str]] = None,
        leader_wait_timeout: Optional[int] = None,
    ):
        if node_config is None:
            node_config = {}
        if binary_path is None:
            binary_path = self.FLOW_BINARY_PATH
        binary_path = yatest.common.get_param("FLOW_BINARY_PATH", binary_path)

        if runner_binary_path is None:
            runner_binary_path = binary_path

        if patch_node_config:
            self.patch_config(node_config)

        if leader_wait_timeout is None:
            # A vanilla controller pays the operation-startup latency (binary upload + cold
            # flow_server boot, doubly slow under sanitizers) before it elects a leader; a
            # host-launched controller comes up immediately. Mirror the java base's allowance.
            leader_wait_timeout = 120 if use_vanilla_jobs else 30

        if use_vanilla_jobs:
            self._inject_vanilla_block(pipeline_binary_args, workers_count, vanilla_secret_env, vanilla_runtime_cluster)

        if problems:
            if controller_problems_config is None:
                controller_problems_config = ProblemsConfig(interval_seconds=60, problems_max_count=1)
            if worker_problems_config is None:
                worker_problems_config = ProblemsConfig(interval_seconds=60, problems_max_count=2)
            if companion_problems_config is None:
                companion_problems_config = ProblemsConfig(interval_seconds=60, problems_max_count=2)

        env = dict(os.environ)
        if additional_env is not None:
            env.update(additional_env)
        debug_hang = False
        with flow_process.FlowSimpleProcessFederation(
            binary_path=binary_path,
            runner_binary_path=runner_binary_path,
            node_config=node_config,
            pipeline_binary_args=pipeline_binary_args,
            logs_dir=self.path_to_flow_logs,
            port_manager=self.port_manager,
            env=env,
            workers_count=workers_count,
            controllers_count=controllers_count,
            start_watcher_thread=start_watcher_thread,
            controller_problems_config=controller_problems_config,
            worker_problems_config=worker_problems_config,
            companion_problems_config=companion_problems_config,
            run_pipeline=run_pipeline,
            use_vanilla_jobs=use_vanilla_jobs,
            run_companion_externally=run_companion_externally,
            companion_binary_path=companion_binary_path,
            companion_binary_args=companion_binary_args,
            companion_cluster_url=(
                self.cluster_name_to_url[self.primary_cluster_name] if run_companion_externally else None
            ),
            companion_pipeline_path=self.pipeline_path if run_companion_externally else None,
            client=self.client,
        ) as federation:
            try:
                if controllers_count > 0:
                    wait(
                        lambda: self.client.exists(f"{self.pipeline_path}/@leader_controller_address"),
                        timeout=leader_wait_timeout,
                    )
                if run_pipeline:
                    self.wait_pipeline_state(["working", "completed"])
                else:
                    wait(
                        lambda: self.client.get_pipeline_state(self.pipeline_path) != "",
                        timeout=180,
                        ignore_exceptions=True,
                    )
                yield federation
            except WaitFailed:
                debug_hang = True
                raise
            finally:
                self._try_dump_flow_view()
                self._try_dump_description()
                federation.try_dump_processes_state(debug_hang=debug_hang)
                while int(yatest.common.get_param("PAUSE_BEFORE_FLOW_PROCESS_FEDERATION_TEARDOWN", 0)):
                    time.sleep(1)

    def _try_dump_flow_view(self):
        try:
            view = self.client.get_flow_view(self.pipeline_path, cache=False)
            with open(os.path.join(self.path_to_flow_logs, "final_flow_view.yson"), "wb") as f:
                yson.dump(view, f, sort_keys=True, yson_format="pretty")
        except Exception as e:
            log.error("Can't dump flow view", exc_info=e)

    def _try_dump_description(self):
        try:
            descr = self.client.flow_execute(self.pipeline_path, flow_command="describe-pipeline")
            with open(os.path.join(self.path_to_flow_logs, "final_description.yson"), "wb") as f:
                yson.dump(descr, f, sort_keys=True, yson_format="pretty")
        except Exception as e:
            log.error("Can't dump flow description", exc_info=e)

    def _get_recent_controller_messages(self, count=5, scan_count=1000):
        """Return the last `count` error-level and last `count` warning-level lines
        from the controller public log (errors first)."""
        try:
            rows, _ = get_controller_logs(self.pipeline_path, count=scan_count, client=self.client)
        except Exception as e:
            log.warning("Can't read controller logs for timeout error enrichment", exc_info=e)
            return []

        errors = []
        warnings = []
        for row in rows:
            data = row.get("data", "")
            # Log line format: "<datetime>\t<level>\t<category>\t<message>...".
            fields = data.split("\t")
            if len(fields) < 2:
                continue
            if fields[1] == "E":
                errors.append(data)
            elif fields[1] == "W":
                warnings.append(data)
        return errors[-count:] + warnings[-count:]

    def wait_pipeline_state(self, state, timeout=180):
        """Wait until the pipeline reaches the given state (or one of the given states).

        Tolerates transient errors (e.g. "Cannot connect to pipeline controller leader")
        that occur while the controller is starting up or restarting.

        On timeout the raised error is enriched with the last controller error and warning
        log lines, so a stuck pipeline (e.g. a repeatedly failing job) shows its cause directly.

        Args:
            state: a string (e.g. "completed") or a list of strings (e.g. ["working", "completed"]).
            timeout: maximum wait time in seconds (default 180).
        """
        if isinstance(state, str):
            state = [state]
        try:
            wait(
                lambda: self.client.get_pipeline_state(self.pipeline_path) in state,
                timeout=timeout,
                ignore_exceptions=True,
            )
        except WaitFailed:
            messages = self._get_recent_controller_messages()
            if not messages:
                raise
            raise WaitFailed(
                "Pipeline did not reach state {} in {}s. Recent controller errors and warnings:\n{}".format(
                    state, timeout, "\n".join(messages)
                )
            )

    def wait_for_pipeline_error(self, error_substring, timeout=60):
        """Wait until an error containing the given substring appears in the flow view."""

        # TODO: Search only in feedback/retryable_errors instead of the entire flow view.
        def check():
            view = self.client.get_flow_view(self.pipeline_path, cache=False)
            return error_substring in str(view)

        wait(check, timeout=timeout, ignore_exceptions=True)

    def get_processing_watermark(self):
        epoch = self.client.get_flow_view(self.pipeline_path, view_path="/state/epoch")
        united_stream = self.client.get_flow_view(self.pipeline_path, view_path="/state/traverse_data/united_stream")
        if epoch == united_stream.get("epoch", -1):
            return united_stream.get("event_watermark", 0)
        return 0

    def run_yt_sync_ensure(self, yt_sync_binary_path, extra_env=None):
        env = dict(os.environ)
        env.update(
            {
                # Custom environ variables with current local yt pipeline setup.
                "TEST_YT_CLUSTER": self.primary_cluster_name,
                "TEST_YT_REMOTE_CLUSTERS": yson.dumps(self.remote_cluster_names).decode("utf-8"),
                "TEST_YT_PATH": self.work_yt_path,
                "TEST_YT_PRIMARY_MEDIUM": self.primary_medium,
                "TEST_YT_TABLET_CELL_BUNDLE": self.tablet_cell_bundle,
            }
        )
        if extra_env is not None:
            env.update(extra_env)

        yatest.common.execute(
            [yt_sync_binary_path, "--stage", "test", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
            env=env,
            wait=True,
            stdout=os.path.join(self.path_to_flow_logs, "yt_sync.out"),
            stderr=os.path.join(self.path_to_flow_logs, "yt_sync.err"),
        )
