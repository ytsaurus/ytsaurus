from yt.common import wait, YtError, makedirp

from yt.environment.configs_provider import _init_logging
from yt.environment import YTServerComponentBase, YTComponent

import logging
import os

logger = logging.getLogger("YtLocal")


class YqlAgent(YTServerComponentBase, YTComponent):
    LOWERCASE_NAME = "yql_agent"
    DASHED_NAME = "yql-agent"
    PLURAL_HUMAN_READABLE_NAME = "yql agents"

    USER_NAME = "yql_agent"

    def __init__(self):
        super(YqlAgent, self).__init__()
        self.client = None
        self.artifacts_path = None
        self.subprocess_count = None
        self.env = None
        self.remote_envs = []
        self.enable_dq = False
        self._qtworker_enabled = False
        self._qtworker_processes = []
        self._qtworker_binary = None
        self._qtworker_worker_conf = None
        self._qtworker_fs_conf = None
        self._qtworker_gateways_conf = None
        self._qtworker_socket_dir = None
        self._qtworker_run_dir = None
        self._qtworker_tasks_dir = None
        self._qtworker_filecache_dir = None

    def prepare(self, env, config, remote_envs=[]):
        logger.info("Preparing yql agent")

        if "path" in config:
            self.PATH = config["path"]

        self.libraries = config.get("libraries", {})
        self.config = config
        self.env = env
        self.remote_envs = remote_envs
        self.enable_dq = config.get("enable_dq", False)

        if "artifacts_path" in config:
            self.artifacts_path = config["artifacts_path"]
        elif "mr_job_bin" not in config \
                or "mr_job_udfs_dir" not in config \
                or "yql_plugin_shared_library" not in config:
            raise YtError("Artifacts path is not specified in yql agent config")

        if "subprocess_count" in config and config["subprocess_count"] != 0:
            self.subprocess_count = config["subprocess_count"]

        self.max_supported_yql_version = config.get("max_supported_yql_version")
        self.default_yql_ui_version = config.get("default_yql_ui_version")
        self.allow_not_released_yql_versions = config.get("allow_not_released_yql_versions")

        super(YqlAgent, self).prepare(env, config)

        if config.get("native_client_supported", False):
            self.client = env.create_native_client()
        else:
            self.client = env.create_client()

        self.client.create("user", attributes={"name": self.USER_NAME})
        self.client.add_member(self.USER_NAME, "superusers")

        self.client.create("document", "//sys/yql_agent/config", recursive=True, force=True, attributes={"value": {}})

        yql_agent_token = self.client.issue_token(self.USER_NAME)
        with open(self.token_path, "w") as file:
            file.write(yql_agent_token)

        if self.config.get("enable_qtworker", False):
            self._prepare_qtworker()

        logger.info("Yql agent prepared")

    def _prepare_qtworker_fs_conf(self):
        fs_src = self.config.get("qtworker_fs_conf")
        if not fs_src:
            raise YtError("qtworker_fs_conf is not specified in config")

        with open(fs_src) as fs_file:
            fs_text = fs_file.read()
        fs_text = fs_text.replace("${filecache}", os.path.join(self._instance_root, "filecache"))

        self._qtworker_fs_conf = os.path.join(self.env.configs_path, "qtworker_fs.conf")
        with open(self._qtworker_fs_conf, "w") as fs_out:
            fs_out.write(fs_text)

    def _prepare_qtworker_worker_conf(self):
        self._qtworker_run_dir = os.path.join(self._instance_root, "run")
        self._qtworker_tasks_dir = os.path.join(self._instance_root, "qtworker-tasks")
        self._qtworker_filecache_dir = os.path.join(self._instance_root, "filecache")
        self._qtworker_socket_dir = os.path.join(self._instance_root, "qt_socket_dir")

        makedirp(self._qtworker_run_dir)
        makedirp(self._qtworker_tasks_dir)
        makedirp(self._qtworker_filecache_dir)
        makedirp(self._qtworker_socket_dir)

        self._qtworker_core_port = next(self.env._open_port_iterator)
        self._qtworker_core_task_port = next(self.env._open_port_iterator)

        udfs_dir = self._resolve_udfs_dir()

        udf_resolver = self.config.get("qtworker_udf_resolver_path")
        if udf_resolver is None:
            raise YtError("qtworker_udf_resolver_path is not specified in config")

        stub = self.config.get("qtworker_udf_dep_stub_path")
        if not stub:
            raise YtError("qtworker_udf_dep_stub_path is not specified in config")
        worker_template = self.config.get("qtworker_worker_conf")
        if not worker_template:
            raise YtError("qtworker_worker_conf is not specified in config")
        with open(worker_template) as template_file:
            worker_text = template_file.read()
        qtworker_log_file = os.path.join(self.env.logs_path, "qtworker.log")

        for key, value in [
            ("${instance_root}", self._instance_root),
            ("${udfs_dir}", udfs_dir),
            ("${udf_resolver_path}", udf_resolver),
            ("${udf_dep_stub_path}", stub),
            ("${yql_yt_token_path}", self.token_path),
            ("${qtworker_log_file}", qtworker_log_file),
            ("${qt_socket_dir}", self._qtworker_socket_dir),
            ("${inspector_port}", str(self._qtworker_inspector_port)),
            ("${core_port}", str(self._qtworker_core_port)),
            ("${core_task_port}", str(self._qtworker_core_task_port)),
        ]:
            worker_text = worker_text.replace(key, value)

        self._qtworker_worker_conf = os.path.join(self.env.configs_path, "qtworker_worker.conf")
        with open(self._qtworker_worker_conf, "w") as worker_out:
            worker_out.write(worker_text)

    def _prepare_qtworker_gateways_conf(self):
        mr_job_bin = self._resolve_mrjob()

        gw_src = self.config.get("qtworker_gateways_conf")
        if not gw_src:
            raise YtError("qtworker_gateways_conf is not specified in config")

        with open(gw_src) as gw_file:
            gw_text = gw_file.read()
        gw_text = gw_text.replace("${instance_root}", self._instance_root)
        gw_text = gw_text.replace("${yt_debug_log_file}", os.path.join(self.env.logs_path, "qtworker_yt_debug.log"))
        gw_text = gw_text.replace("${cluster_name}", self.env.id)
        gw_text = gw_text.replace("${cluster_address}", self.env.get_http_proxy_address())
        gw_text = gw_text.replace("${mr_job_bin}", mr_job_bin)
        self._qtworker_gateways_conf = os.path.join(self.env.configs_path, "qtworker_gateways.conf")
        with open(self._qtworker_gateways_conf, "w") as gw_out:
            gw_out.write(gw_text)

    def _prepare_qtworker(self):
        binary = self.config.get("qtworker_path")

        self._qtworker_enabled = True
        self._qtworker_binary = binary
        self._qtworker_processes = []

        self._instance_root = os.path.join(self.env.path, "qtworker_instance")
        self._prepare_qtworker_fs_conf()
        self._prepare_qtworker_gateways_conf()
        self._prepare_qtworker_worker_conf()

    def run(self):
        logger.info("Starting yql agent")
        super(YqlAgent, self).run()
        if self._qtworker_enabled:
            logger.info("Starting qtworker")
            self._start_qtworker()
            logger.info("Qtworker started")
        logger.info("Yql agent started")

    def _run_qtworker_role(self, role: str):
        lowercase_name = f"qtworker_{role}"
        self._qtworker_processes.append(lowercase_name)

        self.env.prepare_external_component(
            self._qtworker_binary,
            lowercase_name,
            f"qtworker {role}s",
            [{}])  # qt_worker does not have the usual yson config

        args = [
            self._qtworker_binary,
            "--role",
            role,
            "--cfg",
            self._qtworker_worker_conf,
            "--fs-cfg",
            self._qtworker_fs_conf,
            "--gateways-cfg",
            self._qtworker_gateways_conf,
        ]

        self.env.run_external_component(name=lowercase_name, args=args)

    def _start_qtworker(self):
        self._run_qtworker_role("master")

        for role in ["forker", "core"]:
            socket_path = os.path.join(self._qtworker_socket_dir, f"{role}.socket")
            wait(lambda: os.path.exists(socket_path), ignore_exceptions=True)

        self._run_qtworker_role("forker")
        self._run_qtworker_role("core")

    def wait(self):
        logger.info("Waiting for yql agent to become ready")
        super(YqlAgent, self).wait()
        logger.info("Yql agent ready")

    def init(self):
        logger.info("Initialization for yql agent started")

        yql_agent_config = {
            "stages": {
                "production": {
                    "channel": {
                        "addresses": self.addresses,
                    }
                },
            },
        }

        self.client.set(f"//sys/clusters/{self.env.id}/yql_agent", yql_agent_config)
        self.client.set("//sys/@cluster_connection/yql_agent", yql_agent_config)

        def check_query():
            query_id = self.client.start_query("yql", "select 1")

            def get_state():
                return self.client.get_query(query_id, attributes=["state"])["state"]

            def is_finished():
                state = get_state()
                return state == "completed" or state == "failed"

            wait(is_finished)

            return get_state() == "completed"

        wait(check_query, ignore_exceptions=True)

        logger.info("Initialization for yql agent completed")

    def _get_artifact_path(self, subpath=""):
        return os.path.join(self.artifacts_path, subpath)

    def _resolve_mrjob(self):
        if self.artifacts_path:
            return self._get_artifact_path("mrjob")
        return self.config["mr_job_bin"]

    def _resolve_udfs_dir(self):
        if self.artifacts_path:
            return self._get_artifact_path()
        return self.config["mr_job_udfs_dir"]

    def _resolve_yql_plugin_so(self):
        if self.artifacts_path:
            return self._get_artifact_path("libyqlplugin.so")
        return self.config["yql_plugin_shared_library"]

    def override_common_settings(self, config, instance_index: int):
        # TODO(mpereskokova): YQLOVERYT-333: Remove after rpc timeout set in dq
        if self.enable_dq:
            config["rpc_dispatcher"]["alert_on_unset_request_timeout"] = False
        return config

    def get_default_config(self, instance_index: int):
        self.token_path = os.path.join(self.env.configs_path, "yql_agent_token")

        mr_job_bin, mr_job_udfs_dir, yql_plugin_shared_library = self._resolve_mrjob(), self._resolve_udfs_dir(), self._resolve_yql_plugin_so()

        process_plugin_config = None
        if self.subprocess_count:
            logging_config = {}
            yql_agent_plugin_slots_path = os.path.join(self.env.path, 'yql_agent', str(instance_index), 'plugin_slots')
            _init_logging(
                yql_agent_plugin_slots_path,
                'yql-plugin',
                logging_config,
                self.env.yt_config,
                enable_log_compression=False,
                log_errors_to_stderr=True
            )

            process_plugin_config = {
                "enabled": True,
                "slot_count": self.subprocess_count,
                "log_manager_template": logging_config,
                "slots_root_path": yql_agent_plugin_slots_path
            }

        cluster_mapping = [
            {
                "name": self.env.id,
                "cluster": self.env.get_http_proxy_address(),
                "default": True,
                "settings": [{"name": "_AllowRemoteClusterInput", "value": "true" if len(self.remote_envs) > 0 else "false"}]
            }
        ]

        for env in self.remote_envs:
            cluster_mapping.append({
                "name": env.id,
                "cluster": env.get_http_proxy_address(),
                "default": False
            })

        config = {
            "user": self.USER_NAME,
            "yql_agent": {
                "gateway_config": {
                    "mr_job_bin": mr_job_bin,
                    "mr_job_udfs_dir": mr_job_udfs_dir,
                    "cluster_mapping": cluster_mapping,
                    "default_settings": [{"name": "DefaultCalcMemoryLimit", "value": "2G"}]
                },
                # Slightly change the defaults to check if they can be overwritten.
                "file_storage_config": {"max_size_mb": 1 << 13},
                "yql_plugin_shared_library": yql_plugin_shared_library,
                "yt_token_path": self.token_path,
                "libraries": self.libraries,
            },
        }

        # Add DQ configuration if enabled
        if self.enable_dq:
            dq_vanilla_job = self._get_artifact_path("dq_vanilla_job")
            dq_vanilla_job_lite = self._get_artifact_path("dq_vanilla_job.lite")

            config["yql_agent"]["enable_dq"] = True
            config["yql_agent"]["dq_gateway_config"] = {
                "default_settings": [
                    {"name": "EnableComputeActor", "value": "1"},
                    {"name": "MemoryLimit", "value": "3G"},
                ]
            }
            config["yql_agent"]["dq_manager_config"] = {
                "interconnect_port": 31002,
                "grpc_port": 31001,
                "yt_backends": [
                    {
                        "cluster_name": self.env.get_http_proxy_address(),
                        "vanilla_job_lite": dq_vanilla_job_lite,
                        "vanilla_job_file": [
                            {
                                "name": "dq_vanilla_job",
                                "local_path": dq_vanilla_job,
                            }
                        ],
                        "token_file": self.token_path,
                        "user": self.USER_NAME,
                        "max_jobs": 1,
                        "jobs_per_operation": 1,
                    },
                ],
                "yt_coordinator": {
                    "cluster_name": self.env.get_http_proxy_address(),
                    "prefix": "//sys/yql_agent/dq_coord",
                    "token_file": self.token_path,
                    "user": self.USER_NAME,
                },
            }

        modify_yql_agent_config = self.config.get('modify_yql_agent_config')
        if modify_yql_agent_config is not None:
            modify_yql_agent_config(config)

        if self.max_supported_yql_version:
            config["yql_agent"]["max_supported_yql_version"] = self.max_supported_yql_version

        if self.default_yql_ui_version:
            config["yql_agent"]["default_yql_ui_version"] = self.default_yql_ui_version

        if self.allow_not_released_yql_versions is not None:
            config["yql_agent"]["allow_not_released_yql_versions"] = self.allow_not_released_yql_versions

        if self.config.get("enable_qtworker", False):
            config["yql_agent"]["use_qtworker_yql_plugin"] = True

            self._qtworker_inspector_port = next(self.env._open_port_iterator)
            config["yql_agent"]["qtworker_inspector_port"] = self._qtworker_inspector_port

        if process_plugin_config:
            config["yql_agent"]["process_plugin_config"] = process_plugin_config

        return config

    def wait_for_readiness(self, address):
        wait(lambda: self.client.get(f"//sys/yql_agent/instances/{address}/orchid/service/version"),
             ignore_exceptions=True,
             timeout=600)

    def stop(self):
        if self._qtworker_processes:
            logger.info("Stopping qtworker processes")
            for process in self._qtworker_processes:
                self.env.kill_service(process)
            logger.info("Qtworker processes stopped")
            self._qtworker_processes = []

        logger.info("Stopping yql agent")
        super(YqlAgent, self).stop()

        self.client.remove(f"//sys/users/{self.USER_NAME}")
        self.client.remove("//sys/yql_agent/instances", recursive=True, force=True)
        self.client.remove(f"//sys/clusters/{self.env.id}/yql_agent", recursive=True, force=True)
        self.client.remove("//sys/yql_agent", recursive=True, force=True)
        logger.info("Yql agent stopped")
