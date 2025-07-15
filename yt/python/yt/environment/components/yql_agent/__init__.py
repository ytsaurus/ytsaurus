from yt.common import wait, YtError

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

    def prepare(self, env, config):
        logger.info("Preparing yql agent")

        if "path" in config:
            self.PATH = config["path"]

        self.libraries = config.get("libraries", {})
        self.config = config

        if "artifacts_path" in config:
            self.artifacts_path = config["artifacts_path"]
        elif "mr_job_bin" not in config \
                or "mr_job_udfs_dir" not in config \
                or "yql_plugin_shared_library" not in config:
            raise YtError("Artifacts path is not specified in yql agent config")
        
        if "process_plugin_config" in config:
            enabled = config["process_plugin_config"].get("enabled", False)
            self.process_plugin_config = config["process_plugin_config"] if enabled else None

        self.max_supported_yql_version = config["max_supported_yql_version"] if "max_supported_yql_version" in config else None

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

        logger.info("Yql agent prepared")

    def run(self):
        logger.info("Starting yql agent")
        super(YqlAgent, self).run()
        logger.info("Yql agent started")

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

        wait(check_query)

        logger.info("Initialization for yql agent completed")

    def _get_artifact_path(self, subpath=""):
        return os.path.join(self.artifacts_path, subpath)

    def get_default_config(self):
        self.token_path = os.path.join(self.env.configs_path, "yql_agent_token")

        mr_job_bin, mr_job_udfs_dir, yql_plugin_shared_library = None, None, None
        if self.artifacts_path:
            mr_job_bin = self._get_artifact_path("mrjob")
            mr_job_udfs_dir = self._get_artifact_path()
            yql_plugin_shared_library = self._get_artifact_path("libyqlplugin.so")
        else:
            mr_job_bin = self.config["mr_job_bin"]
            mr_job_udfs_dir = self.config["mr_job_udfs_dir"]
            yql_plugin_shared_library = self.config["yql_plugin_shared_library"]

        config = {
            "user": self.USER_NAME,
            "yql_agent": {
                "gateway_config": {
                    "mr_job_bin": mr_job_bin,
                    "mr_job_udfs_dir": mr_job_udfs_dir,
                    "cluster_mapping": [
                        {
                            "name": self.env.id,
                            "cluster": self.env.get_http_proxy_address(),
                            "default": True,
                        }
                    ],
                    "default_settings": [{"name": "DefaultCalcMemoryLimit", "value": "2G"}]
                },
                # Slightly change the defaults to check if they can be overwritten.
                "file_storage_config": {"max_size_mb": 1 << 13},
                "yql_plugin_shared_library": yql_plugin_shared_library,
                "ui_origin": self.config.get("ui_origin", ""),
                "yt_token_path": self.token_path,
                "libraries": self.libraries,
            },
        }

        modify_yql_agent_config = self.config.get('modify_yql_agent_config')
        if modify_yql_agent_config is not None:
            modify_yql_agent_config(config)

        if self.max_supported_yql_version:
            config["yql_agent"]["max_supported_yql_version"] = self.max_supported_yql_version

        return config

    def wait_for_readiness(self, address):
        wait(lambda: self.client.get(f"//sys/yql_agent/instances/{address}/orchid/service/version"),
             ignore_exceptions=True,
             timeout=600)

    def stop(self):
        logger.info("Stopping yql agent")
        super(YqlAgent, self).stop()
        self.client.remove(f"//sys/users/{self.USER_NAME}")
        self.client.remove("//sys/yql_agent/instances", recursive=True, force=True)
        self.client.remove(f"//sys/clusters/{self.env.id}/yql_agent", recursive=True, force=True)
        self.client.remove("//sys/yql_agent", recursive=True, force=True)
        logger.info("Yql agent stopped")
