from conftest_lib.conftest_queries import *  # noqa

from yt_commands import (
    create, create_user, remove_user, remove, add_member, sync_remove_tablet_cells, ls, set,
    issue_token, get_connection_config, wait, get)

import os.path
import os

import pytest

from yt.environment import ExternalComponent

import yatest.common


class YqlAgent(ExternalComponent):
    LOWERCASE_NAME = "yql_agent"
    DASHED_NAME = "yql-agent"
    PLURAL_HUMAN_READABLE_NAME = "yql agents"
    PATH = yatest.common.binary_path("yt/yql/agent/bin")

    def __init__(self, env, count):
        super().__init__(env, count)

        yql_agent_token, _ = issue_token("yql_agent")
        with open(self.token_path, "w") as file:
            file.write(yql_agent_token)

    def get_artifact_path(self, subpath=""):
        root_path = None
        if "YDB_ARTIFACTS_PATH" in os.environ:
            root_path = os.environ["YDB_ARTIFACTS_PATH"]
        else:
            root_path = yatest.common.runtime.work_path("yt_binaries")
        return os.path.join(root_path, subpath)

    def get_default_config(self):
        self.token_path = os.path.join(self.env.configs_path, "yql_agent_token")

        return {
            "user": "yql_agent_test_user",
            "yql_agent": {
                "gateway_config": {
                    "mr_job_bin": self.get_artifact_path("mrjob"),
                    "mr_job_udfs_dir": self.get_artifact_path(),
                    "cluster_mapping": [
                        {
                            "name": "primary",
                            "cluster": self.env.get_http_proxy_address(),
                            "default": True,
                        }
                    ],
                    "default_settings": [{"name": "DefaultCalcMemoryLimit", "value": "2G"}]
                },
                # Slightly change the defaults to check if they can be overwritten.
                "file_storage_config": {"max_size_mb": 1 << 13},
                "yql_plugin_shared_library": self.get_artifact_path("libyqlplugin.so"),
                "yt_token_path": self.token_path,
            },
        }

    def wait_for_readiness(self, address):
        wait(lambda: get(f"//sys/yql_agent/instances/{address}/orchid/service/version",
                         verbose=False, verbose_error=False),
             ignore_exceptions=True)

    def on_start(self):
        yql_agent_config = {
            "stages": {
                "production": {
                    "channel": {
                        "addresses": self.addresses,
                    }
                },
            },
        }
        set("//sys/clusters/primary/yql_agent", yql_agent_config)
        wait(lambda: get_connection_config(verbose=False)
             .get("yql_agent", {}).get("stages", {}).get("production") is not None)

    def on_finish(self):
        remove("//sys/yql_agent/instances", recursive=True, force=True)
        remove("//sys/clusters/primary/yql_agent", recursive=True, force=True)


@pytest.fixture
def yql_agent_environment():
    create_user("yql_agent_test_user")
    add_member("yql_agent_test_user", "superusers")
    create("document", "//sys/yql_agent/config", recursive=True, force=True, attributes={"value": {}})
    yield
    sync_remove_tablet_cells(ls("//sys/tablet_cells"))
    remove_user("yql_agent_test_user")
    remove("//sys/yql_agent", recursive=True, force=True)


@pytest.fixture
def yql_agent(request, yql_agent_environment):
    cls = request.cls
    count = getattr(cls, "NUM_YQL_AGENTS", 1)
    with YqlAgent(cls.Env, count) as yql_agent:
        yield yql_agent
