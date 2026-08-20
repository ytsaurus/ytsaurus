from conftest_lib.conftest_queries import *  # noqa

from common import merge_old_dynconfig_into_new_static

from yt_commands import (get, set, create, write_file)

from yt.environment.components.yql_agent import YqlAgent as YqlAgentComponent

from yt.environment.helpers import wait, wait_for_dynamic_config_update

from yt.common import YtError, YtResponseError

from google.protobuf.text_format import Parse, MessageToString

import yql.essentials.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

import datetime
import os
import shutil

import pytest

import yatest.common


class YqlAgent():
    def __init__(self, env, remote_envs, count, libraries, config):
        self.yql_agent = YqlAgentComponent()

        config = {
            "count": count,
            "path": yatest.common.binary_path("yt/yql/agent/bin"),
            "mr_job_bin": yatest.common.binary_path("yt/yql/tools/mrjob/mrjob"),
            "mr_job_udfs_dir": yatest.common.binary_path("yql/essentials/udfs/common"),
            "native_client_supported": True,
            "libraries": libraries,
        } | config

        self.yql_agent.prepare(env, config=config, remote_envs=remote_envs)

        initial_dynamic_config = config.get("initial_dynamic_config")
        if initial_dynamic_config is not None:
            self.apply_dynamic_config(initial_dynamic_config)

    def __enter__(self):
        self.yql_agent.run()
        self.yql_agent.wait()
        self.yql_agent.init()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.yql_agent.stop()

    def render_gateways_conf(self, env):
        gateways_text = self.yql_agent.render_gateways_conf()
        return Parse(gateways_text, gateways_config_pb2.TGatewaysConfig())

    def apply_dynamic_config(self, dynconfig):
        if self.yql_agent.config.get("enable_qtworker", False) and "gateways" in dynconfig:
            config = self.render_gateways_conf(self.yql_agent.env)

            merge_old_dynconfig_into_new_static(config, dynconfig["gateways"])
            filename = "//sys/yql_agent/proto_gateways/default.conf"
            create("file", filename, recursive=True, force=True)
            write_file(filename, MessageToString(config).encode('utf-8'))

        config = get("//sys/yql_agent/config")
        config["yql_agent"] = dynconfig
        set("//sys/yql_agent/config", config)

        return config


def copy_yql_configs_to_test_folder(yql_agent):
    for config_path in yql_agent.config_paths:
        test_folder_configs_path = os.path.join(yatest.common.output_path(), "yql_agent_configs")
        os.makedirs(test_folder_configs_path, exist_ok=True)
        shutil.copy(config_path, test_folder_configs_path)


def update_yql_agent_environment(cls, yql_agent):
    if hasattr(cls, "YQL_AGENT_DYNAMIC_CONFIG") :
        config = yql_agent.apply_dynamic_config(getattr(cls, "YQL_AGENT_DYNAMIC_CONFIG"))
        wait_for_dynamic_config_update(yql_agent.yql_agent.client, config, "//sys/yql_agent/instances")


def wait_for_udf_meta_update(client, expected_meta):
    instances = client.list("//sys/yql_agent/instances")

    if not instances:
        return

    def check():
        batch_client = client.create_batch_client()

        responses = [
            batch_client.get("//sys/yql_agent/instances/{0}/orchid/yql_agent/udf_meta".format(instance))
            for instance in instances
        ]
        batch_client.commit_batch()

        for response in responses:
            if not response.is_ok():
                raise YtResponseError(response.get_error())

            if expected_meta != response.get_result():
                return False

        return True

    wait(check, error_message="UDF meta didn't become as expected in time", ignore_exceptions=True)


def setup_udf_registry(cls, yql_agent, udfs):
    client = yql_agent.yql_agent.client
    cluster = yql_agent.yql_agent.env.id

    udfs_root = "//sys/yql_agent/udfs"
    client.create("map_node", udfs_root)

    updated_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    meta = {}
    for key, entry in udfs.items():
        local_path = yatest.common.binary_path(entry["path"])
        remote_path = f"{udfs_root}/{os.path.basename(local_path)}"

        client.create("file", remote_path)
        with open(local_path, "rb") as f:
            client.write_file(remote_path, f)

        meta[key] = {
            **entry,
            "alias": f"yt://{cluster}{remote_path}",
            "updated_at": updated_at,
        }

    meta = {
        "udfs": meta
    }

    meta_path = f"{udfs_root}/_meta"
    client.create("document", meta_path)
    client.set(meta_path, meta)

    wait_for_udf_meta_update(client, meta)


@pytest.fixture
def yql_agent(request):
    cls = request.cls
    count = getattr(cls, "NUM_YQL_AGENTS", 1)

    libraries = {}
    if hasattr(cls, "YQL_TEST_LIBRARY"):
        test_lib_path = os.path.join(cls.Env.configs_path, "test_lib.sql")
        libraries["test"] = test_lib_path
        with open(test_lib_path, "w") as fp:
            fp.write(getattr(cls, "YQL_TEST_LIBRARY"))

    config = {}
    config["modify_yql_agent_config"] = getattr(cls, "modify_yql_agent_config", None)
    config["max_supported_yql_version"] = getattr(cls, "MAX_YQL_VERSION", None)
    config["default_yql_ui_version"] = getattr(cls, "DEFAULT_YQL_UI_VERSION", None)
    config["allow_not_released_yql_versions"] = getattr(cls, "ALLOW_NOT_RELEASED_YQL_VERSIONS", True)
    config["subprocess_count"] = getattr(cls, "YQL_SUBPROCESS_COUNT", None)
    config["dynamic_config_update_period"] = getattr(cls, "DYNAMIC_CONFIG_UPDATE_PERIOD", "1s")
    config["initial_dynamic_config"] = getattr(cls, "YQL_AGENT_INITIAL_DYNAMIC_CONFIG", None)

    use_qtworker = getattr(cls, "YQL_QTWORKER", False)
    if use_qtworker:
        if config.get("subprocess_count"):
            raise YtError("YQL_QTWORKER and YQL_SUBPROCESS_COUNT cannot be set together")
        config["enable_qtworker"] = True
        config["qtworker_path"] = yatest.common.binary_path("yt/yql/tools/qtworker/qtworker")
        config["qtworker_worker_conf"] = yatest.common.source_path("yt/yql/cfg/tests/worker.conf")
        config["qtworker_fs_conf"] = yatest.common.source_path("yt/yql/cfg/tests/fs.conf")
        config["qtworker_gateways_conf"] = yatest.common.source_path(
            "yt/yql/cfg/tests/gateways.conf")
        config["qtworker_udf_resolver_path"] = yatest.common.binary_path(
            "yql/essentials/tools/udf_resolver/udf_resolver")
        config["qtworker_udf_dep_stub_path"] = yatest.common.binary_path(
            "yql/essentials/tools/udf_dep_stub/libyql_udf_dep_stub.so")

    with YqlAgent(cls.Env, cls.remote_envs, count, libraries, config) as yql_agent:
        update_yql_agent_environment(cls, yql_agent)
        copy_yql_configs_to_test_folder(yql_agent.yql_agent)

        udfs = getattr(cls, "YQL_UDF_REGISTRY", {})
        if udfs:
            if not use_qtworker:
                raise YtError("YQL_UDF_REGISTRY requires YQL_QTWORKER to be set")

            setup_udf_registry(cls, yql_agent, udfs)

        yield yql_agent
