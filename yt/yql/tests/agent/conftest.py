from conftest_lib.conftest_queries import *  # noqa

from yt_commands import (get, set, create, write_file)

from yt.environment.components.yql_agent import YqlAgent as YqlAgentComponent

from yt.environment.helpers import wait_for_dynamic_config_update

from yt.common import YtError

from google.protobuf.text_format import Parse, MessageToString

import yql.essentials.providers.common.proto.gateways_config_pb2 as gateways_config_pb2

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
            "mr_job_udfs_dir": os.path.dirname(yatest.common.binary_path("yql/essentials/udfs/common")),
            "yql_plugin_shared_library": yatest.common.binary_path("yt/yql/plugin/dynamic/libyqlplugin.so"),
            "native_client_supported": True,
            "libraries": libraries,
        } | config

        self.yql_agent.prepare(env, config=config, remote_envs=remote_envs)

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


def copy_yql_configs_to_test_folder(yql_agent):
    for config_path in yql_agent.config_paths:
        test_folder_configs_path = os.path.join(yatest.common.output_path(), "yql_agent_configs")
        os.makedirs(test_folder_configs_path, exist_ok=True)
        shutil.copy(config_path, test_folder_configs_path)


def convert_camel_to_snake(camel_str):
    result = []
    for i, ch in enumerate(camel_str):
        if ch.isupper() and i > 0 and not camel_str[i - 1].isupper():
            result.append("_")
        result.append(ch.lower())
    return "".join(result)


def merge_old_dynconfig_into_new_static(config, override):
    fields_by_snake = {convert_camel_to_snake(field.name): field for field in config.DESCRIPTOR.fields}
    for key, value in override.items():
        field = fields_by_snake[key]
        if field.label == field.LABEL_REPEATED:
            repeated = getattr(config, field.name)
            name_field = field.message_type.fields_by_name.get("Name") if field.type == field.TYPE_MESSAGE else None
            if name_field is not None and name_field.type == name_field.TYPE_STRING:
                name_key = convert_camel_to_snake(name_field.name)
                existing_by_name = {getattr(item, name_field.name): item for item in repeated}
                for item in value:
                    item_name = item.get(name_key)
                    if item_name is not None and item_name in existing_by_name:
                        merge_old_dynconfig_into_new_static(existing_by_name[item_name], item)
                    else:
                        merge_old_dynconfig_into_new_static(repeated.add(), item)
            elif field.type == field.TYPE_MESSAGE:
                for item in value:
                    merge_old_dynconfig_into_new_static(repeated.add(), item)
            else:
                repeated.extend(value)
        elif field.type == field.TYPE_MESSAGE:
            merge_old_dynconfig_into_new_static(getattr(config, field.name), value)
        else:
            setattr(config, field.name, value)


def update_yql_agent_environment(cls, yql_agent):
    if hasattr(cls, "YQL_AGENT_DYNAMIC_CONFIG") :
        dynconfig = getattr(cls, "YQL_AGENT_DYNAMIC_CONFIG")

        if getattr(cls, "YQL_QTWORKER", False) and "gateways" in dynconfig:
            config = yql_agent.render_gateways_conf(yql_agent.yql_agent.env)

            merge_old_dynconfig_into_new_static(config, dynconfig["gateways"])
            filename = "//sys/yql_agent/proto_gateways/default.conf"
            create("file", filename, recursive=True, force=True)
            write_file(filename, MessageToString(config).encode('utf-8'))

        config = get("//sys/yql_agent/config")
        config["yql_agent"] = dynconfig
        set("//sys/yql_agent/config", config)

        wait_for_dynamic_config_update(yql_agent.yql_agent.client, config, "//sys/yql_agent/instances")


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
        yield yql_agent
