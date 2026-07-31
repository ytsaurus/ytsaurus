import yt_queries

from yt_env_setup import YTEnvSetup, get_sanitizer_type

from yt_commands import (create, get, set, write_file)

from yt.environment.helpers import wait_for_dynamic_config_update

from google.protobuf.text_format import MessageToString

import hashlib
import tarfile
import tempfile


LLVM_SYMBOLIZER_PATH = "contrib/libs/llvm18/tools/llvm-symbolizer/llvm-symbolizer"

SANITIZER_STUFF_PATH = "sanitizer-stuff"
SANITIZER_STUFF_ARCHIVE_FILENAME = "sanitizer-stuff.tar"
LLVM_SYMBOLIZER_FILENAME = "llvm-symbolizer"
SUPPRESSIONS_FILENAME = "suppressions.txt"


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


class TestQueriesYqlBase(YTEnvSetup):
    NUM_NODES = 3
    NUM_YQL_AGENTS = 1
    NUM_MASTERS = 1
    NUM_DISCOVERY_SERVERS = 1
    NUM_QUERY_TRACKER = 1
    ENABLE_HTTP_PROXY = True
    NUM_HTTP_PROXIES = 1
    ENABLE_RPC_PROXY = True
    NUM_RPC_PROXIES = 1
    USE_DYNAMIC_TABLES = True
    NUM_SCHEDULERS = 1
    CLASS_TEST_LIMIT = 960

    DELTA_DRIVER_CONFIG = {
        "cluster_connection_dynamic_config_policy": "from_cluster_directory",
    }

    COPY_YTSERVER = False

    if get_sanitizer_type():
        # llvm-symbolizer is required in *san builds to symbolize stack,
        # so we bring it into user jobs via porto layers
        USE_PORTO = True

        DELTA_NODE_CONFIG = {
            "exec_node": {
                "job_proxy": {
                    "test_root_fs": True,
                },
                "slot_manager": {
                    "job_environment": {
                        "type": "porto",
                    },
                },
            },
        }

    _root_value = None
    _files_initialized = False

    @property
    def _root(self):
        if self._root_value is None:
            import yatest.common as yc

            md5 = hashlib.md5(yc.context.test_name.encode("utf-8")).hexdigest()
            self._root_value = "/".join(["//tmp", md5])

        return self._root_value

    @property
    def _sanitizer_stuff_archive_yt_path(self):
        return "/".join([self._root, SANITIZER_STUFF_ARCHIVE_FILENAME])

    def _setup_files(self, authenticated_user=None):
        if self._files_initialized:
            return

        import yatest.common as yc

        def set_permissions(tarinfo):
            tarinfo.mode = 0o755
            return tarinfo

        with (
            tempfile.NamedTemporaryFile(delete_on_close=False) as suppressions_file,
            tempfile.TemporaryFile() as archive_file,
        ):
            suppressions_file.write(b'''
# known leaks in python3: https://github.com/python/cpython/issues/113190
leak:*Py_InitializeEx*
# some unknown python leaks which should be debugged
leak:*PyRun_SimpleStringFlags*
# some yql leaks which should be debugged
leak:*InitYqlModule*
leak:*ToPySecureParam*
''')
            suppressions_file.close()

            with tarfile.open(fileobj=archive_file, mode="w", dereference=True) as tar_archive:
                tar_archive.add(
                    yc.binary_path(LLVM_SYMBOLIZER_PATH),
                    arcname=f"{SANITIZER_STUFF_PATH}/{LLVM_SYMBOLIZER_FILENAME}",
                    filter=set_permissions,
                )

                tar_archive.add(
                    suppressions_file.name,
                    arcname=f"{SANITIZER_STUFF_PATH}/{SUPPRESSIONS_FILENAME}",
                    filter=set_permissions,
                )

            create(
                "file", self._sanitizer_stuff_archive_yt_path,
                recursive=True,
                authenticated_user=authenticated_user,
            )

            archive_file.seek(0)
            write_file(
                self._sanitizer_stuff_archive_yt_path, archive_file.read(),
                authenticated_user=authenticated_user,
            )

        self._files_initialized = True

    def start_query(self, engine, query, **kwargs):
        if engine != "yql" or not get_sanitizer_type():
            return yt_queries.start_query(engine, query, **kwargs)

        self._setup_files(authenticated_user=kwargs.get("authenticated_user"))

        query_prefix = f"""
pragma yt.LayerPaths = "{self._sanitizer_stuff_archive_yt_path}";
pragma yt.JobEnv = '{{
    ASAN_SYMBOLIZER_PATH = "$(RootFS)/{SANITIZER_STUFF_PATH}/{LLVM_SYMBOLIZER_FILENAME}";
    MSAN_SYMBOLIZER_PATH = "$(RootFS)/{SANITIZER_STUFF_PATH}/{LLVM_SYMBOLIZER_FILENAME}";
    LSAN_SYMBOLIZER_PATH = "$(RootFS)/{SANITIZER_STUFF_PATH}/{LLVM_SYMBOLIZER_FILENAME}";
    TSAN_SYMBOLIZER_PATH = "$(RootFS)/{SANITIZER_STUFF_PATH}/{LLVM_SYMBOLIZER_FILENAME}";
    UBSAN_SYMBOLIZER_PATH = "$(RootFS)/{SANITIZER_STUFF_PATH}/{LLVM_SYMBOLIZER_FILENAME}";
    LSAN_OPTIONS = "malloc_context_size=200:suppressions=$(RootFS)/{SANITIZER_STUFF_PATH}/{SUPPRESSIONS_FILENAME}";
}}';
"""

        query = query_prefix + query

        return yt_queries.start_query(engine, query, **kwargs)


class TestUpdateYqlAgentDynamicConfigMixin:
    def _update_dyn_config(self, yql_agent, dyn_config):
        config = get("//sys/yql_agent/config")
        config["yql_agent"] = dyn_config
        set("//sys/yql_agent/config", config)
        wait_for_dynamic_config_update(yql_agent.yql_agent.client, config, "//sys/yql_agent/instances")


class TestUpdateYqlAgentQtWorkerDynamicConfigMixin(TestUpdateYqlAgentDynamicConfigMixin):
    def _update_dyn_config(self, yql_agent, dyn_config):
        if "gateways" in dyn_config:
            config = yql_agent.render_gateways_conf(yql_agent.yql_agent.env)
            merge_old_dynconfig_into_new_static(config, dyn_config["gateways"])
            filename = "//sys/yql_agent/proto_gateways/default.conf"
            create("file", filename, recursive=True, force=True)
            write_file(filename, MessageToString(config).encode('utf-8'))

        super()._update_dyn_config(yql_agent, dyn_config)
