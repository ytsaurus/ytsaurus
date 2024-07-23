from helpers import get_scheduling_options

from yt_commands import (create, write_file, ls, start_op, get, exists, update_op_parameters, create_user,
                         sync_create_cells, print_debug, get_driver, remove, make_ace, set as yt_set)

from yt.clickhouse import get_clique_spec_builder
from yt.clickhouse.test_helpers import get_host_paths, get_clickhouse_server_config

from yt.environment import arcadia_interop

from yt_env_setup import YTEnvSetup, is_asan_build

from yt.wrapper import YtClient
from yt.wrapper.common import simplify_structure

from yt.common import update, update_inplace, YtError, wait, parts_to_uuid, YtResponseError

from yt.test_helpers.profiler import Profiler

import yt.packages.requests as requests

import yt.yson as yson

from threading import Thread
import os
import errno
import time
import json
import random
import copy
import string

HOST_PATHS = get_host_paths(arcadia_interop, ["ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer"])

DEFAULTS = {
    "memory_config": {
        "footprint": 1 * 1024 ** 3,
        "clickhouse": int(2.5 * 1024 ** 3),
        "reader": 1 * 1024 ** 3,
        "uncompressed_block_cache": 0,
        "compressed_block_cache": 0,
        "chunk_meta_cache": 0,
        "log_tailer": 0,
        "watchdog_oom_watermark": 0,
        "watchdog_window_oom_watermark": 0,
        "clickhouse_watermark": 1 * 1024 ** 3,
        "memory_limit": int((1 + 2.5 + 1 + 1) * 1024 ** 3),
        "max_server_memory_usage": int((1 + 2.5 + 1) * 1024 ** 3),
    },
    "host_ytserver_clickhouse_path": HOST_PATHS["ytserver-clickhouse"],
    "host_clickhouse_trampoline_path": HOST_PATHS["clickhouse-trampoline"],
    "host_ytserver_log_tailer_path": HOST_PATHS["ytserver-log-tailer"],
    "cpu_limit": 1,
    "enable_monitoring": False,
    "clickhouse_config": {},
    "max_instance_count": 100,
}

QUERY_TYPES_WITH_OUTPUT = ("describe", "select", "show", "exists", "explain", "with")

UserJobFailed = 1205
QueryFailedError = 2200
InstanceUnavailableCode = 2201


def get_current_test_name():
    return os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]


def _generate_random_alias():
    return ''.join(random.choices(string.ascii_lowercase, k=10))


class Clique(object):
    base_config = None
    clique_index = 0
    query_index = 0
    path_to_run = None
    core_dump_path = None
    proxy_address = None
    clique_index_by_test_name = {}
    alias = None
    tvm_secret = None
    sql_udf_path = None

    def __init__(self, instance_count, max_failed_job_count=0, config_patch=None, cpu_limit=None, alias=None, **kwargs):
        """
        alias: str
            Alias for the database. With or without asterisk: both forms are legal.
            It will be stored in self.alias with all asterisks discarded.

            Is generated randomly if not provided.
        """

        discovery_patch = {
            "yt": {
                "discovery": {
                    "version": 2,
                    "heartbeat_period": 400,
                    "attribute_update_period": 2000,
                    "lease_timeout": 1000,
                },
                "user_defined_sql_objects_storage": {
                    "update_period": 300,
                    "expire_after_successful_sync_time": 1000,
                }
            },
        }
        config = update(Clique.base_config, discovery_patch)

        if config_patch is not None:
            config = update(config, config_patch)

        self.discovery_version = config["yt"]["discovery"]["version"]
        self.discovery_servers = ls("//sys/discovery_servers")

        self.alias = (alias.removeprefix("*")
                      if alias is not None
                      else _generate_random_alias())

        assert self.alias != ""

        # alias processing
        config["yt"]["clique_alias"] = self.alias

        self.sql_udf_path = "//sys/strawberry/chyt/{}/user_defined_sql_functions".format(self.alias)
        ace = make_ace("allow", "chyt-sql-objects", ["read", "write", "remove"])
        create("map_node", self.sql_udf_path, recursive=True, attributes={
            "acl": [ace],
        })
        config["yt"]["user_defined_sql_objects_storage"]["path"] = self.sql_udf_path
        config["yt"]["user_defined_sql_objects_storage"]["enabled"] = True

        spec = {"pool": None}
        self.is_tracing = False
        if "YT_TRACE_DUMP_DIR" in os.environ:
            self.is_tracing = True
            spec["tasks"] = {"instances": {"environment": {"YT_TRACE_DUMP_DIR": os.environ["YT_TRACE_DUMP_DIR"]}}}
        if "spec" in kwargs:
            spec = update(spec, kwargs.pop("spec"))

        test_name = get_current_test_name()
        clique_index_in_test = Clique.clique_index_by_test_name.get(test_name, 0)
        Clique.clique_index_by_test_name[test_name] = clique_index_in_test + 1
        clique_dir_name = "clickhouse-{}-{}".format(test_name, clique_index_in_test)

        self.log_root = os.path.join(self.path_to_run, "logs", clique_dir_name)
        self.stderr_root = os.path.join(self.path_to_run, "stderrs", clique_dir_name)
        for root in (self.log_root, self.stderr_root):
            try:
                os.makedirs(root)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    pass
            os.chmod(root, 0o777)

        for writer_key, writer in config["logging"]["writers"].items():
            if writer["type"] == "file":
                writer["file_name"] = os.path.join(self.log_root, writer["file_name"])

        filename = "//sys/clickhouse/config-{}.yson".format(Clique.clique_index)
        Clique.clique_index += 1
        create("file", filename)
        config_str = yson.dumps(config, yson_format="pretty")
        assert config_str.find(b"TestSecret") == -1
        write_file(filename, config_str)

        cypress_config_paths = {"clickhouse_server": (filename, "config.yson")}
        if "cypress_ytserver_log_tailer_config_path" in kwargs:
            cypress_config_paths["log_tailer"] = (
                kwargs.pop("cypress_ytserver_log_tailer_config_path"),
                "log_tailer_config.yson",
            )

        core_dump_destination = os.environ.get("YT_CORE_DUMP_DESTINATION")

        spec_builder = get_clique_spec_builder(
            instance_count,
            cypress_config_paths=cypress_config_paths,
            max_failed_job_count=max_failed_job_count,
            defaults=DEFAULTS,
            cpu_limit=cpu_limit,
            spec=spec,
            core_dump_destination=core_dump_destination,
            trampoline_log_file=os.path.join(self.log_root, "trampoline-$YT_JOB_INDEX.debug.log"),
            stderr_file=os.path.join(self.stderr_root, "stderr.clickhouse-$YT_JOB_INDEX"),
            ytserver_readiness_timeout=15,
            tvm_secret=Clique.tvm_secret,
            **kwargs
        )
        self.spec = simplify_structure(spec_builder.build())
        if not is_asan_build() and core_dump_destination is not None:
            self.spec["tasks"]["instances"]["force_core_dump"] = True

        self.spec["alias"] = "*" + self.alias

        self.instance_count = instance_count

    def get_active_instances_for_discovery_v1(self):
        if exists("//sys/clickhouse/cliques/{0}".format(self.op.id), verbose=False):
            instances = ls(
                "//sys/clickhouse/cliques/{0}".format(self.op.id),
                attributes=["locks", "host", "http_port", "monitoring_port", "job_cookie", "pid"],
                verbose=False,
            )

            def is_active(instance):
                if not instance.attributes["locks"]:
                    return False
                for lock in instance.attributes["locks"]:
                    if lock["child_key"] and lock["child_key"] == "lock":
                        return True
                return False

            return list(filter(is_active, instances))
        else:
            return []

    def get_group_id(self):
        return self.alias or self.get_clique_id()

    def get_active_instances_for_discovery_v2(self):
        assert len(self.discovery_servers) > 0
        discovery_server = self.discovery_servers[0]
        group_id = self.get_group_id()
        try:
            instances = ls(
                "//sys/discovery_servers/{}/orchid/discovery_server/chyt/{}/@members"
                .format(discovery_server, group_id),
                attributes=["host", "http_port", "monitoring_port", "job_cookie", "pid"],
                verbose=False,
            )
            return [instance for instance in instances if instance.attributes]
        except YtResponseError:
            return []

    def get_active_instances(self):
        if self.discovery_version == 1:
            return self.get_active_instances_for_discovery_v1()
        else:
            assert self.discovery_version == 2
            return self.get_active_instances_for_discovery_v2()

    def get_active_instance_count(self):
        return len(self.get_active_instances())

    def make_query_and_validate_row_count(self, query, exact=None, min=None, max=None, verbose=True, **kwargs):
        result = self.make_query(query, verbose=verbose, only_rows=False, **kwargs)
        assert (exact is not None) ^ (min is not None and max is not None)
        if exact is not None:
            assert result["statistics"]["rows_read"] == exact
        else:
            assert min <= result["statistics"]["rows_read"] <= max
        return result["data"]

    def _print_progress(self):
        print_debug(self.op.build_progress(), "(active instance count: {})".format(self.get_active_instance_count()))

    def __enter__(self):
        self.op = start_op("vanilla", spec=self.spec, track=False)

        self.log_root_alternative = os.path.realpath(
            os.path.join(self.log_root, "..", "clickhouse-{}".format(self.op.id))
        )
        os.symlink(self.log_root, self.log_root_alternative)

        print_debug("Waiting for clique {} to become ready".format(self.op.id))
        print_debug("Logging roots:\n- {}\n- {}".format(self.log_root, self.log_root_alternative))

        max_counter_value = 600
        counter = 0
        while True:
            state = self.op.get_state(verbose=False)

            # ClickHouse operations should never complete by itself.
            assert state != "completed"

            if state == "aborted" or state == "failed":
                raise self.op.get_error()
            elif state == "running":
                if self.get_active_instance_count() == self.instance_count:
                    break

            if counter % 30 == 0:
                self._print_progress()
            elif counter >= max_counter_value:
                clique_directory_path = "//sys/clickhouse/cliques/{}".format(self.op.id)
                clique_directory = None
                if exists(clique_directory_path):
                    clique_directory = get(clique_directory_path, verbose=False)

                raise YtError(
                    "Clique did not start in time, clique directory: {}".format(
                        clique_directory if clique_directory is not None else "does not exist"
                    )
                )

            time.sleep(self.op._poll_frequency)
            counter += 1

        self._print_progress()

        print_debug("Waiting for all instances to know about each other")

        def check_all_instance_pairs():
            clique_size_per_instance = []
            for instance in self.get_active_instances():
                clique_size = self.make_direct_query(instance, "select count(*) from system.clique", verbose=False)[0][
                    "count()"
                ]
                clique_size_per_instance.append(clique_size)
            # print_debug("Clique sizes over all instances: {}".format(clique_size_per_instance))
            return min(clique_size_per_instance) == self.instance_count

        wait(check_all_instance_pairs)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        print_debug("Exiting clique %s", self.op.id)
        time.sleep(2)
        try:
            self.op.complete()

            if self.sql_udf_path:
                remove(self.sql_udf_path, recursive=True, force=True)

        except YtError as err:
            print_debug("Error while completing clique operation:", err)
        clique_error = None
        try:
            self.op.track()
        except YtError as err:
            clique_error = err

        for breakpoint_name in ls("//sys/clickhouse/breakpoints"):
            remove("//sys/clickhouse/breakpoints/{}".format(breakpoint_name), recursive=True)

        if clique_error is not None:
            if exc_type is not None:
                original_error = exc_value
                raise YtError(
                    "ClickHouse request failed and resulted in clique failure",
                    inner_errors=[original_error, clique_error],
                )
            else:
                raise YtError("Clique failed", inner_errors=[clique_error])

        # Pass the error.
        return False

    @staticmethod
    def _parse_error(text):
        lines = text.split("\n")
        in_error = False
        result = []
        for line in lines:
            if line.startswith("std::exception") or line.startswith("Code:"):
                in_error = True
            if in_error:
                result.append(line)
        if len(result) == 0:
            return None
        return YtError("ClickHouse request failed:\n" + "\n".join(result))

    def make_request(self, url, query, headers, format="JSON", params=None, verbose=False,
                     only_rows=True, full_response=False, timeout=None):
        if params is None:
            params = {}
        # Make some improvements to query: strip trailing semicolon, add format if needed.
        query = query.strip()
        assert "format" not in query.lower()
        query_type = query.strip().split(" ", 1)[0]
        if query.endswith(";"):
            query = query[:-1]
        output_present = query_type.lower() in QUERY_TYPES_WITH_OUTPUT
        if output_present:
            query = query + " format " + format

        params["output_format_json_quote_64bit_integers"] = 0

        result = requests.post(url, data=query, headers=headers, params=params, timeout=timeout, verify=False)

        inner_errors = []

        output = ""
        if result.status_code != 200:
            output += "Query failed, HTTP code: {}\n".format(result.status_code)
            if "X-Yt-Error" in result.headers:
                error = YtError(**json.loads(result.headers["X-Yt-Error"]))
            else:
                error = self._parse_error(result.text)

            if error:
                output += "Error: {}\n".format(error)
                inner_errors.append(error)
            else:
                output += "Cannot parse error from response data; full response is listed below.\n"
                verbose = True

        if verbose:
            output += "Headers:\n"
            output += json.dumps(dict(result.headers)) + "\n"
            output += "Data:\n"
            output += result.text

        print_debug(output)

        if full_response:
            return result

        if result.status_code != 200:
            raise YtError(
                message="ClickHouse query failed. Output:\n" + output,
                attributes={"query": query, "query_id": result.headers.get("query_id", "(n/a)")},
                code=QueryFailedError,
                inner_errors=inner_errors,
            )
        else:
            if output_present:
                if format == "JSON":
                    result = result.json()
                if only_rows and format == "JSON":
                    result = result["data"]
                return result
            else:
                return None

    def make_direct_query(
            self,
            instance,
            query,
            settings=None,
            user="root",
            format="JSON",
            verbose=True,
            only_rows=True,
            full_response=False,
            timeout=None,
            headers=None,
    ):
        host = instance.attributes["host"]
        port = instance.attributes["http_port"]

        query_id = parts_to_uuid(random.randint(0, 2 ** 64 - 1), (Clique.clique_index << 32) | Clique.query_index)
        Clique.query_index += 1

        print_debug()
        print_debug("Querying {0}:{1} with the following data:\n> {2}".format(host, port, query))
        print_debug("Query id: {}".format(query_id))

        if headers is None:
            headers = {}

        headers["X-ClickHouse-User"] = user
        headers["X-Yt-Trace-Id"] = query_id
        headers["X-Yt-Span-Id"] = "0"
        headers["X-Yt-Sampled"] = str(int(self.is_tracing))

        try:
            return self.make_request(
                "http://{}:{}/query".format(host, port),
                query,
                params=settings,
                format=format,
                verbose=verbose,
                only_rows=only_rows,
                full_response=full_response,
                headers=headers,
                timeout=timeout,
            )
        except requests.ConnectionError as err:
            print_debug("Caught network level error: ", err)
            errors = [YtError("ConnectionError: " + str(err))]
            stderr = "(n/a)"
            print_debug("Waiting for instance {} to finish".format(str(instance)))
            try:
                wait(
                    lambda: get(
                        self.op.get_path() + "/controller_orchid/running_jobs/" + str(instance) + "/state", default=None
                    ) != "running"
                )
                print_debug("Instance {} has failed".format(str(instance)))
                stderr = self.op.read_stderr(str(instance)).decode()
                if verbose:
                    print_debug("Stderr:\n" + stderr)
            except YtError as err2:
                errors.append(err2)
            raise YtError("Instance unavailable, stderr:\n" + stderr, inner_errors=errors, code=InstanceUnavailableCode)

    def make_query_via_proxy(
        self,
        query,
        format="JSON",
        settings=None,
        verbose=True,
        only_rows=True,
        full_response=False,
        headers=None,
        database=None,
        user="root",
        endpoint="/chyt",
        chyt_proxy=False,
        https_proxy=False,
        session_id: str | None = None,
    ):
        """
        chyt_proxy:
            Use special chyt-proxy instead of regular one.
            Note: at this moment the only valid for chyt-proxy endpoint is "/".
        https_proxy:
            Use https proxy instead of http one.
        """

        if headers is None:
            headers = {}
        headers["X-Yt-User"] = user

        if chyt_proxy:
            address = (self.chyt_https_address if https_proxy
                       else self.chyt_http_address)
        else:
            address = (self.proxy_https_address if https_proxy
                       else self.proxy_address)

        assert address is not None
        url = address + endpoint

        if database is None:
            database = self.alias

        params = {"database": database}

        if session_id is not None:
            params["session_id"] = session_id

        if settings is not None:
            update_inplace(params, settings)

        print_debug()
        print_debug("Querying proxy {0} with the following data:\n> {1}".format(url, query))
        return self.make_request(
            url,
            query,
            headers,
            params=params,
            format=format,
            verbose=verbose,
            only_rows=only_rows,
            full_response=full_response,
        )

    def make_query(
            self,
            query,
            user="root",
            format="JSON",
            settings=None,
            verbose=True,
            only_rows=True,
            full_response=False,
            timeout=None,
            headers=None,
    ):
        instances = self.get_active_instances()
        assert len(instances) > 0
        instance = random.choice(instances)
        return self.make_direct_query(
            instance,
            query,
            settings=settings,
            user=user,
            format=format,
            verbose=verbose,
            only_rows=only_rows,
            full_response=full_response,
            timeout=timeout,
            headers=headers,
        )

    def make_async_query(self, *args, **kwargs):
        t = Thread(target=self.make_query, args=args, kwargs=kwargs)
        t.start()
        return t

    def make_async_query_via_proxy(self, *args, **kwargs):
        t = Thread(target=self.make_query_via_proxy, args=args, kwargs=kwargs)
        t.start()
        return t

    def get_orchid(self, instance, path, verbose=True):
        orchid_path = "//sys/clickhouse/orchids/{}/{}".format(self.op.id, instance.attributes["job_cookie"])
        return get(orchid_path + path, verbose=verbose)

    def get_profiler(self, instance_id=None):
        if not instance_id:
            instance_id = self.get_active_instances()[0].attributes["job_cookie"]

        yt_client = YtClient(config={
            "enable_token": False,
            "backend": "native",
            "driver_config": get_driver().get_config(),
        })
        sensors_path = "//sys/clickhouse/orchids/{}/{}/sensors".format(self.op.id, instance_id)

        return Profiler(yt_client, sensors_path)

    def get_profiler_counter(self, sensor, instance_id=None):
        return self.get_profiler(instance_id).counter(sensor)

    def get_profiler_gauge(self, sensor, instance_id=None):
        return self.get_profiler(instance_id).gauge(sensor)

    def resize(self, size):
        update_op_parameters(self.op.id, parameters=get_scheduling_options(user_slots=size))
        wait(lambda: self.get_active_instance_count() == size)

    def get_clique_id(self):
        return self.op.id

    def wait_instance_count(self, instance_count, unwanted_jobs=[], wait_discovery_sync=False):
        unwanted_jobs = [str(job) for job in unwanted_jobs]

        def check():
            instances = sorted(self.get_active_instances())
            job_ids = [str(instance) for instance in instances]
            if len(instances) != instance_count:
                return False
            for instance in instances:
                if str(instance) in unwanted_jobs:
                    return False
            if wait_discovery_sync:
                for instance in instances:
                    result = self.make_direct_query(instance, "select job_id from system.clique order by job_id")
                    result = [instance["job_id"] for instance in result]
                    if result != job_ids:
                        return False
            return True

        wait(check)


class ClickHouseTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    NUM_DISCOVERY_SERVERS = 1
    NODE_PORT_SET_SIZE = 25
    USE_DYNAMIC_TABLES = True

    ENABLE_HTTP_PROXY = True
    ENABLE_CHYT_HTTP_PROXIES = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "job_environment": {
                    "memory_watchdog_period": 100,
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "memory": 10 * 2 ** 30,
            },
        }
    }

    DELTA_PROXY_CONFIG = {
        "clickhouse": {
            "discovery_cache": {
                "soft_age_threshold": 500,
                "hard_age_threshold": 1500,
                "master_cache_expire_time": 500,
            },
            "operation_cache": {
                "expire_after_successful_update_time": 0,
                "refresh_time": yson.YsonEntity(),
            },
        },
    }

    @classmethod
    def _get_proxy_address(cls):
        return "http://" + cls.Env.get_http_proxy_address()

    @classmethod
    def _get_proxy_https_address(cls):
        return "https://" + cls.Env.get_http_proxy_address(https=True)

    @classmethod
    def _get_chyt_http_address(cls):
        return "http://" + cls.Env.get_http_proxy_address(chyt=True)

    @classmethod
    def _get_chyt_https_address(cls):
        return "https://" + cls.Env.get_http_proxy_address(chyt=True, https=True)

    @staticmethod
    def _signal_instance(pid, signal_number):
        print_debug("Killing instance with with os.kill({}, {})".format(pid, signal_number))
        os.kill(pid, signal_number)

    @classmethod
    def setup_class(cls, test_name=None, run_id=None):
        super().setup_class(test_name=test_name, run_id=run_id)
        Clique.path_to_run = cls.path_to_run
        Clique.core_dump_path = os.path.join(cls.path_to_run, "core_dumps")
        if not os.path.exists(Clique.core_dump_path):
            os.mkdir(Clique.core_dump_path)
            os.chmod(Clique.core_dump_path, 0o777)

        if exists("//sys/clickhouse"):
            return
        create("map_node", "//sys/clickhouse")
        create("document", "//sys/clickhouse/config", attributes={"value": {}})
        create("map_node", "//sys/clickhouse/breakpoints")

        # We need to inject cluster_connection into yson config.
        Clique.base_config = get_clickhouse_server_config()
        # COMPAT(max42): see get_clickhouse_server_config() compats.
        Clique.base_config["clickhouse"] = Clique.base_config["engine"]
        del Clique.base_config["engine"]
        Clique.base_config["cluster_connection"] = copy.deepcopy(cls.Env.configs["driver"])
        if "tvm_service" in Clique.base_config["cluster_connection"]:
            Clique.base_config["native_authentication_manager"] = {
                "tvm_service": Clique.base_config["cluster_connection"].pop("tvm_service"),
                "enable_validation": True,
            }
            Clique.base_config["native_authentication_manager"]["tvm_service"].pop("client_dst_map")
            Clique.tvm_secret = Clique.base_config["native_authentication_manager"]["tvm_service"].pop("client_self_secret")

        Clique.proxy_address = cls._get_proxy_address()
        Clique.chyt_http_address = cls._get_chyt_http_address()

    def setup_method(self, method):
        super().setup_method(method)

        create_user("yt-clickhouse-cache")
        create_user("yt-clickhouse")

        create_user("chyt-sql-objects")
        yt_set("//sys/accounts/sys/@acl/end", make_ace("allow", "chyt-sql-objects", "use"))

        sync_create_cells(1)
