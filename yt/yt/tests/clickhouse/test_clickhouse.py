from yt_commands import *
from yt_helpers import *

from yt_env_setup import wait, YTEnvSetup, is_asan_build
from yt.clickhouse import get_clique_spec_builder
from yt.wrapper.common import simplify_structure
import yt.packages.requests as requests
from yt.packages.six.moves import map as imap

import yt.environment.init_operation_archive as init_operation_archive

from yt.environment.helpers import assert_items_equal

import yt.yson as yson

from yt.common import update, parts_to_uuid

from threading import Thread

import copy
import json
import os
import os.path
import pytest
import random
import threading
import pprint
import itertools

from yt.clickhouse.test_helpers import get_host_paths, get_clickhouse_server_config, get_log_tailer_config

from flaky import flaky

HOST_PATHS = get_host_paths(arcadia_interop, ["ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer"])

DEFAULTS = {
    "memory_config": {
        "footprint": 1 * 1024 ** 3,
        "clickhouse": int(2.5 * 1024 ** 3),
        "reader": 1 * 1024 ** 3,
        "uncompressed_block_cache": 0,
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

QUERY_TYPES_WITH_OUTPUT = ("describe", "select", "show", "exists")

UserJobFailed = 1205
QueryFailedError = 2200
InstanceUnavailableCode = 2201


class Clique(object):
    base_config = None
    clique_index = 0
    query_index = 0
    path_to_run = None
    core_dump_path = None
    proxy_address = None

    def __init__(self, instance_count, max_failed_job_count=0, config_patch=None, cpu_limit=None, **kwargs):
        config = (
            update(Clique.base_config, config_patch) if config_patch is not None else copy.deepcopy(Clique.base_config)
        )
        spec = {"pool": None}
        self.is_tracing = False
        if "YT_TRACE_DUMP_DIR" in os.environ:
            self.is_tracing = True
            spec["tasks"] = {"instances": {"environment": {"YT_TRACE_DUMP_DIR": os.environ["YT_TRACE_DUMP_DIR"]}}}
        if "spec" in kwargs:
            spec = update(spec, kwargs.pop("spec"))

        self.log_root = os.path.join(self.path_to_run, "logs", "clickhouse-{}".format(Clique.clique_index))
        self.stderr_root = os.path.join(self.path_to_run, "stderrs", "clickhouse-{}".format(Clique.clique_index))
        for root in (self.log_root, self.stderr_root):
            try:
                os.makedirs(root)
            except FileExistsError:
                pass
            os.chmod(root, 0777)

        for writer_key, writer in config["logging"]["writers"].iteritems():
            if writer["type"] == "file":
                writer["file_name"] = os.path.join(self.log_root, writer["file_name"])

        filename = "//sys/clickhouse/config-{}.yson".format(Clique.clique_index)
        Clique.clique_index += 1
        create("file", filename)
        write_file(filename, yson.dumps(config, yson_format="pretty"))

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
            **kwargs
        )
        self.spec = simplify_structure(spec_builder.build())
        if not is_asan_build() and core_dump_destination is not None:
            self.spec["tasks"]["instances"]["force_core_dump"] = True
        self.instance_count = instance_count

    def get_active_instances(self):
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

        MAX_COUNTER_VALUE = 600
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
            elif counter >= MAX_COUNTER_VALUE:
                raise YtError(
                    "Clique did not start in time, clique directory: {}".format(
                        get("//sys/clickhouse/cliques/{0}".format(self.op.id), verbose=False)
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
        except YtError as err:
            print_debug("Error while completing clique operation:", err)
        clique_error = None
        try:
            self.op.track()
        except YtError as err:
            clique_error = err

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

    def make_request(
        self, url, query, headers, format="JSON", params=None, verbose=False, only_rows=True, full_response=False
    ):
        if params is None:
            params = {}
        # Make some improvements to query: strip trailing semicolon, add format if needed.
        query = query.strip()
        assert "format" not in query.lower()
        query_type = query.strip().split(" ", 1)[0]
        if query.endswith(";"):
            query = query[:-1]
        output_present = query_type in QUERY_TYPES_WITH_OUTPUT
        if output_present:
            query = query + " format " + format

        params["output_format_json_quote_64bit_integers"] = 0

        result = requests.post(url, data=query, headers=headers, params=params)

        inner_errors = []

        output = ""
        if result.status_code != 200:
            output += "Query failed, HTTP code: {}\n".format(result.status_code)
            if "X-Yt-Error" in result.headers:
                error = YtError(**json.loads(result.headers["X-Yt-Error"]))
            else:
                error = self._parse_error(result.content)

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
            output += result.content

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
    ):
        host = instance.attributes["host"]
        port = instance.attributes["http_port"]

        query_id = parts_to_uuid(random.randint(0, 2 ** 64 - 1), (Clique.clique_index << 32) | Clique.query_index)
        Clique.query_index += 1

        print_debug()
        print_debug("Querying {0}:{1} with the following data:\n> {2}".format(host, port, query))
        print_debug("Query id: {}".format(query_id))

        try:
            return self.make_request(
                "http://{}:{}/query".format(host, port),
                query,
                params=settings,
                format=format,
                verbose=verbose,
                only_rows=only_rows,
                full_response=full_response,
                headers={
                    "X-ClickHouse-User": user,
                    "X-Yt-Trace-Id": query_id,
                    "X-Yt-Span-Id": "0",
                    "X-Yt-Sampled": str(int(self.is_tracing)),
                },
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
                    )
                    != "running"
                )
                print_debug("Instance {} has failed".format(str(instance)))
                wait(lambda: exists(self.op.get_path() + "/jobs/" + str(instance) + "/stderr"))
                stderr = read_file(self.op.get_path() + "/jobs/" + str(instance) + "/stderr", verbose=False)
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
    ):
        if headers is None:
            headers = {}
        headers["X-Yt-User"] = user
        assert self.proxy_address is not None
        url = self.proxy_address + "/query"
        if database is None:
            database = self.op.id
        params = {"database": database}
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
        self, query, user="root", format="JSON", settings=None, verbose=True, only_rows=True, full_response=False
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

    def resize(self, size, jobs_to_abort=[]):
        update_op_parameters(self.op.id, parameters=get_scheduling_options(user_slots=size))
        for job in jobs_to_abort:
            abort_job(job)
        wait(lambda: self.get_active_instance_count() == size)


class ClickHouseTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    NODE_PORT_SET_SIZE = 25
    USE_DYNAMIC_TABLES = True

    ENABLE_HTTP_PROXY = True

    DELTA_PROXY_CONFIG = {
        "clickhouse": {
            "discovery_cache": {
                "soft_age_threshold": 500,
                "hard_age_threshold": 1500,
                "master_cache_expire_time": 500,
            },
            "force_enqueue_profiling": True,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "memory_watchdog_period": 100,
                },
            },
            "job_controller": {
                "resource_limits": {
                    "memory": 10 * 2 ** 30,
                },
            },
        }
    }

    def _get_proxy_address(self):
        return "http://" + self.Env.get_http_proxy_address()

    def _signal_instance(self, pid, signal):
        kill_cmd = "kill -s {0} {1}".format(signal, pid)
        # Try to execute `kill_cmd` both with sudo and without sudo.
        # To prevent freeze on sudo password reading we pass fake empty
        # password through stdin.
        cmd = "echo | sudo -S {0} || {0}".format(kill_cmd)
        print_debug("Killing instance with {0}".format(cmd))
        os.system(cmd)

    def _setup(self):
        Clique.path_to_run = self.path_to_run
        Clique.core_dump_path = os.path.join(self.path_to_run, "core_dumps")
        if not os.path.exists(Clique.core_dump_path):
            os.mkdir(Clique.core_dump_path)
            os.chmod(Clique.core_dump_path, 0o777)

        create_user("yt-clickhouse-cache")
        create_user("yt-clickhouse")
        sync_create_cells(1)

        if exists("//sys/clickhouse"):
            return
        create("map_node", "//sys/clickhouse")
        create("document", "//sys/clickhouse/config", attributes={"value": {}})

        # We need to inject cluster_connection into yson config.
        Clique.base_config = get_clickhouse_server_config()
        # COMPAT(max42): see get_clickhouse_server_config() compats.
        Clique.base_config["clickhouse"] = Clique.base_config["engine"]
        del Clique.base_config["engine"]
        Clique.base_config["cluster_connection"] = self.__class__.Env.configs["driver"]
        Clique.proxy_address = self._get_proxy_address()


def get_scheduling_options(user_slots):
    return {"scheduling_options_per_pool_tree": {"default": {"resource_limits": {"user_slots": user_slots}}}}


# TODO(max42): rework this.
def get_async_expiring_cache_config(expire_after_access_time, expire_after_successful_update_time, refresh_time):
    return {
        "expire_after_access_time": expire_after_access_time,
        "expire_after_successful_update_time": expire_after_successful_update_time,
        "refresh_time": refresh_time,
    }


def get_object_attibute_cache_config(expire_after_access_time, expire_after_successful_update_time, refresh_time):
    return {
        "yt": {
            "table_attribute_cache": get_async_expiring_cache_config(
                expire_after_access_time, expire_after_successful_update_time, refresh_time
            ),
            "permission_cache": get_async_expiring_cache_config(
                expire_after_access_time, expire_after_successful_update_time, refresh_time
            ),
        },
    }


def get_disabled_cache_config():
    disabled_cache_config = {
        "read_from": "follower",
        "expire_after_successful_update_time": 0,
        "expire_after_failed_update_time": 0,
        "refresh_time": 0,
        "expire_after_access_time": 0,
    }
    return {
        "yt": {
            "table_attribute_cache": disabled_cache_config,
            "permission_cache": disabled_cache_config,
        }
    }


def get_schema_from_description(describe_info):
    schema = []
    for info in describe_info:
        schema.append(
            {
                "name": info["name"],
                "type": info["type"],
            }
        )
    return schema


class TestClickHouseCommon(ClickHouseTestBase):
    def setup(self):
        self._setup()

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "cpu": 2,
                }
            }
        }
    }

    @authors("evgenstf")
    def test_show_tables(self):
        tables = ["/t11", "/t12", "/n1/t3", "/n1/t4"]

        def create_subtrees(root):
            create("map_node", root)
            create("map_node", root + "/n1")
            for table in tables:
                create("table", root + table, attributes={"schema": [{"name": "a", "type": "string"}]})

        roots = ["//tmp/root1", "//tmp/root2"]
        for root in roots:
            create_subtrees(root)

        with Clique(1, config_patch={"yt": {"show_tables": {"roots": roots}}}) as clique:
            shown_tables = {table["name"] for table in clique.make_query("show tables")}
            for root in roots:
                for table in tables:
                    assert root + table in shown_tables
                    shown_tables.remove(root + table)
            assert len(shown_tables) == 0

            shown_tables_like_t1 = {table["name"] for table in clique.make_query("show tables like '%t1%'")}
            for root in roots:
                assert root + "/t11" in shown_tables_like_t1
                shown_tables_like_t1.remove(root + "/t11")

                assert root + "/t12" in shown_tables_like_t1
                shown_tables_like_t1.remove(root + "/t12")

        with raises_yt_error(UserJobFailed):
            with Clique(
                1,
                config_patch={
                    "yt": {"show_tables": {"roots": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"]}}
                },
            ) as clique:
                pass

    @authors("evgenstf")
    def test_subquery_columnar_data_weight(self):
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]},
        )
        write_table("//tmp/t", [{"a": "2012-12-12 20:00:00", "b": "2012-12-12 20:00:00"}])
        column_weight = get_table_columnar_statistics('["//tmp/t{a,b}"]')[0]["column_data_weights"]["a"]

        with Clique(
            1, config_patch={"yt": {"subquery": {"max_data_weight_per_subquery": column_weight - 1}}}
        ) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('select a from "//tmp/t"')
            with raises_yt_error(QueryFailedError):
                clique.make_query('select b from "//tmp/t"')

        with Clique(
            1, config_patch={"yt": {"subquery": {"max_data_weight_per_subquery": column_weight + 1}}}
        ) as clique:
            clique.make_query('select a from "//tmp/t"') == [{"a": "2012-12-12 20:00:00"}]
            with raises_yt_error(QueryFailedError):
                clique.make_query('select a, b from "//tmp/t"')

        with Clique(
            1,
            config_patch={
                "yt": {
                    "subquery": {"use_columnar_statistics": False, "max_data_weight_per_subquery": column_weight + 1}
                }
            },
        ) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('select a from "//tmp/t"') == [{"a": "2012-12-12 20:00:00"}]
            with raises_yt_error(QueryFailedError):
                clique.make_query('select a, b from "//tmp/t"')

    @authors("evgenstf")
    def test_extract_array_raw(self):
        with Clique(1) as clique:
            assert clique.make_query('select YSONExtractArrayRaw(\'["a";"0";""]\')') == [
                {'YSONExtractArrayRaw(\'["a";"0";""]\')': ['"a"', '"0"', '""']}
            ]
            assert clique.make_query('select JSONExtractArrayRaw(\'["a","0",""]\')') == [
                {'JSONExtractArrayRaw(\'["a","0",""]\')': ['"a"', '"0"', '""']}
            ]

    @authors("evgenstf")
    def test_timezone(self):
        create("table", "//tmp/test_table", attributes={"schema": [{"name": "date_time", "type": "datetime"}]})
        write_table("//tmp/test_table", [{"date_time": 100}])

        with Clique(1) as clique:
            assert clique.make_query("select timezone()") == [{"timezone()": "Europe/Moscow"}]
            assert clique.make_query('select date_time from "//tmp/test_table"') == [
                {"date_time": "1970-01-01 03:01:40"}
            ]

        with Clique(1, config_patch={"clickhouse": {"timezone": "America/Los_Angeles"}}) as clique:
            assert clique.make_query("select timezone()") == [{"timezone()": "America/Los_Angeles"}]
            assert clique.make_query('select date_time from "//tmp/test_table"') == [
                {"date_time": "1970-01-01 09:01:40"}
            ]

    @authors("evgenstf")
    def test_not_table_in_query(self):
        with Clique(1) as clique:
            table_schema = [{"name": "value", "type": "int64"}]
            create("table", "//tmp/test_table", attributes={"schema": table_schema})
            write_table("//tmp/test_table", [{"value": 1}])

            # TODO(evgenstf): CHYT-112 - use error code instead of the substring
            with raises_yt_error("point to attributes"):
                clique.make_query('select * from "//tmp/test_table/@schema"')

    @authors("evgenstf")
    def test_distinct_one_instance_several_threads(self):
        with Clique(1, config_patch={"clickhouse": {"settings": {"max_threads": 2}}}, cpu_limit=2) as clique:
            table_schema = [{"name": "value", "type": "int64"}]
            create("table", "//tmp/test_table", attributes={"schema": table_schema})

            write_table("//tmp/test_table", [{"value": 1}])
            write_table("<append=%true>//tmp/test_table", [{"value": 1}])

            assert get("//tmp/test_table/@chunk_count") == 2
            assert clique.make_query('select distinct value from "//tmp/test_table" where value = 1') == [{"value": 1}]

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for, required", itertools.product(["lookup", "scan"], [False, True]))
    def test_prewhere_actions(self, optimize_for, required):
        with Clique(1) as clique:
            create(
                "table",
                "//tmp/t1",
                attributes={
                    "schema": [{"name": "value", "type": "int64", "required": required}],
                    "optimize_for": optimize_for,
                },
            )
            write_table("//tmp/t1", [{"value": 0}, {"value": 1}, {"value": 2}, {"value": 3}])

            assert clique.make_query('select count() from "//tmp/t1"') == [{"count()": 4}]
            assert clique.make_query('select count() from "//tmp/t1" prewhere (value < 3)') == [{"count()": 3}]
            assert clique.make_query('select count(*) from "//tmp/t1" prewhere (value < 3)') == [{"count()": 3}]
            assert clique.make_query('select count(value) from "//tmp/t1" prewhere (value < 3)') == [
                {"count(value)": 3}
            ]
            assert clique.make_query('select count() from "//tmp/t1" prewhere (value < 3)') == [{"count()": 3}]
            assert clique.make_query('select any(0) from "//tmp/t1" prewhere (value < 3)') == [{"any(0)": 0}]

            create(
                "table",
                "//tmp/t2",
                attributes={
                    "schema": [
                        {"name": "key", "type": "int64", "required": required},
                        {"name": "value", "type": "string", "required": required},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            write_table(
                "//tmp/t2",
                [
                    {"key": 0, "value": "aaa"},
                    {"key": 1, "value": "bbb"},
                    {"key": 2, "value": "bbb"},
                    {"key": 3, "value": "ddd"},
                ],
            )
            assert clique.make_query(
                'select value from "//tmp/t2" prewhere key in (select key from "//tmp/t2" where value = \'bbb\')'
            ) == [{"value": "bbb"}, {"value": "bbb"}]

    @authors("evgenstf")
    def test_acl(self):
        with Clique(1) as clique:
            create_user("user_with_denied_column")
            create_user("user_with_allowed_one_column")
            create_user("user_with_allowed_all_columns")

            def create_and_fill_table(path):
                create(
                    "table",
                    path,
                    attributes={"schema": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]},
                    recursive=True,
                )
                write_table(path, [{"a": "value1", "b": "value2"}])

            create_and_fill_table("//tmp/t1")
            set(
                "//tmp/t1/@acl",
                [
                    make_ace("allow", "user_with_denied_column", "read"),
                    make_ace("deny", "user_with_denied_column", "read", columns="a"),
                ],
            )

            with raises_yt_error(QueryFailedError):
                clique.make_query('select * from "//tmp/t1"', user="user_with_denied_column")

            with raises_yt_error(QueryFailedError):
                clique.make_query('select a from "//tmp/t1"', user="user_with_denied_column")

            assert clique.make_query('select b from "//tmp/t1"', user="user_with_denied_column") == [{"b": "value2"}]

            create_and_fill_table("//tmp/t2")
            set(
                "//tmp/t2/@acl",
                [
                    make_ace("allow", "user_with_allowed_one_column", "read", columns="b"),
                    make_ace("allow", "user_with_allowed_all_columns", "read", columns="a"),
                    make_ace("allow", "user_with_allowed_all_columns", "read", columns="b"),
                ],
            )

            with raises_yt_error(QueryFailedError):
                clique.make_query('select * from "//tmp/t2"', user="user_with_allowed_one_column")
            with raises_yt_error(QueryFailedError):
                clique.make_query('select a from "//tmp/t2"', user="user_with_allowed_one_column")
            assert clique.make_query('select b from "//tmp/t2"', user="user_with_allowed_one_column") == [
                {"b": "value2"}
            ]
            assert clique.make_query('select * from "//tmp/t2"', user="user_with_allowed_all_columns") == [
                {"a": "value1", "b": "value2"}
            ]
            assert clique.make_query('select b from "//tmp/t2"', user="user_with_allowed_one_column") == [
                {"b": "value2"}
            ]
            assert clique.make_query('select * from "//tmp/t2"', user="user_with_allowed_all_columns") == [
                {"a": "value1", "b": "value2"}
            ]
            assert clique.make_query('select b from "//tmp/t2"', user="user_with_allowed_one_column") == [
                {"b": "value2"}
            ]
            assert clique.make_query('select * from "//tmp/t2"', user="user_with_allowed_all_columns") == [
                {"a": "value1", "b": "value2"}
            ]

            time.sleep(1.5)

            assert clique.make_query('select b from "//tmp/t2"', user="user_with_allowed_one_column") == [
                {"b": "value2"}
            ]
            assert clique.make_query('select * from "//tmp/t2"', user="user_with_allowed_all_columns") == [
                {"a": "value1", "b": "value2"}
            ]

            time.sleep(0.5)

            assert (
                clique.get_orchid(
                    clique.get_active_instances()[0], "/profiling/clickhouse/yt/object_attribute_cache/hit"
                )[-1]["value"]
                > 0
            )
            assert (
                clique.get_orchid(clique.get_active_instances()[0], "/profiling/clickhouse/yt/permission_cache/hit")[
                    -1
                ]["value"]
                > 0
            )

    @authors("evgenstf")
    def test_orchid_error_handle(self):
        if not exists("//sys/clickhouse/orchids"):
            create("map_node", "//sys/clickhouse/orchids")

        create_user("test_user")
        set(
            "//sys/clickhouse/@acl",
            [
                make_ace("allow", "test_user", ["write", "create", "remove", "modify_children"]),
            ],
        )
        set("//sys/accounts/sys/@acl", [make_ace("allow", "test_user", "use")])

        set(
            "//sys/clickhouse/orchids/@acl",
            [
                make_ace("deny", "test_user", "create"),
            ],
        )

        with pytest.raises(YtError):
            with Clique(1, config_patch={"yt": {"user": "test_user"}}) as clique:
                pass

    @authors("evgenstf")
    def test_orchid_nodes(self):
        node_to_ban = None
        try:
            with Clique(3) as clique:
                for i in range(3):
                    assert "monitoring" in clique.get_orchid(clique.get_active_instances()[i], "/")

                job_to_abort = str(clique.get_active_instances()[0])
                node_to_ban = clique.op.get_node(job_to_abort)

                abort_job(job_to_abort)
                set_banned_flag(True, [node_to_ban])

                def instances_relocated():
                    active_instances = [str(instance) for instance in clique.get_active_instances()]
                    if len(active_instances) != 3:
                        return False
                    return job_to_abort not in active_instances

                wait(instances_relocated)

                for i in range(3):
                    assert "monitoring" in clique.get_orchid(clique.get_active_instances()[i], "/")
        finally:
            if node_to_ban is not None:
                set_banned_flag(False, [node_to_ban])

    @authors("evgenstf")
    def test_drop_nonexistent_table(self):
        patch = get_object_attibute_cache_config(500, 500, None)
        with Clique(1, config_patch=patch) as clique:
            assert exists("//tmp/t") == False
            assert clique.make_query('exists "//tmp/t"') == [{"result": 0}]
            with raises_yt_error(QueryFailedError):
                assert clique.make_query('drop table "//tmp/t"')

    @authors("evgenstf")
    def test_drop_table(self):
        patch = get_object_attibute_cache_config(500, 500, None)
        with Clique(1, config_patch=patch) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])
            assert clique.make_query('select * from "//tmp/t"') == [{"a": "2012-12-12 20:00:00"}]
            clique.make_query('drop table "//tmp/t"')
            time.sleep(1)
            assert exists("//tmp/t") == False
            assert clique.make_query('exists "//tmp/t"') == [{"result": 0}]

    @authors("evgenstf")
    def test_subquery_data_weight_limit_exceeded(self):
        with Clique(1, config_patch={"yt": {"subquery": {"max_data_weight_per_subquery": 0}}}) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])
            with raises_yt_error(QueryFailedError):
                clique.make_query('select CAST(a as datetime) from "//tmp/t"')

    @authors("evgenstf")
    def test_discovery_nodes_self_cleaning(self):
        patch = {
            "yt": {
                "discovery": {
                    # Allow node cleaning 1s after creation (default is 5m).
                    "lock_node_timeout": 1000,
                }
            }
        }
        with Clique(5, config_patch=patch) as clique:
            clique_path = "//sys/clickhouse/cliques/{0}".format(clique.op.id)

            nodes_before_resizing = ls(clique_path, verbose=False)
            assert len(nodes_before_resizing) == 5

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 5

            clique.resize(3, jobs[:2])
            wait(lambda: len(ls(clique_path, verbose=False)) == 3, iter=10)

    @authors("evgenstf")
    def test_discovery_transaction_restore(self):
        with Clique(1) as clique:
            instances_before_transaction_abort = clique.get_active_instances()
            assert len(instances_before_transaction_abort) == 1

            locks = instances_before_transaction_abort[0].attributes["locks"]
            assert len(locks) == 1

            transaction_id = locks[0]["transaction_id"]

            abort_transaction(transaction_id)
            time.sleep(5)

            wait(lambda: clique.get_active_instance_count() == 1, iter=10)

    @authors("max42")
    @pytest.mark.parametrize("instance_count", [1, 5])
    def test_avg(self, instance_count):
        with Clique(instance_count) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"a": 2 * i}, {"a": 2 * i + 1}])

            assert abs(clique.make_query('select avg(a) from "//tmp/t"')[0]["avg(a)"] - 4.5) < 1e-6
            with raises_yt_error(QueryFailedError):
                clique.make_query('select avg(b) from "//tmp/t"')

            assert abs(clique.make_query('select avg(a) from "//tmp/t[#2:#9]"')[0]["avg(a)"] - 5.0) < 1e-6

    # YT-9497
    @authors("max42")
    def test_aggregation_with_multiple_string_columns(self):
        with Clique(1) as clique:
            create(
                "table",
                "//tmp/t",
                attributes={
                    "schema": [
                        {"name": "key1", "type": "string"},
                        {"name": "key2", "type": "string"},
                        {"name": "value", "type": "int64"},
                    ]
                },
            )
            for i in range(5):
                write_table(
                    "<append=%true>//tmp/t",
                    [{"key1": "dream", "key2": "theater", "value": i * 5 + j} for j in range(5)],
                )
            total = 24 * 25 // 2

            result = clique.make_query('select key1, key2, sum(value) from "//tmp/t" group by key1, key2')
            assert result == [{"key1": "dream", "key2": "theater", "sum(value)": total}]

    @authors("max42")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_cast(self, instance_count):
        with Clique(instance_count) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
            write_table("//tmp/t", [{"a": "2012-12-12 20:00:00"}])

            result = clique.make_query('select CAST(a as datetime) from "//tmp/t"')
            assert result == [{"cast(a, 'datetime')": "2012-12-12 20:00:00"}]

    @authors("max42")
    def test_settings(self):
        with Clique(1) as clique:
            # I took some random option from the documentation and changed it in config.yson.
            # Let's see if it changed in internal table with settings.
            result = clique.make_query("select * from system.settings where name = 'max_temporary_non_const_columns'")
            assert result[0]["value"] == "1234"
            assert result[0]["changed"] == 1

    @authors("max42", "dakovalkov", "evgenstf")
    @pytest.mark.parametrize("remove_method", ["yt", "chyt"])
    def test_schema_caching(self, remove_method):
        patch = get_object_attibute_cache_config(2000, 2000, None)

        with Clique(1, config_patch=patch) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            write_table("//tmp/t", [{"a": 1}])
            old_description = clique.make_query('describe "//tmp/t"')
            assert old_description[0]["name"] == "a"
            if remove_method == "yt":
                remove("//tmp/t")
            else:
                clique.make_query('drop table "//tmp/t"')
            cached_description = clique.make_query('describe "//tmp/t"')
            assert cached_description == old_description
            create("table", "//tmp/t", attributes={"schema": [{"name": "b", "type": "int64"}]})
            write_table("//tmp/t", [{"b": 1}])
            time.sleep(5)
            new_description = clique.make_query('describe "//tmp/t"')
            assert new_description[0]["name"] == "b"

    @authors("dakovalkov")
    def test_cache_auto_update(self):
        # Will never expire.
        patch = get_object_attibute_cache_config(100500, 100500, 100)

        with Clique(1, config_patch=patch) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            write_table("//tmp/t", [{"a": 1}])
            old_description = clique.make_query('describe "//tmp/t"')
            assert old_description[0]["name"] == "a"

            remove("//tmp/t")
            time.sleep(0.5)
            with raises_yt_error(QueryFailedError):
                cached_description = clique.make_query('describe "//tmp/t"')

            create("table", "//tmp/t", attributes={"schema": [{"name": "b", "type": "int64"}]})
            write_table("//tmp/t", [{"b": 1}])
            time.sleep(0.5)

            new_description = clique.make_query('describe "//tmp/t"')
            assert new_description[0]["name"] == "b"

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_prewhere_one_chunk(self, optimize_for):
        with Clique(1) as clique:
            create(
                "table",
                "//tmp/table_1",
                attributes={
                    "schema": [
                        {"name": "i", "type": "int64"},
                        {"name": "j", "type": "int64"},
                        {"name": "k", "type": "int64"},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            write_table(
                "//tmp/table_1",
                [
                    {"i": 1, "j": 11, "k": 101},
                    {"i": 2, "j": 12, "k": 102},
                    {"i": 3, "j": 13, "k": 103},
                    {"i": 4, "j": 14, "k": 104},
                    {"i": 5, "j": 15, "k": 105},
                    {"i": 6, "j": 16, "k": 106},
                    {"i": 7, "j": 17, "k": 107},
                    {"i": 8, "j": 18, "k": 108},
                    {"i": 9, "j": 19, "k": 109},
                    {"i": 10, "j": 110, "k": 110},
                ],
            )
            assert clique.make_query('select i from "//tmp/table_1" prewhere j > 13 and j < 18 order by i') == [
                {"i": 4},
                {"i": 5},
                {"i": 6},
                {"i": 7},
            ]

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_prewhere_several_chunks(self, optimize_for):
        with Clique(1) as clique:
            create(
                "table",
                "//tmp/test_table",
                attributes={
                    "schema": [
                        {"name": "key", "type": "string"},
                        {"name": "index", "type": "int64"},
                        {"name": "data", "type": "string"},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            rows = [
                {"key": "b_key", "index": i, "data": "b" * 50}
                if i == 1234
                else {"key": "a_key", "data": "a" * 50 + str(random.randint(0, 1000000))}
                for i in range(10 * 10 * 1024)
            ]
            for i in range(10):
                write_table(
                    "<append=%true>//tmp/test_table",
                    rows[len(rows) * i // 10 : len(rows) * (i + 1) // 10],
                    table_writer={"block_size": 1024, "desired_chunk_size": 10 * 1024},
                )

            assert get("//tmp/test_table/@chunk_count") == 10
            assert clique.make_query("select index from \"//tmp/test_table\" prewhere key = 'b_key'") == [
                {"index": 1234}
            ]
            clique.make_query_and_validate_row_count(
                "select index from \"//tmp/test_table\" where key = 'b_key'", exact=102400
            )
            clique.make_query_and_validate_row_count(
                "select index from \"//tmp/test_table\" prewhere key = 'b_key'", exact=1
            )

    @authors("evgenstf")
    def test_concat_directory_with_mixed_objects(self):
        with Clique(1) as clique:
            create("map_node", "//tmp/test_dir")

            # static table
            create("table", "//tmp/test_dir/table_1", attributes={"schema": [{"name": "i", "type": "int64"}]})
            write_table("//tmp/test_dir/table_1", [{"i": 1}])

            # link to static table
            create("map_node", "//tmp/dir_with_static_table")
            create(
                "table", "//tmp/dir_with_static_table/table_2", attributes={"schema": [{"name": "i", "type": "int64"}]}
            )
            write_table("//tmp/dir_with_static_table/table_2", [{"i": 2}])
            create(
                "link",
                "//tmp/test_dir/link_to_table_2",
                attributes={"target_path": "//tmp/dir_with_static_table/table_2"},
            )

            # dynamic table
            sync_create_cells(1)
            create(
                "table",
                "//tmp/test_dir/table_3",
                attributes={"dynamic": True, "schema": [{"name": "i", "type": "int64"}]},
            )
            sync_mount_table("//tmp/test_dir/table_3")
            insert_rows("//tmp/test_dir/table_3", [{"i": 3}])

            # link to dynamic table
            create("map_node", "//tmp/dir_with_dynamic_table")
            create(
                "table",
                "//tmp/dir_with_dynamic_table/table_4",
                attributes={"dynamic": True, "schema": [{"name": "i", "type": "int64"}]},
            )
            sync_mount_table("//tmp/dir_with_dynamic_table/table_4", sync=True)
            insert_rows("//tmp/dir_with_dynamic_table/table_4", [{"i": 4}])
            create(
                "link",
                "//tmp/test_dir/link_to_table_4",
                attributes={"target_path": "//tmp/dir_with_dynamic_table/table_4"},
            )

            # map_node
            create("map_node", "//tmp/test_dir/map_node")

            # link to map_node
            create("map_node", "//tmp/dir_with_map_node")
            create("map_node", "//tmp/dir_with_map_node/map_node")
            create(
                "link",
                "//tmp/test_dir/link_to_map_node",
                attributes={"target_path": "//tmp/dir_with_map_node/map_node"},
            )

            # link to link to static table
            create(
                "table", "//tmp/dir_with_static_table/table_5", attributes={"schema": [{"name": "i", "type": "int64"}]}
            )
            write_table("//tmp/dir_with_static_table/table_5", [{"i": 5}])
            create("map_node", "//tmp/dir_with_link_to_static_table")
            create(
                "link",
                "//tmp/dir_with_link_to_static_table/link_to_table_5",
                attributes={"target_path": "//tmp/dir_with_static_table/table_5"},
            )
            create(
                "link",
                "//tmp/test_dir/link_to_link_to_table_5",
                attributes={"target_path": "//tmp/dir_with_link_to_static_table/link_to_table_5"},
            )

            assert clique.make_query("select * from concatYtTablesRange('//tmp/test_dir') order by i") == [
                {"i": 1},
                {"i": 2},
                {"i": 5},
            ]

    @authors("evgenstf")
    def test_concat_tables_filter_range(self):
        with Clique(1) as clique:
            create("map_node", "//tmp/test_dir")
            for table_index in range(1, 7):
                create(
                    "table",
                    "//tmp/test_dir/table_" + str(table_index),
                    attributes={"schema": [{"name": "i", "type": "int64"}]},
                )
                write_table("//tmp/test_dir/table_" + str(table_index), [{"i": table_index}])
            assert clique.make_query(
                "select * from concatYtTablesRange('//tmp/test_dir', 'table_2', 'table_5') order by i"
            ) == [{"i": 2}, {"i": 3}, {"i": 4}, {"i": 5}]

    @authors("evgenstf")
    def test_concat_tables_filter_regexp(self):
        with Clique(1) as clique:
            create("map_node", "//tmp/test_dir")
            create("table", "//tmp/test_dir/t1", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table_2", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table_3", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table_4", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table_5", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/t6", attributes={"schema": [{"name": "i", "type": "int64"}]})
            write_table("//tmp/test_dir/t1", [{"i": 1}])
            write_table("//tmp/test_dir/table_2", [{"i": 2}])
            write_table("//tmp/test_dir/table_3", [{"i": 3}])
            write_table("//tmp/test_dir/table_4", [{"i": 4}])
            write_table("//tmp/test_dir/table_5", [{"i": 5}])
            write_table("//tmp/test_dir/t6", [{"i": 6}])
            assert clique.make_query("select * from concatYtTablesRegexp('//tmp/test_dir', 'table_*') order by i") == [
                {"i": 2},
                {"i": 3},
                {"i": 4},
                {"i": 5},
            ]

    @authors("evgenstf")
    def test_concat_tables_filter_like(self):
        with Clique(1) as clique:
            create("map_node", "//tmp/test_dir")
            create("table", "//tmp/test_dir/t1", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table_3", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table.3", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table.4", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/table_4", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/test_dir/t6", attributes={"schema": [{"name": "i", "type": "int64"}]})
            write_table("//tmp/test_dir/t1", [{"i": 1}])
            write_table("//tmp/test_dir/table_3", [{"i": 2}])
            write_table("//tmp/test_dir/table.3", [{"i": 3}])
            write_table("//tmp/test_dir/table.4", [{"i": 4}])
            write_table("//tmp/test_dir/table_4", [{"i": 5}])
            write_table("//tmp/test_dir/t6", [{"i": 6}])
            assert clique.make_query("select * from concatYtTablesLike('//tmp/test_dir', 'table.*') order by i") == [
                {"i": 3},
                {"i": 4},
            ]

    @authors("max42")
    def test_concat_tables_inside_link(self):
        with Clique(1) as clique:
            create("map_node", "//tmp/dir")
            create("link", "//tmp/link", attributes={"target_path": "//tmp/dir"})
            create("table", "//tmp/link/t1", attributes={"schema": [{"name": "i", "type": "int64"}]})
            create("table", "//tmp/link/t2", attributes={"schema": [{"name": "i", "type": "int64"}]})
            write_table("//tmp/link/t1", [{"i": 0}])
            write_table("//tmp/link/t2", [{"i": 1}])
            assert len(clique.make_query("select * from concatYtTablesRange('//tmp/link')")) == 2

    @authors("dakovalkov")
    def test_system_clique(self):
        with Clique(3) as clique:
            time.sleep(1)
            instances = clique.get_active_instances()
            assert len(instances) == 3
            responses = []

            def get_clique_list(instance):
                clique_list = clique.make_direct_query(instance, "select * from system.clique")
                for item in clique_list:
                    assert item["self"] == (1 if str(instance) == item["job_id"] else 0)
                    del item["self"]
                return sorted(clique_list)

            for instance in instances:
                responses.append(get_clique_list(instance))
            assert len(responses[0]) == 3
            for node in responses[0]:
                assert "host" in node and "rpc_port" in node and "monitoring_port" in node
                assert "tcp_port" in node and "http_port" in node and "job_id" in node
            assert responses[0] == responses[1]
            assert responses[1] == responses[2]

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 3

            clique.resize(2, [jobs[0]])
            clique.resize(3)

            time.sleep(1)

            instances = clique.get_active_instances()
            assert len(instances) == 3
            responses2 = []
            for instance in instances:
                responses2.append(get_clique_list(instance))
            assert len(responses2[0]) == 3
            assert responses2[0] == responses2[1]
            assert responses2[1] == responses2[2]
            assert responses != responses2

    @authors("dakovalkov")
    def test_ban_nodes(self):
        patch = {
            "yt": {
                "discovery": {
                    # Set big value to prevent unlocking node.
                    "transaction_timeout": 1000000,
                }
            }
        }
        with Clique(2, config_patch=patch) as clique:
            time.sleep(1)
            old_instances = clique.get_active_instances()
            assert len(old_instances) == 2

            for instance in old_instances:
                assert len(clique.make_direct_query(instance, "select * from system.clique")) == 2

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 2
            abort_job(jobs[0])

            wait(lambda: clique.get_active_instance_count() == 3)

            time.sleep(1)

            instances = clique.get_active_instances()
            # One instance is dead, but the lock should be alive.
            assert len(instances) == 3

            for instance in instances:
                if instance in old_instances:
                    # Avoid sending request to the dead instance.
                    continue

                wait(lambda: len(clique.make_direct_query(instance, "select * from system.clique")) == 2)

    @authors("dakovalkov")
    def test_single_interrupt(self):
        patch = {
            "interruption_graceful_timeout": 1000,
        }
        with Clique(1, max_failed_job_count=2, config_patch=patch) as clique:
            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=0))
            instances = clique.get_active_instances()
            assert len(instances) == 1
            self._signal_instance(instances[0].attributes["pid"], "INT")
            time.sleep(0.5)
            assert len(clique.get_active_instances()) == 0
            assert clique.make_direct_query(instances[0], "select 1", full_response=True).status_code == 301
            time.sleep(0.8)
            with raises_yt_error(InstanceUnavailableCode):
                clique.make_direct_query(instances[0], "select 1")

            clique.resize(1)

            new_instances = clique.get_active_instances()
            assert len(new_instances) == 1
            assert new_instances != instances

    @authors("dakovalkov")
    def test_double_interrupt(self):
        patch = {
            "interruption_graceful_timeout": 10000,
        }
        with Clique(1, max_failed_job_count=2, config_patch=patch) as clique:
            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=0))
            instances = clique.get_active_instances()
            assert len(instances) == 1
            pid = instances[0].attributes["pid"]
            self._signal_instance(pid, "INT")
            time.sleep(0.2)
            assert len(clique.get_active_instances()) == 0
            assert clique.make_direct_query(instances[0], "select 1", full_response=True).status_code == 301
            self._signal_instance(pid, "INT")
            time.sleep(0.2)
            with raises_yt_error(InstanceUnavailableCode):
                clique.make_direct_query(instances[0], "select 1")

            clique.resize(1)

            new_instances = clique.get_active_instances()
            assert len(new_instances) == 1
            assert new_instances != instances

    @authors("dakovalkov")
    def test_long_query_interrupt(self):
        patch = {
            "interruption_graceful_timeout": 1000,
        }
        with Clique(1, max_failed_job_count=2, config_patch=patch) as clique:
            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=0))
            instances = clique.get_active_instances()
            assert len(instances) == 1

            def signal_job_later():
                time.sleep(0.3)
                self._signal_instance(instances[0].attributes["pid"], "INT")

            signal_thread = threading.Thread(target=signal_job_later)
            signal_thread.start()

            assert clique.make_direct_query(instances[0], "select sleep(3)") == [{"sleep(3)": 0}]
            time.sleep(0.3)
            with raises_yt_error(InstanceUnavailableCode):
                clique.make_direct_query(instances[0], "select 1")

            clique.resize(1)

            new_instances = clique.get_active_instances()
            assert len(new_instances) == 1
            assert new_instances != instances

    @authors("dakovalkov")
    def test_convert_yson(self):
        create(
            "table",
            "//tmp/table",
            attributes={"schema": [{"name": "i", "type": "any"}, {"name": "fmt", "type": "string"}]},
        )
        value1 = 1
        value2 = [1, 2]
        value3 = {"key": "value"}
        write_table(
            "//tmp/table",
            [
                {"i": value1, "fmt": "binary"},
                {"i": value2, "fmt": "pretty"},
                {"i": value3, "fmt": "text"},
                {"i": None, "fmt": "text"},
            ],
        )
        with Clique(1) as clique:
            value = {"key": [1, 2]}
            func = "ConvertYson('" + yson.dumps(value, yson_format="text") + "', 'pretty')"
            assert clique.make_query("select " + func) == [{func: yson.dumps(value, yson_format="pretty")}]
            func = "ConvertYson(NULL, 'text')"
            assert clique.make_query("select " + func) == [{func: None}]
            func = "ConvertYson(i, 'text')"
            assert clique.make_query("select " + func + ' from "//tmp/table"') == [
                {func: yson.dumps(value1, yson_format="text")},
                {func: yson.dumps(value2, yson_format="text")},
                {func: yson.dumps(value3, yson_format="text")},
                {func: None},
            ]
            func = "ConvertYson(i, fmt)"
            assert clique.make_query("select " + func + ' from "//tmp/table"') == [
                {func: yson.dumps(value1, yson_format="binary")},
                {func: yson.dumps(value2, yson_format="pretty")},
                {func: yson.dumps(value3, yson_format="text")},
                {func: None},
            ]
            with raises_yt_error(QueryFailedError):
                clique.make_query("select ConvertYson('{key=[1;2]}', NULL)")
            with raises_yt_error(QueryFailedError):
                clique.make_query("select ConvertYson('{key=[1;2]}', 'xxx')")
            with raises_yt_error(QueryFailedError):
                clique.make_query("select ConvertYson('{{{{', 'binary')")
            with raises_yt_error(QueryFailedError):
                clique.make_query("select ConvertYson(1, 'text')")

    @authors("dakovalkov")
    def test_reject_request(self):
        with Clique(1) as clique:
            instance = clique.get_active_instances()[0]

            host = instance.attributes["host"]
            port = instance.attributes["http_port"]
            query_id = parts_to_uuid(random.randint(0, 2 ** 64 - 1), random.randint(0, 2 ** 64 - 1))

            result = requests.post(
                "http://{}:{}/query?query_id={}".format(host, port, query_id),
                data="select 1",
                headers={"X-ClickHouse-User": "root", "X-Yt-Request-Id": query_id, "X-Clique-Id": "wrong-id"},
            )
            print_debug(result.content)
            assert result.status_code == 301

            result = requests.post(
                "http://{}:{}/query?query_id={}".format(host, port, query_id),
                data="select 1",
                headers={"X-ClickHouse-User": "root", "X-Yt-Request-Id": query_id, "X-Clique-Id": clique.op.id},
            )
            print_debug(result.content)
            assert result.status_code == 200

            self._signal_instance(instance.attributes["pid"], "INT")

            result = requests.post(
                "http://{}:{}/query?query_id={}".format(host, port, query_id),
                data="select 1",
                headers={"X-ClickHouse-User": "root", "X-Yt-Request-Id": query_id, "X-Clique-Id": clique.op.id},
            )
            print_debug(result.content)
            assert result.status_code == 301

    @authors("dakovalkov")
    def test_exists_table(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        with Clique(1) as clique:
            assert clique.make_query('exists table "//tmp/t1"') == [{"result": 1}]
            # Table doesn't exist.
            assert clique.make_query('exists table "//tmp/t123456"') == [{"result": 0}]
            # Not a table.
            with raises_yt_error(QueryFailedError):
                clique.make_query('exists table "//sys"')

    @authors("dakovalkov")
    def test_date_types(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "datetime", "type": "datetime"},
                    {"name": "date", "type": "date"},
                    {"name": "timestamp", "type": "timestamp"},
                    {"name": "interval_", "type": "interval"},
                ]
            },
        )
        write_table(
            "//tmp/t1",
            [
                {
                    "datetime": 1,
                    "date": 2,
                    "timestamp": 3,
                    "interval_": 4,
                },
            ],
        )
        with Clique(1) as clique:
            assert get_schema_from_description(clique.make_query('describe "//tmp/t1"')) == [
                {"name": "datetime", "type": "Nullable(DateTime)"},
                {"name": "date", "type": "Nullable(Date)"},
                # TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
                # {"name": "timestamp", "type": "Nullable(DateTime64)"},
                {"name": "timestamp", "type": "Nullable(UInt64)"},
                {"name": "interval_", "type": "Nullable(Int64)"},
            ]
            assert clique.make_query(
                "select toTimeZone(datetime, 'UTC') as datetime, date, timestamp, interval_ from \"//tmp/t1\""
            ) == [
                {
                    "datetime": "1970-01-01 00:00:01",
                    "date": "1970-01-03",
                    "timestamp": 3,
                    "interval_": 4,
                }
            ]
            clique.make_query('create table "//tmp/t2" engine YtTable() as select * from "//tmp/t1"')
            assert get_schema_from_description(get("//tmp/t2/@schema")) == [
                {"name": "datetime", "type": "datetime"},
                {"name": "date", "type": "date"},
                # TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
                # {"name": "timestamp", "type": "timestamp"},
                {"name": "timestamp", "type": "uint64"},
                {"name": "interval_", "type": "int64"},
            ]
            assert read_table("//tmp/t1") == read_table("//tmp/t2")

    @authors("dakovalkov")
    def test_yson_extract(self):
        with Clique(1) as clique:
            assert clique.make_query("select YSONHas('{a=5;b=6}', 'a') as a") == [{"a": 1}]
            assert clique.make_query("select YSONHas('{a=5;b=6}', 'c') as a") == [{"a": 0}]
            assert clique.make_query("select YSONHas('{a=5;b=[5; 4; 3]}', 'b', 1) as a") == [{"a": 1}]

            assert clique.make_query("select YSONLength('{a=5;b=6}') as a") == [{"a": 2}]
            assert clique.make_query("select YSONLength('{a=5;b=[5; 4; 3]}', 'b') as a") == [{"a": 3}]

            assert clique.make_query("select YSONKey('{a=5;b={c=4}}', 'b', 'c') as a") == [{"a": "c"}]

            assert clique.make_query("select YSONType('{a=5}') as a") == [{"a": "Object"}]
            assert clique.make_query("select YSONType('[1; 3; 4]') as a") == [{"a": "Array"}]
            assert clique.make_query("select YSONType('{a=5;b=4}', 'b') as a") == [{"a": "Int64"}]

            assert clique.make_query("select YSONExtractInt('{a=5;b=[5; 4; 3]}', 'b', 1) as a") == [{"a": 5}]

            assert clique.make_query("select YSONExtractUInt('{a=5;b=[5; 4; 3]}', 'b', 1) as a") == [{"a": 5}]

            assert clique.make_query("select YSONExtractFloat('[1; 2; 4.4]', 3) as a") == [{"a": 4.4}]

            assert clique.make_query("select YSONExtractBool('[%true; %false]', 1) as a") == [{"a": 1}]
            assert clique.make_query("select YSONExtractBool('[%true; %false]', 2) as a") == [{"a": 0}]

            assert clique.make_query("select YSONExtractString('[true; false]', 1) as a") == [{"a": "true"}]
            assert clique.make_query("select YSONExtractString('{a=true; b=false}', 'b') as a") == [{"a": "false"}]

            assert clique.make_query("select YSONExtract('{a=5;b=[5; 4; 3]}', 'b', 'Array(Int64)') as a") == [
                {"a": [5, 4, 3]}
            ]

            assert sorted(
                clique.make_query("select YSONExtractKeysAndValues('[{a=5};{a=5;b=6;c=10}]', 2, 'Int8') as a")[0]["a"]
            ) == [["a", 5], ["b", 6], ["c", 10]]

            assert yson.loads(clique.make_query("select YSONExtractRaw('[{a=5};{a=5;b=6;c=10}]', 2) as a")[0]["a"]) == {
                "a": 5,
                "b": 6,
                "c": 10,
            }

    @authors("dakovalkov")
    def test_yson_extract_invalid(self):
        with Clique(1) as clique:
            assert clique.make_query("select YSONLength('{a=5;b=6}', 'invalid_key') as a") == [{"a": 0}]
            assert clique.make_query("select YSONKey('{a=5;b={c=4}}', 'b', 'c', 'invalid_key') as a") == [{"a": ""}]
            assert clique.make_query("select YSONType('{a=5}', 'invalid_key') as a") == [{"a": "Null"}]
            assert clique.make_query("select YSONExtractInt('{a=5;b=[5; 4; 3]}', 'b', 100500) as a") == [{"a": 0}]
            assert clique.make_query("select YSONExtractUInt('{a=5;b=[5; 4; 3]}', 'b', -100500) as a") == [{"a": 0}]
            assert clique.make_query("select YSONExtractFloat('[1; 2; 4.4]', 42) as a") == [{"a": 0.0}]
            assert clique.make_query("select YSONExtractBool('[%true; %false]', 10) as a") == [{"a": 0}]
            assert clique.make_query("select YSONExtractString('[true; false]', 10) as a") == [{"a": ""}]
            assert clique.make_query("select YSONExtractString('{a=true; b=false}', 'invalid_key') as a") == [{"a": ""}]
            assert clique.make_query("select YSONExtract('{a=5;b=[5; 4; 3]}', 'invalid_key', 'Array(Int64)') as a") == [
                {"a": []}
            ]
            assert (
                clique.make_query("select YSONExtractKeysAndValues('[{a=5};{a=5;b=6;c=10}]', 2, 10, 'Int8') as a")[0][
                    "a"
                ]
                == []
            )
            assert clique.make_query("select YSONExtractRaw('[{a=5};{a=5;b=6;c=10}]', 2, 1) as a") == [{"a": ""}]

            assert clique.make_query("select YSONExtractString('{Invalid_YSON') as a") == [{"a": ""}]

    @authors("max42")
    def test_old_chunk_schema(self):
        # CHYT-256.
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create(
            "table", "//tmp/t2", attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "int64"}]}
        )
        write_table("//tmp/t1", [{"a": 1}])
        merge(in_=["//tmp/t1"], out="//tmp/t2", mode="ordered")

        with Clique(1) as clique:
            assert clique.make_query('select b from "//tmp/t2"') == [{"b": None}]

    @authors("max42")
    def test_nothing(self):
        with Clique(5):
            pass

    @authors("max42")
    def test_any_empty_result(self):
        # CHYT-338, CHYT-246.
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ]
            },
        )
        write_table("//tmp/t", [{"key": 1, "value": "a"}])

        with Clique(1) as clique:
            assert clique.make_query("select any(value) from `//tmp/t` where key = 2") == [{"any(value)": None}]

    @authors("max42")
    def test_constants(self):
        # CHYT-400.
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "string"}]},
        )
        write_table("//tmp/t", [{"key": 1, "value": "a"}])

        with Clique(1) as clique:
            assert clique.make_query("select 1 from `//tmp/t`") == [{"1": 1}]

    @authors("max42")
    def test_group_by(self):
        # CHYT-401.
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "key", "type": "int64"}, {"name": "value", "type": "int64"}]},
        )
        write_table(
            "//tmp/t", [{"key": 1, "value": 3}, {"key": 2, "value": 1}, {"key": 1, "value": 2}, {"key": 2, "value": 5}]
        )

        with Clique(1) as clique:
            assert clique.make_query("select key, min(value), max(value) from `//tmp/t` group by key order by key") == [
                {"key": 1, "min(value)": 2, "max(value)": 3},
                {"key": 2, "min(value)": 1, "max(value)": 5},
            ]

    @authors("dakovalkov")
    @pytest.mark.parametrize("optimize_for", ["scan", "lookup"])
    def test_virtual_columns(self, optimize_for):
        for i in range(4):
            table_path = "//tmp/t{}".format(i)
            create(
                "table",
                table_path,
                attributes={
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            write_table(
                table_path, [{"key": i}]
            )

        with Clique(1) as clique:
            # Virtual columns are not visible via 'select *' and 'describe'.
            assert clique.make_query("select * from `//tmp/t0`") == [{"key": 0}]
            query = "select * from concatYtTables('//tmp/t0', '//tmp/t1', '//tmp/t2', '//tmp/t3')"
            assert sorted(clique.make_query(query)) == [{"key": index} for index in range(4)]
            assert len(clique.make_query("describe `//tmp/t0`")) == 1
            assert len(clique.make_query("describe concatYtTables('//tmp/t0', '//tmp/t1')")) == 1

            def get_table_virtual_values(index):
                return {
                    "$table_index": index,
                    "$table_path": "//tmp/t{}".format(index),
                    "$table_name": "t{}".format(index),
                }

            def get_table_content(index):
                result = get_table_virtual_values(index)
                result["key"] = index
                return result

            query = "select *, $table_index, $table_path, $table_name from `//tmp/t0`"
            assert clique.make_query(query) == [get_table_content(0)]
            query = '''
                select *, $table_index, $table_path, $table_name
                from concatYtTables('//tmp/t0', '//tmp/t1', '//tmp/t2', '//tmp/t3')
            '''
            assert sorted(clique.make_query(query)) == [get_table_content(index) for index in range(4)]

            # Select only virtual values.
            query = '''
                select $table_index, $table_path, $table_name
                from concatYtTables('//tmp/t0', '//tmp/t1', '//tmp/t2', '//tmp/t3')
            '''
            assert sorted(clique.make_query(query)) == [get_table_virtual_values(index) for index in range(4)]

            # Join on virtual column.
            # XXX(dakovalkov): this does not work (https://github.com/ClickHouse/ClickHouse/issues/17860).
            # query = '''
            #     select * from concatYtTables("//tmp/t0", "//tmp/t1") as a
            #     global join "//tmp/t2" as b
            #     using $table_index
            # '''
            # assert sorted(clique.make_query(query)) == [
            #     {"key": 0, "b.key": 2},
            # ]

            query = '''
                select * from concatYtTables("//tmp/t0", "//tmp/t1") as a
                global join (select *, $table_index from concatYtTables("//tmp/t2", "//tmp/t3")) as b
                using $table_index
            '''
            assert sorted(clique.make_query(query)) == [
                {"key": 0, "b.key": 2},
                {"key": 1, "b.key": 3},
            ]

            # TODO(dakovalkov): This should not work since virtual columns are not key columns,
            # but the error is strange.
            # query = '''
            #     select * from concatYtTables("//tmp/t0", "//tmp/t1") as a
            #     join concatYtTables("//tmp/t2", "//tmp/t3") as b
            #     using $table_index
            # '''
            # assert sorted(clique.make_query(query)) == [
            #     {"key": 0, "b.key": 2},
            #     {"key": 1, "b.key": 3},
            # ]

    @authors("dakovalkov")
    @pytest.mark.skipif(True, reason="Virtual columns are not supported in dynamic tables (CHYT-506)")
    def test_virtual_columns_in_dynamic_tables(self):
        for i in range(2):
            table_path = "//tmp/dt{}".format(i)
            create(
                "table",
                table_path,
                attributes={
                    "dynamic": True,
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "int64"},
                    ],
                    "enable_dynamic_store_read": True,
                },
            )
            sync_mount_table(table_path)
            insert_rows(table_path, [{"key": i, "value": i + 10}])

        with Clique(1, config_patch={"yt": {"enable_dynamic_tables": True}}) as clique:
            def get_table_virtual_values(index):
                return {
                    "$table_index": index,
                    "$table_path": "//tmp/dt{}".format(index),
                    "$table_name": "dt{}".format(index),
                }

            def get_table_content(index):
                result = get_table_virtual_values(index)
                result["key"] = index
                result["value"] = index + 10
                return result

            assert clique.make_query("select * from `//tmp/dt0`") == [{"key": 0, "value": 10}]
            
            query = "select *, $table_index, $table_path, $table_name from `//tmp/dt0`"
            assert clique.make_query(query) == [get_table_content(0)]

            query = '''
                select *, $table_index, $table_path, $table_name
                from concatYtTables("//tmp/dt0", "//tmp/dt1")
            '''
            assert sorted(clique.make_query(query)) == [get_table_content(index) for index in range(2)]

            query = '''
                select $table_index, $table_path, $table_name
                from concatYtTables("//tmp/dt0", "//tmp/dt1")
            '''
            assert sorted(clique.make_query(query)) == [get_table_virtual_values(index) for index in range(2)]

    @authors("dakovalkov")
    def test_virtual_column_index(self):
        rows_per_table = 10
        table_data = []

        for i in range(4):
            table_path = "//tmp/t{}".format(i)
            create(
                "table",
                table_path,
                attributes={
                    "schema": [
                        {"name": "key", "type": "int64"},
                        {"name": "subkey", "type": "int64"},
                    ],
                },
            )
            rows = [{"key": i, "subkey": j} for j in range(0, rows_per_table)]
            write_table(table_path, rows)
            table_data.append(rows)

        with Clique(1) as clique:
            # Simple.
            query = "select * from concatYtTablesRange('//tmp') where $table_index = 2"
            assert sorted(clique.make_query_and_validate_row_count(query, exact=(1 * rows_per_table))) == \
                table_data[2]

            # Non-monotonic transformation.
            query = "select * from concatYtTablesRange('//tmp') where $table_index % 2 = 0"
            assert sorted(clique.make_query_and_validate_row_count(query, exact=(2 * rows_per_table))) == \
                table_data[0] + table_data[2]

            # Several expressions.
            query = """
            select * from concatYtTablesRange('//tmp')
            where $table_index = 0 or $table_name = 't1' or $table_path = '//tmp/t2'
            """
            assert sorted(clique.make_query_and_validate_row_count(query, exact=(3 * rows_per_table))) == \
                table_data[0] + table_data[1] + table_data[2]

            # Non-monotonic transformation + $table_index check.
            query = """
            select *, $table_index from concatYtTablesRange('//tmp')
            where endsWith($table_path, '1')
            """
            assert sorted(clique.make_query_and_validate_row_count(query, exact=(1 * rows_per_table))) == \
                [{"key": 1, "$table_index": 1, "subkey": i} for i in range(0, rows_per_table)]

class TestJobInput(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42", "evgenstf")
    @pytest.mark.parametrize("where_prewhere", ["where", "prewhere"])
    def test_chunk_filter(self, where_prewhere):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}]})
        for i in xrange(10):
            write_table("<append=%true>//tmp/t", [{"i": i}])
        with Clique(1) as clique:
            clique.make_query_and_validate_row_count(
                'select * from "//tmp/t" {} i >= 3'.format(where_prewhere), exact=7
            )
            clique.make_query_and_validate_row_count('select * from "//tmp/t" {} i < 2'.format(where_prewhere), exact=2)
            clique.make_query_and_validate_row_count(
                'select * from "//tmp/t" {} 5 <= i and i <= 8'.format(where_prewhere), exact=4
            )
            clique.make_query_and_validate_row_count(
                'select * from "//tmp/t" {} i in (-1, 2, 8, 8, 15)'.format(where_prewhere), exact=2
            )

    @authors("max42")
    def test_computed_column_chunk_filter(self):
        # See also: computed_columns_ut.cpp.

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "c", "type": "int64", "sort_order": "ascending", "expression": "i *  2"},
                    {"name": "i", "type": "int64", "sort_order": "ascending"},
                ]
            },
        )
        for i in xrange(5):
            write_table("<append=%true>//tmp/t", [{"i": 2 * i}, {"i": 2 * i + 1}])

        for enable_computed_column_deduction in (False, True):
            with Clique(
                1,
                config_patch={
                    "yt": {"settings": {"enable_computed_column_deduction": enable_computed_column_deduction}}
                },
            ) as clique:
                correct_row_count = lambda row_count: row_count if enable_computed_column_deduction else 10
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i == 3', exact=correct_row_count(2)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i == 6 or i == 7', exact=correct_row_count(2)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i == 0 or i == 9', exact=correct_row_count(4)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=correct_row_count(4)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in tuple(-1, 2, 8, 8, 15)', exact=correct_row_count(4)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in (1)', exact=correct_row_count(2)
                )
                clique.make_query_and_validate_row_count(
                    'select * from "//tmp/t" where i in tuple(1)', exact=correct_row_count(2)
                )

                # This case should not be optimized.
                clique.make_query_and_validate_row_count('select * from "//tmp/t" where 5 <= i and i <= 8', exact=10)

    @authors("max42")
    def test_dynamic_table_farm_hash(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {
                        "name": "computed_key",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key)",
                    },
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "enable_dynamic_store_read": True,
            },
        )
        tablet_count = 100
        sync_reshard_table(
            "//tmp/t", [[]] + [[yson.YsonUint64(i * 2 ** 64 // tablet_count)] for i in xrange(tablet_count)]
        )
        sync_mount_table("//tmp/t")
        key_count = 5
        for i in xrange(key_count):
            insert_rows("//tmp/t", [{"key": "k" + str(i), "value": "v" + str(i)}])

        with Clique(
            1,
            config_patch={
                "yt": {"settings": {"enable_computed_column_deduction": True}, "enable_dynamic_tables": True}
            },
        ) as clique:
            clique.make_query_and_validate_row_count("select * from `//tmp/t`", exact=5)
            clique.make_query_and_validate_row_count("select * from `//tmp/t` where key == 'k1' or key = 'k3'", exact=2)
            clique.make_query_and_validate_row_count(
                "select * from (select * from `//tmp/t` where key == 'k4')", exact=1
            )

    @authors("max42")
    def test_dynamic_table_farm_hash_two_components(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {
                        "name": "computed_key",
                        "type": "uint64",
                        "sort_order": "ascending",
                        "expression": "farm_hash(key, subkey)",
                    },
                    {"name": "key", "type": "string", "sort_order": "ascending"},
                    {"name": "subkey", "type": "string", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "dynamic": True,
                "enable_dynamic_store_read": True,
            },
        )
        tablet_count = 100
        sync_reshard_table(
            "//tmp/t", [[]] + [[yson.YsonUint64(i * 2 ** 64 // tablet_count)] for i in xrange(tablet_count)]
        )
        sync_mount_table("//tmp/t")
        key_count = 5
        for i in xrange(key_count):
            insert_rows("//tmp/t", [{"key": "k" + str(i), "subkey": "sk" + str(i), "value": "v" + str(i)}])

        with Clique(
            1,
            config_patch={
                "yt": {"settings": {"enable_computed_column_deduction": True}, "enable_dynamic_tables": True}
            },
        ) as clique:
            assert len(clique.make_query_and_validate_row_count("select * from `//tmp/t`", exact=5)) == 5
            assert (
                len(
                    clique.make_query_and_validate_row_count(
                        "select * from `//tmp/t` where (key, subkey) == ('k1', 'sk1') or (key, subkey) = ('k3', 'sk3')",
                        exact=2,
                    )
                )
                == 2
            )
            assert (
                len(
                    clique.make_query_and_validate_row_count(
                        "select * from (select * from `//tmp/t` where (key, subkey) == ('k4', 'sk4'))", exact=1
                    )
                )
                == 1
            )

    @authors("dakovalkov")
    def test_common_schema_sorted(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "string", "sort_order": "ascending"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t3",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "c", "type": "double"},
                ]
            },
        )

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 18, "c": 2.71})
        write_table("//tmp/t3", {"a": 18, "c": 2.71})

        with Clique(1) as clique:
            # Column 'a' is sorted.
            clique.make_query_and_validate_row_count(
                'select * from concatYtTables("//tmp/t1", "//tmp/t2") where a > 18', exact=1
            )
            # Column 'a' isn't sorted.
            clique.make_query_and_validate_row_count(
                'select * from concatYtTables("//tmp/t1", "//tmp/t3") where a > 18', exact=2
            )

    @authors("dakovalkov")
    @pytest.mark.parametrize("required", [False, True])
    def test_key_types(self, required):
        int_types = ["uint64", "uint32", "uint16", "uint8", "int64", "int32", "int16", "int8"]
        int_values = [i * 2 for i in range(3)]
        # TODO(dakovalkov): For some reason KeyCondition can not construct from Nullable(int8) now.
        # Delete this test till ClickHouse fix this.
        if not required:
            int_types.pop()

        float_types = ["float", "double"]
        float_values = [i * 2.0 for i in range(3)]

        string_types = ["string", "any"]
        string_values = ["{abc=2}", "{zzz=3}"]
        # Column of type "any" can not be required.
        if required:
            string_types.pop()

        date_types = ["date", "datetime"] #, "timestamp"]
        date_scales = [1, 24 * 60 * 60] #, 24 * 60 * 60 * 10**6]
        date_values = [i * 10 for i in range(3)]

        # TODO(dakovalkov): Delete this when timestamp is represented as DateTime64.
        int_types.append("timestamp")
        # Interval is represented as Int64.
        int_types.append("interval")

        def create_type_table(type, values):
            path = "//tmp/t_{}".format(type)
            create(
                "table",
                path,
                attributes={
                    "schema": [
                        {
                            "name": "key",
                            "type": type,
                            "sort_order": "ascending",
                            "required": required,
                        },
                    ],
                }
            )
            for value in values:
                write_table("<append=%true>" + path, [{"key": value}])

        for type in int_types:
            create_type_table(type, int_values)

        for type in float_types:
            create_type_table(type, float_values)

        for type in string_types:
            create_type_table(type, string_values)

        for type, scale in zip(date_types, date_scales):
            create_type_table(type, [scale * value for value in date_values])

        with Clique(1) as clique:
            query1 = 'select * from "//tmp/t_{}" where key = 2'
            query2 = 'select * from "//tmp/t_{}" where 1 < key and key < 3'
            for type in (int_types + float_types):
                clique.make_query_and_validate_row_count(query1.format(type), exact=1)
                clique.make_query_and_validate_row_count(query2.format(type), exact=1)

            query = 'select * from "//tmp/t_{}" where key = \'{{abc=2}}\''
            for type in string_types:
                clique.make_query_and_validate_row_count(query.format(type), exact=1)

            query = 'select * from "//tmp/t_{}" where \'1970.01.10\' < key and key < \'1970.01.12\''
            for type in date_types:
                clique.make_query_and_validate_row_count(query.format(type), exact=1)

    @authors("max42")
    @pytest.mark.xfail(run="False", reason="Chunk slicing is temporarily not supported")
    def test_chunk_slicing(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "chunk_writer": {"block_size": 1024},
                "compression_codec": "none",
                # TODO(max42): investigate what happens when both columns are sorted.
                "schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}, {"name": "s", "type": "string"}],
            },
        )

        write_table("//tmp/t", [{"i": i, "s": str(i) * (10 * 1024)} for i in range(10)], verbose=False)
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@compressed_data_size") > 100 * 1024
        assert get("#" + chunk_id + "/@max_block_size") < 20 * 1024

        with Clique(1) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i >= 3', min=7, max=8)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i < 2', min=3, max=4)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where 5 <= i and i <= 8', min=4, max=6)
            clique.make_query_and_validate_row_count(
                'select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', min=2, max=4
            )

        # Forcefully disable chunk slicing.
        with Clique(1, config_patch={"yt": {"subquery": {"max_sliced_chunk_count": 0}}}) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i >= 3', exact=10)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i < 2', exact=10)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where 5 <= i and i <= 8', exact=10)
            clique.make_query_and_validate_row_count('select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=10)

    @authors("max42", "gritukan")
    @pytest.mark.parametrize("use_block_sampling", [False, True])
    def test_sampling(self, use_block_sampling):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}],
                "chunk_writer": {"block_size": 1024},
            },
        )
        write_table("//tmp/t", [{"a": i, "b": "A" * 1500} for i in range(1000)], verbose=False)
        with Clique(1) as clique:
            settings = {"chyt.use_block_sampling": int(use_block_sampling)}
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0.1', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 100', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 2/20', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0.1 offset 42', min=60, max=170,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 10000', exact=1000, verbose=False,
                                                     settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 10000', exact=1000, verbose=False,
                                                     settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0', exact=0, verbose=False,
                                                     settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 0.000000000001', exact=0,
                                                     verbose=False, settings=settings)
            clique.make_query_and_validate_row_count('select a from "//tmp/t" sample 1/100000000000', exact=0,
                                                     verbose=False, settings=settings)

    @authors("max42")
    def test_CHYT_143(self):
        # Issues with chunk name table ids, read schema ids and unversioned value row indices.
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "b", "type": "string"}, {"name": "a", "type": "int64"}]},
        )
        write_table("//tmp/t1", [{"a": 42, "b": "asd"}])
        write_table("//tmp/t2", [{"b": "qwe", "a": 27}])
        with Clique(1) as clique:
            result = clique.make_query("select * from concatYtTables('//tmp/t1', '//tmp/t2')")
            assert len(result) == 2
            assert len(result[0]) == 2

    @authors("max42")
    def test_duplicating_table_functions(self):
        # CHYT-194.
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 42}])
        with Clique(1) as clique:
            result = clique.make_query(
                "select * from concatYtTables('//tmp/t') union all select * from concatYtTables('//tmp/t')"
            )
            assert result == [{"a": 42}, {"a": 42}]

    @authors("max42")
    def DISABLED_test_min_data_weight_per_thread(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "x" * 100} for i in range(30)])

        def get_momentary_stats(instances):
            stats = []
            for instance in instances:
                wait(lambda: clique.get_orchid(instance, "/queries/users/root", verbose=False) is not None)
                query_registry = clique.get_orchid(instance, "/queries/users/root")
                stats.append(
                    (
                        (query_registry["historical_initial_query_count"]),
                        query_registry["historical_secondary_query_count"],
                    )
                )
            return stats

        def get_delta_stats(instances, initial_instance, query):
            old_stats = get_momentary_stats(instances)
            clique.make_direct_query(initial_instance, query, verbose=False)
            new_stats = get_momentary_stats(instances)
            return [(rhs[0] - lhs[0], rhs[1] - lhs[1]) for lhs, rhs in zip(old_stats, new_stats)]

        with Clique(3) as clique:
            instances = clique.get_active_instances()
            assert len(instances) == 3
            initial_instance = instances[random.randint(0, 2)]
            delta_stats = get_delta_stats(instances, initial_instance, 'select * from "//tmp/t"')

            for delta_stat, instance in zip(delta_stats, instances):
                assert delta_stat[0] == (1 if instance == initial_instance else 0)
                assert delta_stat[1] == 1

        with Clique(3, config_patch={"yt": {"subquery": {"min_data_weight_per_thread": 5000}}}) as clique:
            instances = clique.get_active_instances()
            assert len(instances) == 3
            initial_instance = instances[random.randint(0, 2)]
            delta_stats = get_delta_stats(instances, initial_instance, 'select * from "//tmp/t"')

            for delta_stat, instance in zip(delta_stats, instances):
                assert delta_stat[0] == (1 if instance == initial_instance else 0)
                assert delta_stat[1] == (1 if instance == initial_instance else 0)

    @authors("max42")
    def test_ypath(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "ki", "type": "int64", "sort_order": "ascending"},
                    {"name": "ks", "type": "string", "sort_order": "ascending"},
                    {"name": "v", "type": "string"},
                ]
            },
        )
        rows = []
        for ki in xrange(9):
            for ks in ("abc", "def", "ghi"):
                rows.append({"ki": ki, "ks": ks, "v": str(ki) + ks})
            if ki % 3 == 2:
                write_table("<append=%true>//tmp/t", rows)
                rows = []

        yson_max = yson.to_yson_type(None, attributes={"type": "max"})
        yson_min = yson.to_yson_type(None, attributes={"type": "min"})
        yson_null = yson.to_yson_type(None)

        with Clique(1) as clique:
            # Simple form.
            def check_simple(lower_limit, upper_limit):
                if upper_limit is not None:
                    range_spec = "{}:{}".format(lower_limit, upper_limit)
                else:
                    range_spec = lower_limit
                table_path = "//tmp/t[{}]".format(range_spec)
                expected_rows = read_table(table_path)
                actual_rows = clique.make_query("select * from `{}` order by (ki, ks)".format(table_path))
                assert actual_rows == expected_rows

            for lower_limit in ("", "#3", "#12", "()", "(0)", '(0, "def")', "(1)", '(1, "def")'):
                check_simple(lower_limit, None)
                for upper_limit in ("", "#24", "#15", "()", "(2)", '(2, "ghi")', "(1)", '(1, "ghi")'):
                    check_simple(lower_limit, upper_limit)
                    check_simple(upper_limit, lower_limit)

            # Complex form.
            def check_complex(lower_limit, upper_limit):
                if upper_limit is not None:
                    range_spec = {"exact": lower_limit}
                else:
                    range_spec = {"lower": lower_limit, "upper": upper_limit}
                table_path = "<ranges=[{}]>//tmp/t".format(yson.dumps(range_spec, yson_format="text"))
                expected_rows = read_table(table_path)
                actual_rows = clique.make_query("select * from `{}` order by (ki, ks)".format(table_path))
                assert actual_rows == expected_rows

            for lower_limit in (
                {},
                {"row_index": 3},
                {"row_index": 12},
                {"key": []},
                {"key": [0]},
                {"key": [0, "def"]},
                {"key": [1]},
                {"key": [1, "def"]},
                {"key": [0, yson_max]},
                {"key": [0, yson_min]},
                {"key": [0, yson_null]},
                {"key": [1, yson_max]},
                {"key": [1, yson_min]},
                {"key": [1, yson_null]},
            ):
                check_complex(lower_limit, None)
                for upper_limit in (
                    {},
                    {"row_index": 24},
                    {"row_index": 15},
                    {"key": []},
                    {"key": [2]},
                    {"key": [2, "ghi"]},
                    {"key": [1]},
                    {"key": [1, "ghi"]},
                    {"key": [2, yson_max]},
                    {"key": [2, yson_min]},
                    {"key": [2, yson_null]},
                    {"key": [1, yson_max]},
                    {"key": [1, yson_min]},
                    {"key": [1, yson_null]},
                ):
                    check_complex(lower_limit, upper_limit)
                    check_complex(upper_limit, lower_limit)


class TestMutations(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_insert_values(self):
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t"(i64) values (1), (-2)')
            clique.make_query('insert into "//tmp/t"(ui64) values (7), (8)')
            clique.make_query('insert into "//tmp/t"(str) values (2)')
            clique.make_query("insert into \"//tmp/t\"(i64, ui64, str, dbl, bool) values (-1, 1, 'abc', 3.14, 1)")
            clique.make_query('insert into "//tmp/t"(i64, ui64, str, dbl, bool) values (NULL, NULL, NULL, NULL, NULL)')
            with raises_yt_error(QueryFailedError):
                clique.make_query('insert into "//tmp/t"(bool) values (3)')
            with raises_yt_error(QueryFailedError):
                clique.make_query('insert into "//tmp/t"(bool) values (2.4)')
            assert read_table("//tmp/t") == [
                {"i64": 1, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": -2, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 7, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 8, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": None, "str": "2", "dbl": None, "bool": None},
                {"i64": -1, "ui64": 1, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": None, "ui64": None, "str": None, "dbl": None, "bool": None},
            ]
            assert get("//tmp/t/@chunk_count") == 5
            clique.make_query("insert into \"<append=%false>//tmp/t\" values (-2, 2, 'xyz', 2.71, 0)")
            assert read_table("//tmp/t") == [
                {"i64": -2, "ui64": 2, "str": "xyz", "dbl": 2.71, "bool": False},
            ]
            assert get("//tmp/t/@chunk_count") == 1

    @authors("max42")
    def test_insert_select(self):
        create(
            "table",
            "//tmp/s1",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        write_table(
            "//tmp/s1",
            [
                {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ],
        )

        # Table with different order of columns.
        create(
            "table",
            "//tmp/s2",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        write_table(
            "//tmp/s2",
            [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
            ],
        )

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t" select * from "//tmp/s1"')
            assert read_table("//tmp/t") == [
                {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ]

            # Number of columns does not match.
            with raises_yt_error(QueryFailedError):
                clique.make_query('insert into "//tmp/t" select i64, ui64 from "//tmp/s1"')

            # Columns are matched according to positions. Values are best-effort casted due to CH logic.
            clique.make_query('insert into "<append=%false>//tmp/t" select * from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": None, "str": "12.3", "dbl": 9.0, "bool": False},
                {"i64": -5, "ui64": None, "str": "-3.1", "dbl": 5.0, "bool": True},
            ]

            clique.make_query('insert into "<append=%false>//tmp/t" select i64, ui64, str, dbl, bool from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
            ]

            clique.make_query('insert into "//tmp/t"(i64, ui64) select max(i64), min(ui64) from "//tmp/s2"')
            assert read_table("//tmp/t") == [
                {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
                {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
                {"i64": 4, "ui64": 5, "str": None, "dbl": None, "bool": None},
            ]

    @authors("max42")
    def test_create_table_simple(self):
        with Clique(1, config_patch={"yt": {"create_table_default_attributes": {"foo": 42}}}) as clique:
            clique.make_query(
                'create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, i32 Int32, dt Date, dtm DateTime) '
                "engine YtTable() order by (str, i64)"
            )
            assert normalize_schema(get("//tmp/t/@schema")) == make_schema(
                [
                    {"name": "str", "type": "string", "sort_order": "ascending", "required": True},
                    {"name": "i64", "type": "int64", "sort_order": "ascending", "required": True},
                    {"name": "ui64", "type": "uint64", "required": True},
                    {"name": "dbl", "type": "double", "required": True},
                    {"name": "i32", "type": "int32", "required": True},
                    {"name": "dt", "type": "date", "required": True},
                    {"name": "dtm", "type": "datetime", "required": True},
                ],
                strict=True,
                unique_keys=False,
            )

            # Table already exists.
            with raises_yt_error(QueryFailedError):
                clique.make_query(
                    'create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, i32 Int32) '
                    "engine YtTable() order by (str, i64)"
                )

            clique.make_query(
                'create table "//tmp/t_nullable"(i64 Nullable(Int64), ui64 Nullable(UInt64), str Nullable(String), '
                + "dbl Nullable(Float64), i32 Nullable(Int32), dt Nullable(Date), dtm Nullable(DateTime))"
                ""
                "engine YtTable() order by (str, i64)"
            )
            assert normalize_schema(get("//tmp/t_nullable/@schema")) == make_schema(
                [
                    {"name": "str", "type": "string", "sort_order": "ascending", "required": False},
                    {"name": "i64", "type": "int64", "sort_order": "ascending", "required": False},
                    {"name": "ui64", "type": "uint64", "required": False},
                    {"name": "dbl", "type": "double", "required": False},
                    {"name": "i32", "type": "int32", "required": False},
                    {"name": "dt", "type": "date", "required": False},
                    {"name": "dtm", "type": "datetime", "required": False},
                ],
                strict=True,
                unique_keys=False,
            )

            # No non-trivial expressions.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t2"(i64 Int64) engine YtTable() order by (i64 * i64)')

            # Missing key column.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t2"(i Int64) engine YtTable() order by j')

            clique.make_query("create table \"//tmp/t_snappy\"(i Int64) engine YtTable('{compression_codec=snappy}')")
            assert get("//tmp/t_snappy/@compression_codec") == "snappy"

            # Default optimize_for should be scan.
            assert get("//tmp/t_snappy/@optimize_for") == "scan"

            assert get("//tmp/t_snappy/@foo") == 42

            # Empty schema.
            with raises_yt_error(QueryFailedError):
                clique.make_query('create table "//tmp/t2" engine YtTable()')

            # Underscore indicates that the columns should be ignored and schema from attributes should
            # be taken.
            clique.make_query("create table \"//tmp/t2\"(_ UInt8) engine YtTable('{schema=[{name=a;type=int64}]}')")
            assert get("//tmp/t2/@schema/0/name") == "a"

            # Column list has higher priority.
            clique.make_query("create table \"//tmp/t3\"(b String) engine YtTable('{schema=[{name=a;type=int64}]}')")
            assert get("//tmp/t3/@schema/0/name") == "b"

    @authors("max42")
    def test_create_table_as_select(self):
        create(
            "table",
            "//tmp/s1",
            attributes={
                "schema": [
                    {"name": "i64", "type": "int64"},
                    {"name": "ui64", "type": "uint64"},
                    {"name": "str", "type": "string"},
                    {"name": "dbl", "type": "double"},
                    {"name": "bool", "type": "boolean"},
                ]
            },
        )
        write_table(
            "//tmp/s1",
            [
                {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": True},
                {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ],
        )

        with Clique(1) as clique:
            clique.make_query('create table "//tmp/t1" engine YtTable() order by i64 as select * from "//tmp/s1"')

            assert read_table("//tmp/t1") == [
                {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": 1},
                {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": 0},
            ]

    @authors("max42")
    def test_create_table_as_table(self):
        schema = [
            {"name": "i64", "type": "int64", "required": False, "sort_order": "ascending"},
            {"name": "ui64", "type": "uint64", "required": False},
            {"name": "str", "type": "string", "required": False},
            {"name": "dbl", "type": "double", "required": False},
            {"name": "bool", "type": "boolean", "required": False},
        ]
        schema_copied = copy.deepcopy(schema)
        schema_copied[4]["type"] = "uint8"
        create("table", "//tmp/s1", attributes={"schema": schema, "compression_codec": "snappy"})

        with Clique(1) as clique:
            clique.make_query('show create table "//tmp/s1"')
            clique.make_query('create table "//tmp/s2" as "//tmp/s1" engine YtTable() order by i64')
            assert normalize_schema(get("//tmp/s2/@schema")) == make_schema(
                schema_copied, strict=True, unique_keys=False
            )

            # This is wrong.
            # assert get("//tmp/s2/@compression_codec") == "snappy"

    @authors("dakovalkov")
    def test_create_table_clear_cache(self):
        patch = get_object_attibute_cache_config(15000, 15000, 500)
        with Clique(1, config_patch=patch) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})

            # Load attributes into cache.
            assert get_schema_from_description(clique.make_query('describe "//tmp/t"')) == [
                {"name": "a", "type": "Nullable(Int64)"}
            ]

            remove("//tmp/t")

            # Wait for clearing cache.
            time.sleep(1)

            clique.make_query('create table "//tmp/t"(b String) engine YtTable()')

            assert get_schema_from_description(clique.make_query('describe "//tmp/t"')) == [
                {"name": "b", "type": "String"}
            ]


class TestClickHouseNoCache(ClickHouseTestBase):
    def setup(self):
        self._setup()
        remove_user("yt-clickhouse-cache")

    @authors("dakovalkov")
    def test_no_clickhouse_cache(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 123}])
        with Clique(1) as clique:
            for i in range(4):
                clique.make_query('select * from "//tmp/t"') == [{"a": 123}]


class TestYsonFunctionsBase(ClickHouseTestBase):
    ENABLE_COMPOSITE_CONVERSION = False

    def setup(self):
        self._setup()

        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "i", "type": "int64"},
                    {"name": "v", "type": "any"},
                    {"name": "key", "type": "string"},
                    {"name": "fmt", "type": "string"},
                ]
            },
        )
        write_table(
            "//tmp/t",
            [
                {
                    "i": 0,
                    "v": {
                        "i64": -42,
                        "ui64": yson.YsonUint64(23),
                        "bool": True,
                        "dbl": 3.14,
                        "str": "xyz",
                        "subnode": {
                            "i64": 123,
                        },
                        "arr_i64": [-1, 0, 1],
                        "arr_ui64": [1, 1, 2, 3, 5],
                        "arr_dbl": [-1.1, 2.71],
                        "arr_bool": [False, True, False],
                    },
                    "key": "/arr_i64/0",
                },
                {
                    "i": 1,
                    "v": {
                        "i64": "xyz",  # Wrong type.
                    },
                    "key": "/i64",
                    "fmt": "text",
                },
                {
                    "i": 2,
                    "v": {
                        "i64": yson.YsonUint64(2 ** 63 + 42),  # Out of range for getting value as i64.
                    },
                    "fmt": "text",
                },
                {
                    "i": 3,
                    "v": {},  # Key i64 is missing.
                    "fmt": "pretty",
                },
                {
                    "i": 4,
                    "v": {
                        "i64": 57,
                    },
                    "key": None,
                    "fmt": "pretty",
                },
                {
                    "i": 5,
                    "v": None,
                    "key": "/unknown",
                },
            ],
        )
        set(
            "//sys/clickhouse/config/yt/settings/composite/enable_conversion",
            self.ENABLE_COMPOSITE_CONVERSION,
            recursive=True,
        )

    def teardown(self):
        remove("//sys/clickhouse/config/yt/settings/composite/enable_conversion", force=True)

    @authors("max42")
    def test_read_int64_strict(self):
        with Clique(1) as clique:
            for i in xrange(4):
                query = "select YPathInt64Strict(v, '/i64') from \"//tmp/t\" where i = {}".format(i)
                if i != 0:
                    with raises_yt_error(QueryFailedError):
                        clique.make_query(query)
                else:
                    result = clique.make_query(query)
                    assert result[0].popitem()[1] == -42

    @authors("max42")
    def test_read_uint64_strict(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathUInt64Strict(v, '/i64') from \"//tmp/t\" where i = 4")
            assert result[0].popitem()[1] == 57

    @authors("max42")
    def test_read_from_subnode(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathUInt64Strict(v, '/subnode/i64') from \"//tmp/t\" where i = 0")
            assert result[0].popitem()[1] == 123

    @authors("max42", "dakovalkov")
    def test_read_int64_non_strict(self):
        with Clique(1) as clique:
            query = "select YPathInt64(v, '/i64') from \"//tmp/t\""
            result = clique.make_query(query)
            for i, item in enumerate(result):
                if i == 0:
                    assert item.popitem()[1] == -42
                elif i == 4:
                    assert item.popitem()[1] == 57
                elif i == 5:
                    assert item.popitem()[1] == None
                else:
                    assert item.popitem()[1] == 0

    @authors("max42")
    def test_read_all_types_strict(self):
        query = (
            "select YPathInt64Strict(v, '/i64') as i64, YPathUInt64Strict(v, '/ui64') as ui64, "
            "YPathDoubleStrict(v, '/dbl') as dbl, YPathBooleanStrict(v, '/bool') as bool, "
            "YPathStringStrict(v, '/str') as str, YPathArrayInt64Strict(v, '/arr_i64') as arr_i64, "
            "YPathArrayUInt64Strict(v, '/arr_ui64') as arr_ui64, YPathArrayDoubleStrict(v, '/arr_dbl') as arr_dbl, "
            "YPathArrayBooleanStrict(v, '/arr_bool') as arr_bool from \"//tmp/t\" where i = 0"
        )
        with Clique(1) as clique:
            result = clique.make_query(query)
        assert result == [
            {
                "i64": -42,
                "ui64": 23,
                "bool": True,
                "dbl": 3.14,
                "str": "xyz",
                "arr_i64": [-1, 0, 1],
                "arr_ui64": [1, 1, 2, 3, 5],
                "arr_dbl": [-1.1, 2.71],
                "arr_bool": [False, True, False],
            }
        ]

    @authors("max42")
    def test_read_all_types_non_strict(self):
        query = (
            "select YPathInt64(v, '/i64') as i64, YPathUInt64(v, '/ui64') as ui64, "
            "YPathDouble(v, '/dbl') as dbl, YPathBoolean(v, '/bool') as bool, "
            "YPathString(v, '/str') as str, YPathArrayInt64(v, '/arr_i64') as arr_i64, "
            "YPathArrayUInt64(v, '/arr_ui64') as arr_ui64, YPathArrayDouble(v, '/arr_dbl') as arr_dbl, "
            "YPathArrayBoolean(v, '/arr_bool') as arr_bool from \"//tmp/t\" where i = 3"
        )
        with Clique(1) as clique:
            result = clique.make_query(query)
        assert result == [
            {
                "i64": 0,
                "ui64": 0,
                "bool": False,
                "dbl": 0.0,
                "str": "",
                "arr_i64": [],
                "arr_ui64": [],
                "arr_dbl": [],
                "arr_bool": [],
            }
        ]

    @authors("max42")
    def test_const_args(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathString('{a=[1;2;{b=xyz}]}', '/a/2/b') as str")
        assert result == [{"str": "xyz"}]

    @authors("max42", "dakovalkov")
    def test_nulls(self):
        with Clique(1) as clique:
            result = clique.make_query(
                "select YPathString(NULL, NULL) as a, YPathString(NULL, '/x') as b, " "YPathString('{a=1}', NULL) as c"
            )
            assert result == [{"a": None, "b": None, "c": None}]

            result = clique.make_query('select YPathInt64(v, key) from "//tmp/t"')
            for i, item in enumerate(result):
                if i == 0:
                    assert item.popitem()[1] == -1
                elif i == 1:
                    assert item.popitem()[1] == 0
                else:
                    assert item.popitem()[1] == None

    # CHYT-157.
    @authors("max42")
    def test_int64_as_any(self):
        create("table", "//tmp/s1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/s2", attributes={"schema": [{"name": "a", "type": "any"}]})
        lst = [{"a": -(2 ** 63)}, {"a": -42}, {"a": 123456789123456789}, {"a": 2 ** 63 - 1}]
        write_table("//tmp/s1", lst)
        merge(in_="//tmp/s1", out="//tmp/s2")

        with Clique(1) as clique:
            result = clique.make_query("select YPathInt64(a, '') as i from \"//tmp/s2\" order by i")
            assert result == [{"i": row["a"]} for row in lst]

    @authors("dakovalkov")
    def test_raw_yson_as_any(self):
        object = {"a": [1, 2, {"b": "xxx"}]}
        create("table", "//tmp/s1", attributes={"schema": [{"name": "a", "type": "any"}]})
        write_table("//tmp/s1", {"a": object})

        with Clique(1) as clique:
            result = clique.make_query("select YPathRaw(a, '') as i from \"//tmp/s1\"")
            assert result == [{"i": yson.dumps(object, "binary")}]
            result = clique.make_query("select YPathRawStrict(a, '/a') as i from \"//tmp/s1\"")
            assert result == [{"i": yson.dumps(object["a"], "binary")}]
            result = clique.make_query("select YPathRaw(a, '', 'text') as i from \"//tmp/s1\"")
            assert result == [{"i": yson.dumps(object, "text")}]
            result = clique.make_query("select YPathRaw(a, '/b') as i from \"//tmp/s1\"")
            assert result == [{"i": None}]
            with raises_yt_error(QueryFailedError):
                clique.make_query("select YPathRawStrict(a, '/b') as i from \"//tmp/s1\"")

    @authors("dakovlkov")
    def test_ypath_extract(self):
        object = {"a": [[1, 2, 3], [4, 5], [6, 7, 8, 9]]}
        create("table", "//tmp/s1", attributes={"schema": [{"name": "a", "type": "any"}]})
        write_table("//tmp/s1", {"a": object})

        with Clique(1) as clique:
            result = clique.make_query("select YPathExtract(a, '/a/1/1', 'UInt64') as i from \"//tmp/s1\"")
            assert result == [{"i": object["a"][1][1]}]
            result = clique.make_query("select YPathExtract(a, '/a/2', 'Array(UInt64)') as i from \"//tmp/s1\"")
            assert result == [{"i": object["a"][2]}]
            result = clique.make_query("select YPathExtract(a, '/a', 'Array(Array(UInt64))') as i from \"//tmp/s1\"")
            assert result == [{"i": object["a"]}]

    @authors("max42")
    def test_rich_types_v3_are_strings(self):
        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "a", "type_v3": {"type_name": "list", "item": "int64"}}]},
        )
        lst = [42, 23]
        write_table("//tmp/t2", [{"a": lst}])

        with Clique(1) as clique:
            result = clique.make_query("describe `//tmp/t2`")
            assert len(result) == 1
            assert result[0]["type"] == "String"

            assert clique.make_query("select a from `//tmp/t2`")[0] == {"a": yson.dumps(lst, yson_format="binary")}

    # CHYT-370.
    @authors("max42")
    def test_const_arguments(self):
        with Clique(1) as clique:
            assert clique.make_query("select YPathRaw('[foo; bar]', '', 'text') as a")[0] == {"a": '["foo";"bar";]'}
            with raises_yt_error(QueryFailedError):
                clique.make_query("select YPathRaw('[invalid_yson', '', 'text') as a")
                clique.make_query("select YPathRawStrict('[invalid_yson', '', 'text') as a")

    @authors("max42")
    def test_different_format_per_row(self):
        with Clique(1) as clique:
            assert clique.make_query("select YPathRaw(v, '', fmt) as a from `//tmp/t[#1:#5]`") == [
                {"a": yson.dumps(row["v"], row["fmt"])} for row in read_table("//tmp/t[#1:#5]")
            ]


class TestYsonFunctionsCompositeConversionEnabled(TestYsonFunctionsBase):
    ENABLE_COMPOSITE_CONVERSION = True


class TestYsonFunctionsCompositeConversionDisabled(TestYsonFunctionsBase):
    ENABLE_COMPOSITE_CONVERSION = False


class TestYtDictionaries(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_int_key_flat(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "schema": [
                    {"name": "key", "type": "uint64", "required": True},
                    {"name": "value_str", "type": "string", "required": True},
                    {"name": "value_i64", "type": "int64", "required": True},
                ]
            },
        )
        write_table("//tmp/dict", [{"key": i, "value_str": "str" + str(i), "value_i64": i * i} for i in [1, 3, 5]])

        with Clique(
            1,
            config_patch={
                "clickhouse": {
                    "dictionaries": [
                        {
                            "name": "dict",
                            "layout": {"flat": {}},
                            "structure": {
                                "id": {"name": "key"},
                                "attribute": [
                                    {"name": "value_str", "type": "String", "null_value": "n/a"},
                                    {"name": "value_i64", "type": "Int64", "null_value": 42},
                                ],
                            },
                            "lifetime": 0,
                            "source": {"yt": {"path": "//tmp/dict"}},
                        }
                    ]
                }
            },
        ) as clique:
            result = clique.make_query(
                "select number, dictGetString('dict', 'value_str', number) as str, "
                "dictGetInt64('dict', 'value_i64', number) as i64 from numbers(5)"
            )
        assert result == [
            {"number": 0, "str": "n/a", "i64": 42},
            {"number": 1, "str": "str1", "i64": 1},
            {"number": 2, "str": "n/a", "i64": 42},
            {"number": 3, "str": "str3", "i64": 9},
            {"number": 4, "str": "n/a", "i64": 42},
        ]

    @authors("max42")
    def test_composite_key_hashed(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "schema": [
                    {"name": "key", "type": "string", "required": True},
                    {"name": "subkey", "type": "int64", "required": True},
                    {"name": "value", "type": "string", "required": True},
                ]
            },
        )
        write_table(
            "//tmp/dict",
            [
                {"key": "a", "subkey": 1, "value": "a1"},
                {"key": "a", "subkey": 2, "value": "a2"},
                {"key": "b", "subkey": 1, "value": "b1"},
            ],
        )

        create(
            "table",
            "//tmp/queries",
            attributes={
                "schema": [
                    {"name": "key", "type": "string", "required": True},
                    {"name": "subkey", "type": "int64", "required": True},
                ]
            },
        )
        write_table(
            "//tmp/queries",
            [
                {"key": "a", "subkey": 1},
                {"key": "a", "subkey": 2},
                {"key": "b", "subkey": 1},
                {"key": "b", "subkey": 2},
            ],
        )

        with Clique(
            1,
            config_patch={
                "clickhouse": {
                    "dictionaries": [
                        {
                            "name": "dict",
                            "layout": {"complex_key_hashed": {}},
                            "structure": {
                                "key": {
                                    "attribute": [
                                        {"name": "key", "type": "String"},
                                        {"name": "subkey", "type": "Int64"},
                                    ]
                                },
                                "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}],
                            },
                            "lifetime": 0,
                            "source": {"yt": {"path": "//tmp/dict"}},
                        }
                    ]
                }
            },
        ) as clique:
            result = clique.make_query(
                "select dictGetString('dict', 'value', tuple(key, subkey)) as value from \"//tmp/queries\""
            )
        assert result == [{"value": "a1"}, {"value": "a2"}, {"value": "b1"}, {"value": "n/a"}]

    @authors("max42")
    def test_lifetime(self):
        create(
            "table",
            "//tmp/dict",
            attributes={
                "schema": [
                    {"name": "key", "type": "uint64", "required": True},
                    {"name": "value", "type": "string", "required": True},
                ]
            },
        )
        write_table("//tmp/dict", [{"key": 42, "value": "x"}])

        patch = {
            # Disable background update.
            "yt": {
                "table_attribute_cache": get_async_expiring_cache_config(2000, 2000, None),
                "permission_cache": get_async_expiring_cache_config(2000, 2000, None),
            },
            "clickhouse": {
                "dictionaries": [
                    {
                        "name": "dict",
                        "layout": {"flat": {}},
                        "structure": {
                            "id": {"name": "key"},
                            "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}],
                        },
                        "lifetime": 1,
                        "source": {"yt": {"path": "//tmp/dict"}},
                    }
                ]
            },
        }

        with Clique(1, config_patch=patch) as clique:
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "x"
            )

            write_table("//tmp/dict", [{"key": 42, "value": "y"}])
            # TODO(max42): make update time customizable in CH and reduce this constant.
            time.sleep(7)
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "y"
            )

            remove("//tmp/dict")
            time.sleep(7)
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "y"
            )

            create(
                "table",
                "//tmp/dict",
                attributes={
                    "schema": [
                        {"name": "key", "type": "uint64", "required": True},
                        {"name": "value", "type": "string", "required": True},
                    ]
                },
            )
            write_table("//tmp/dict", [{"key": 42, "value": "z"}])
            time.sleep(7)
            assert (
                clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"]
                == "z"
            )


class TestClickHouseSchema(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("evgenstf")
    def test_int_types(self):
        with Clique(5) as clique:
            create(
                "table",
                "//tmp/test_table",
                attributes={
                    "schema": [
                        {"name": "int64_value", "type": "int64"},
                        {"name": "int32_value", "type": "int32"},
                        {"name": "int16_value", "type": "int16"},
                        {"name": "int8_value", "type": "int8"},
                        {"name": "uint64_value", "type": "uint64"},
                        {"name": "uint32_value", "type": "uint32"},
                        {"name": "uint16_value", "type": "uint16"},
                        {"name": "uint8_value", "type": "uint8"},
                    ]
                },
            )
            name_to_expected_type = {
                "int64_value": "Nullable(Int64)",
                "int32_value": "Nullable(Int32)",
                "int16_value": "Nullable(Int16)",
                "int8_value": "Nullable(Int8)",
                "uint64_value": "Nullable(UInt64)",
                "uint32_value": "Nullable(UInt32)",
                "uint16_value": "Nullable(UInt16)",
                "uint8_value": "Nullable(UInt8)",
            }
            table_description = clique.make_query('describe "//tmp/test_table"')
            for column_description in table_description:
                assert name_to_expected_type[column_description["name"]] == column_description["type"]

    @authors("max42")
    def test_missing_schema(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key": 42, "value": "x"}])

        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('select * from "//tmp/t"')

    def _strip_description(self, rows):
        return [{key: value for key, value in row.iteritems() if key in ("name", "type")} for row in rows]

    @authors("max42")
    def test_common_schema_unsorted(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "d", "type": "double"},
                ]
            },
        )
        create("table", "//tmp/t3", attributes={"schema": [{"name": "a", "type": "string"}]})
        create("table", "//tmp/t4", attributes={"schema": [{"name": "a", "type": "string", "required": True}]})

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})
        write_table("//tmp/t3", {"a": "1"})
        write_table("//tmp/t4", {"a": "2"})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2")')) == [
                {"name": "a", "type": "Nullable(Int64)"}
            ]
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") order by a') == [
                {"a": 17},
                {"a": 42},
            ]

            with raises_yt_error(QueryFailedError):
                clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2", "//tmp/t3")')

            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t3", "//tmp/t4")')) == [
                {"name": "a", "type": "Nullable(String)"}
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t4")')) == [
                {"name": "a", "type": "String"}
            ]

            assert sorted(clique.make_query('select * from concatYtTables("//tmp/t3", "//tmp/t4")')) == [
                {"a": "1"},
                {"a": "2"},
            ]

    @authors("max42")
    def test_common_schema_sorted(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "string", "sort_order": "ascending"},
                    {"name": "c", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "d", "type": "double"},
                ]
            },
        )
        create(
            "table",
            "//tmp/t3",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64"},
                    {"name": "c", "type": "double"},
                ]
            },
        )

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t2")')) == [
                {"name": "a", "type": "Nullable(Int64)"}
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t2", "//tmp/t1")')) == [
                {"name": "a", "type": "Nullable(Int64)"}
            ]
            assert self._strip_description(clique.make_query('describe concatYtTables("//tmp/t1", "//tmp/t3")')) == [
                {"name": "a", "type": "Nullable(Int64)"},
                {"name": "c", "type": "Nullable(Float64)"},
            ]

    @authors("max42")
    def test_nulls_in_primary_key(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})

        content = [{"a": None}, {"a": -1}, {"a": 42}]
        write_table("//tmp/t", content)

        with Clique(1) as clique:
            for source in ['"//tmp/t"', "concatYtTables('//tmp/t')"]:
                assert clique.make_query("select * from {}".format(source)) == content
                assert clique.make_query("select * from {} where isNull(a)".format(source)) == [{"a": None}]
                assert clique.make_query("select * from {} where isNotNull(a)".format(source)) == [{"a": -1}, {"a": 42}]


class TestQueryLog(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def _get_engine_specification(self, period):
        return (
            "ENGINE = Buffer('{{database}}', '{{underlying_table_name}}', 1, 1, {}, "
            "1000000000000, 1000000000000, 1000000000000, 1000000000000)".format(period)
        )

    @authors("max42")
    def test_query_log_simple(self):
        with Clique(
            1, config_patch={"clickhouse": {"query_log": {"engine": self._get_engine_specification(1)}}}
        ) as clique:
            clique.make_query("select 1")
            wait(lambda: len(clique.make_query("select * from system.query_log")) >= 1)
            time.sleep(6)
            assert len(clique.make_query("select * from system.query_log")) == 0

    @authors("max42")
    @flaky(max_runs=5)
    def test_query_log_eviction(self):
        period = 3

        with Clique(
            1, config_patch={"clickhouse": {"query_log": {"engine": self._get_engine_specification(period)}}}
        ) as clique:
            timespan = 15
            counter = 0
            start = time.time()

            identifier_to_state = dict()

            while True:
                now = time.time() - start
                if now > timespan:
                    break
                identifier = "foo{:03}".format(counter)
                assert clique.make_query("select 1 as {}".format(identifier), verbose=False)[0] == {identifier: 1}
                counter += 1
                identifier_to_state[identifier] = {"state": "unseen", "started_at": now}

                all_entries = clique.make_query("select * from system.query_log", verbose=False)
                seen_now = {None}
                for entry in all_entries:
                    if not entry["query"]:
                        continue
                    foo_index = entry["query"].find("foo")
                    if foo_index != -1:
                        found_identifier = entry["query"][foo_index : foo_index + 6]
                        seen_now.add(found_identifier)
                        state = identifier_to_state[found_identifier]
                        if state["state"] == "unseen":
                            state["state"] = "seen"
                            state["seen_at"] = now
                for identifier, state in identifier_to_state.iteritems():
                    if identifier in seen_now:
                        continue
                    if state["state"] == "seen":
                        state["state"] = "evicted"
                        state["evicted_at"] = now

        for identifier, state in sorted(identifier_to_state.items(), key=lambda pair: pair[0]):
            print_debug(identifier, state)
            if state["state"] in ("seen", "evicted"):
                assert state["seen_at"] - state["started_at"] <= 0.5
            if state["state"] == "evicted":
                assert state["evicted_at"] - state["started_at"] >= period * 0.5
                assert state["evicted_at"] - state["started_at"] <= period * 3.5

            if state["started_at"] <= 1.5 * period:
                assert state["state"] == "evicted"
            if state["started_at"] <= 3.5 * period:
                assert state["state"] in ("seen", "evicted")


class TestQueryRegistry(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_query_registry(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 0}])
        with Clique(1, config_patch={"yt": {"process_list_snapshot_update_period": 100}}) as clique:
            monitoring_port = clique.get_active_instances()[0].attributes["monitoring_port"]

            def check_query_registry():
                query_registry = requests.get("http://localhost:{}/orchid/queries".format(monitoring_port)).json()
                running_queries = list(query_registry["running_queries"].values())
                print_debug(running_queries)
                if len(running_queries) < 2:
                    return False
                assert len(running_queries) == 2
                qi = running_queries[0]
                qs = running_queries[1]
                if qi["query_kind"] != "initial_query":
                    qi, qs = qs, qi
                print_debug("Initial: ", pprint.pformat(qi))
                print_debug("Secondary: ", pprint.pformat(qs))
                assert qi["query_kind"] == "initial_query"
                assert qs["query_kind"] == "secondary_query"
                assert "initial_query" in qs
                assert qs["initial_query_id"] == qi["query_id"]
                if qs["query_status"] is None or qi["query_status"] is None:
                    return False
                return True

            t = clique.make_async_query('select sleep(3) from "//tmp/t"')
            wait(lambda: check_query_registry())
            t.join()

    @authors("max42")
    def test_codicils(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "foo"}])

        # Normal footprint at start of clique should around 0.5 GiB - 1.5 GiB.
        oom_allocation_size = 2 * 1024 ** 3

        def assert_in_error(config_patch=None, message=None, allocation_size=0):
            print_debug("Checking config_patch={} for '{}' presense".format(config_patch, message))
            clique = Clique(1, config_patch=config_patch)
            with pytest.raises(YtError):
                clique.__enter__()
                clique.make_query(
                    "select sleep(3) from `//tmp/t` settings chyt_testing_subquery_allocation_size = {}".format(
                        allocation_size
                    ),
                    verbose=False,
                )
            clique.op.track(False, False)
            assert message in str(clique.op.get_error())
            with pytest.raises(YtError):
                clique.__exit__(None, None, None)

        def assert_no_error(config_patch=None, completed_job_count=0, allocation_size=0):
            print_debug("Checking config_patch={} for no error".format(config_patch))
            with Clique(1, config_patch=config_patch) as clique:
                assert clique.make_query(
                    "select sleep(3) from `//tmp/t` settings chyt_testing_subquery_allocation_size = {}".format(
                        allocation_size
                    ),
                    verbose=False,
                ) == [{"sleep(3)": 0}]
                # NB: interrupted jobs are considered to be lost.
                wait(lambda: clique.op.get_job_count("lost") >= completed_job_count)

        assert_no_error()
        assert_in_error(
            config_patch={
                "yt": {
                    "memory_watchdog": {
                        "memory_limit": 10 * 1024 ** 3,
                        "period": 50,
                        "codicil_watermark": 8 * 1024 ** 3,
                    }
                }
            },
            message="because memory usage",
            allocation_size=oom_allocation_size,
        )
        assert_no_error(
            config_patch={
                "yt": {
                    "memory_watchdog": {
                        "memory_limit": 10 * 1024 ** 3,
                        "period": 50,
                        "window_codicil_watermark": 8 * 1024 ** 3,
                        "window_width": 200,
                    }
                }
            },
            allocation_size=oom_allocation_size,
            completed_job_count=1,
        )

    @authors("max42")
    @pytest.mark.skipif(True, reason="temporarily broken")
    def test_datalens_header(self):
        with Clique(1) as clique:
            t = clique.make_async_query_via_proxy("select sleep(3)", headers={"X-Request-Id": "ummagumma"})
            wait(
                lambda: "ummagumma"
                in str(clique.get_orchid(clique.get_active_instances()[0], "/queries/running_queries")),
                iter=10,
            )
            t.join()


class TestJoinAndIn(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_global_join(self):
        create(
            "table",
            "//tmp/t1",
            attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]},
        )
        create(
            "table",
            "//tmp/t2",
            attributes={"schema": [{"name": "c", "type": "int64"}, {"name": "d", "type": "string"}]},
        )
        create(
            "table",
            "//tmp/t3",
            attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "e", "type": "double"}]},
        )
        write_table("//tmp/t1", [{"a": 42, "b": "qwe"}, {"a": 27, "b": "xyz"}])
        write_table("//tmp/t2", [{"c": 42, "d": "asd"}, {"c": -1, "d": "xyz"}])
        write_table("//tmp/t3", [{"a": 42, "e": 3.14}, {"a": 27, "e": 2.718}])
        with Clique(1) as clique:
            expected = [{"a": 42, "b": "qwe", "c": 42, "d": "asd"}]
            assert clique.make_query('select * from "//tmp/t1" t1 global join "//tmp/t2" t2 on t1.a = t2.c') == expected
            assert clique.make_query('select * from "//tmp/t1" t1 global join "//tmp/t2" t2 on t2.c = t1.a') == expected

            expected_on = [{"a": 27, "b": "xyz", "t3.a": 27, "e": 2.718}, {"a": 42, "b": "qwe", "t3.a": 42, "e": 3.14}]
            expected_using = [{"a": 27, "b": "xyz", "e": 2.718}, {"a": 42, "b": "qwe", "e": 3.14}]
            assert (
                clique.make_query('select * from "//tmp/t1" t1 global join "//tmp/t3" t3 using a order by a')
                == expected_using
            )
            assert (
                clique.make_query('select * from "//tmp/t1" t1 global join "//tmp/t3" t3 on t1.a = t3.a order by a')
                == expected_on
            )
            assert (
                clique.make_query('select * from "//tmp/t1" t1 global join "//tmp/t3" t3 on t3.a = t1.a order by a')
                == expected_on
            )
            assert (
                clique.make_query('select * from "//tmp/t1" t1 global join "//tmp/t3" t3 on t1.a = t3.a order by t1.a')
                == expected_on
            )
            assert (
                clique.make_query('select * from "//tmp/t1" t1 global join "//tmp/t3" t3 on t3.a = t1.a order by t3.a')
                == expected_on
            )

            with raises_yt_error(QueryFailedError):
                # When JOIN is present, all joined expressions should be provided with aliases.
                assert clique.make_query('select * from "//tmp/t1" global join "//tmp/t2" on a = c') == expected
                assert clique.make_query('select * from "//tmp/t1" global join "//tmp/t2" on c = a') == expected
                assert (
                    clique.make_query('select * from "//tmp/t1" global join "//tmp/t3" using a order by a')
                    == expected_using
                )

    @authors("max42")
    def test_global_in(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64", "required": True}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "a", "type": "int64", "required": True}]})
        write_table("//tmp/t1", [{"a": 1}, {"a": 3}, {"a": -42}])
        write_table("//tmp/t2", [{"a": 5}, {"a": 42}, {"a": 3}, {"a": 1}])
        with Clique(1) as clique:
            expected = [{"a": 1}, {"a": 3}]
            assert (
                clique.make_query('select a from "//tmp/t1" where a global in (select * from "//tmp/t2") order by a')
                == expected
            )
            assert (
                clique.make_query('select a from "//tmp/t2" where a global in (select * from "//tmp/t1") order by a')
                == expected
            )

            assert clique.make_query('select toInt64(42) global in (select * from "//tmp/t2")')[0].values() == [1]
            assert clique.make_query('select toInt64(43) global in (select * from "//tmp/t2")')[0].values() == [0]

            assert clique.make_query('select toInt64(42) global in (select * from "//tmp/t2") from "//tmp/t1" limit 1')[
                0
            ].values() == [1]
            assert clique.make_query('select toInt64(43) global in (select * from "//tmp/t2") from "//tmp/t1" limit 1')[
                0
            ].values() == [0]

    @authors("max42")
    def test_sorted_join_simple(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "lhs", "type": "string", "required": True},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "rhs", "type": "string", "required": True},
                ]
            },
        )
        lhs_rows = [
            [{"key": 1, "lhs": "foo1"}],
            [{"key": 2, "lhs": "foo2"}, {"key": 3, "lhs": "foo3"}],
            [{"key": 4, "lhs": "foo4"}],
        ]
        rhs_rows = [
            [{"key": 1, "rhs": "bar1"}, {"key": 2, "rhs": "bar2"}],
            [{"key": 3, "rhs": "bar3"}, {"key": 4, "rhs": "bar4"}],
        ]

        for rows in lhs_rows:
            write_table("<append=%true>//tmp/t1", rows)
        for rows in rhs_rows:
            write_table("<append=%true>//tmp/t2", rows)

        with Clique(3) as clique:
            expected = [
                {"key": 1, "lhs": "foo1", "rhs": "bar1"},
                {"key": 2, "lhs": "foo2", "rhs": "bar2"},
                {"key": 3, "lhs": "foo3", "rhs": "bar3"},
                {"key": 4, "lhs": "foo4", "rhs": "bar4"},
            ]
            assert (
                clique.make_query('select key, lhs, rhs from "//tmp/t1" t1 join "//tmp/t2" t2 using key order by key')
                == expected
            )
            assert (
                clique.make_query(
                    'select key, lhs, rhs from "//tmp/t1" t1 join "//tmp/t2" t2 on t1.key = t2.key order by key'
                )
                == expected
            )

    @authors("max42")
    def test_right_or_full_join_simple(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "lhs", "type": "string", "required": True},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "rhs", "type": "string", "required": True},
                ]
            },
        )
        lhs_rows = [
            {"key": 0, "lhs": "foo0"},
            {"key": 1, "lhs": "foo1"},
            {"key": 3, "lhs": "foo3"},
            {"key": 7, "lhs": "foo7"},
            {"key": 8, "lhs": "foo8"},
        ]
        rhs_rows = [
            {"key": 0, "rhs": "bar0"},
            {"key": 0, "rhs": "bar0"},
            {"key": 2, "rhs": "bar2"},
            {"key": 4, "rhs": "bar4"},
            {"key": 9, "rhs": "bar9"},
        ]

        for row in lhs_rows:
            write_table("<append=%true>//tmp/t1", [row])
        for row in rhs_rows:
            write_table("<append=%true>//tmp/t2", [row])

        with Clique(2) as clique:
            expected_right = [
                {"key": 0, "lhs": "foo0", "rhs": "bar0"},
                {"key": 0, "lhs": "foo0", "rhs": "bar0"},
                {"key": 2, "lhs": None, "rhs": "bar2"},
                {"key": 4, "lhs": None, "rhs": "bar4"},
                {"key": 9, "lhs": None, "rhs": "bar9"},
            ]
            expected_full = [
                {"key": 0, "lhs": "foo0", "rhs": "bar0"},
                {"key": 0, "lhs": "foo0", "rhs": "bar0"},
                {"key": 1, "lhs": "foo1", "rhs": None},
                {"key": 2, "lhs": None, "rhs": "bar2"},
                {"key": 3, "lhs": "foo3", "rhs": None},
                {"key": 4, "lhs": None, "rhs": "bar4"},
                {"key": 7, "lhs": "foo7", "rhs": None},
                {"key": 8, "lhs": "foo8", "rhs": None},
                {"key": 9, "lhs": None, "rhs": "bar9"},
            ]
            assert (
                clique.make_query(
                    'select key, lhs, rhs from "//tmp/t1" t1 global right join "//tmp/t2" t2 using key order by key'
                )
                == expected_right
            )
            assert (
                clique.make_query(
                    'select key, lhs, rhs from "//tmp/t1" t1 global full join "//tmp/t2" t2 using key order by key'
                )
                == expected_full
            )

    @authors("max42")
    def test_sorted_join_stress(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "lhs", "type": "string", "required": True},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "rhs", "type": "string", "required": True},
                ]
            },
        )
        rnd = random.Random(x=42)

        # Small values (uncomment for debugging):
        # row_count, key_range, chunk_count = 5, 7, 2
        # Large values:
        row_count, key_range, chunk_count = 300, 500, 15

        def generate_rows(row_count, key_range, chunk_count, payload_column_name, payload_value):
            keys = [rnd.randint(0, key_range - 1) for i in range(row_count)]
            keys = sorted(keys)
            delimiters = [0] + sorted([rnd.randint(0, row_count) for i in range(chunk_count - 1)]) + [row_count]
            rows = []
            for i in range(chunk_count):
                rows.append(
                    [
                        {"key": key, payload_column_name: "%s%03d" % (payload_value, key)}
                        for key in keys[delimiters[i] : delimiters[i + 1]]
                    ]
                )
            return rows

        def write_multiple_chunks(path, row_batches):
            for row_batch in row_batches:
                write_table("<append=%true>" + path, row_batch)
            print_debug("Table {}".format(path))
            chunk_ids = get(path + "/@chunk_ids", verbose=False)
            for chunk_id in chunk_ids:
                attrs = get("#" + chunk_id + "/@", attributes=["min_key", "max_key", "row_count"], verbose=False)
                print_debug(
                    "{}: rc = {}, bk = ({}, {})".format(
                        chunk_id, attrs["row_count"], attrs["min_key"], attrs["max_key"]
                    )
                )

        lhs_rows = generate_rows(row_count, key_range, chunk_count, "lhs", "foo")
        rhs_rows = generate_rows(row_count, key_range, chunk_count, "rhs", "bar")

        write_multiple_chunks("//tmp/t1", lhs_rows)
        write_multiple_chunks("//tmp/t2", rhs_rows)

        lhs_rows = sum(lhs_rows, [])
        rhs_rows = sum(rhs_rows, [])

        def expected_result(kind, key_range, lhs_rows, rhs_rows):
            it_lhs = 0
            it_rhs = 0
            result = []
            for key in range(key_range):
                start_lhs = it_lhs
                while it_lhs < len(lhs_rows) and lhs_rows[it_lhs]["key"] == key:
                    it_lhs += 1
                finish_lhs = it_lhs
                start_rhs = it_rhs
                while it_rhs < len(rhs_rows) and rhs_rows[it_rhs]["key"] == key:
                    it_rhs += 1
                finish_rhs = it_rhs
                maybe_lhs_null = []
                maybe_rhs_null = []
                if start_lhs == finish_lhs and start_rhs == finish_rhs:
                    continue
                if kind in ("right", "full") and start_lhs == finish_lhs:
                    maybe_lhs_null.append({"key": key, "lhs": None})
                if kind in ("left", "full") and start_rhs == finish_rhs:
                    maybe_rhs_null.append({"key": key, "rhs": None})
                for lhs_row in lhs_rows[start_lhs:finish_lhs] + maybe_lhs_null:
                    for rhs_row in rhs_rows[start_rhs:finish_rhs] + maybe_rhs_null:
                        result.append({"key": key, "lhs": lhs_row["lhs"], "rhs": rhs_row["rhs"]})
            return result

        expected_results = {}
        for kind in ("inner", "left", "right", "full"):
            expected_results[kind] = expected_result(kind, key_range, lhs_rows, rhs_rows)

        # Transform unicode strings into non-unicode.
        def sanitize(smth):
            if type(smth) == unicode:
                return str(smth)
            else:
                return smth

        # Same for dictionaries
        def sanitize_dict(unicode_dict):
            return dict([(sanitize(key), sanitize(value)) for key, value in unicode_dict.iteritems()])

        index = 0
        for instance_count in range(1, 5):
            with Clique(instance_count) as clique:
                # TODO(max42): CHYT-390.
                for lhs_arg in ('"//tmp/t1"', '(select * from "//tmp/t1")'):
                    for rhs_arg in ('"//tmp/t2"', '(select * from "//tmp/t2")'):
                        for globalness in ("", "global"):
                            for kind in ("inner", "left", "right", "full"):
                                query = (
                                    "select key, lhs, rhs from {lhs_arg} l {globalness} {kind} join {rhs_arg} r "
                                    "using key order by key, lhs, rhs nulls first".format(**locals())
                                )
                                result = list(imap(sanitize_dict, clique.make_query(query, verbose=False)))

                                expected = expected_results[kind]
                                print_debug(
                                    "Query #{}: '{}' produced {} rows, expected {} rows".format(
                                        index, query, len(result), len(expected)
                                    )
                                )
                                index += 1
                                result = list(imap(str, result))
                                expected = list(imap(str, expected))
                                if result != expected:
                                    print_debug("Produced:")
                                    for row in result:
                                        char = "+" if row not in expected else " "
                                        print_debug(char + " " + row)
                                    print_debug("Expected:")
                                    for row in expected:
                                        char = "-" if row not in result else " "
                                        print_debug(char + " " + row)
                                    assert False

    @authors("max42")
    def test_tricky_join(self):
        # CHYT-240.
        create(
            "table", "//tmp/t1", attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]}
        )
        create(
            "table", "//tmp/t2", attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]}
        )
        write_table("//tmp/t1", [{"key": 0}, {"key": 1}])
        write_table("<append=%true>//tmp/t1", [{"key": 2}, {"key": 3}])
        write_table("//tmp/t2", [{"key": 0}, {"key": 1}])
        write_table("<append=%true>//tmp/t2", [{"key": 4}, {"key": 5}])
        write_table("<append=%true>//tmp/t2", [{"key": 6}, {"key": 7}])
        write_table("<append=%true>//tmp/t2", [{"key": 8}, {"key": 9}])
        with Clique(2, config_patch={"yt": {"subquery": {"min_data_weight_per_thread": 5000}}}) as clique:
            assert clique.make_query('select * from "//tmp/t1" t1 join "//tmp/t2" t2 using key') == [
                {"key": 0},
                {"key": 1},
            ]

    @authors("max42")
    def test_sorted_join_with_wider_key_condition(self):
        # CHYT-487.
        create("table", "//tmp/t1", attributes={"schema": [
            {"name": "key", "type": "int64", "sort_order": "ascending"}
        ]})
        create("table", "//tmp/t2", attributes={"schema": [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "subkey", "type": "int64", "sort_order": "ascending"}
        ]})
        write_table("//tmp/t1", [{"key": 1}, {"key": 5}, {"key": 7}, {"key": 10}])
        write_table("<append=%true>//tmp/t2", [{"key": 5, "subkey": 42}])
        write_table("<append=%true>//tmp/t2", [{"key": 7, "subkey": -1}])
        write_table("<append=%true>//tmp/t2", [{"key": 10, "subkey": 42}])
        with Clique(1) as clique:
            # TODO(max42): why 7 rows are read? I would expect 6 rows.
            assert clique.make_query_and_validate_row_count(
                'select * from "//tmp/t1" t1 inner join "//tmp/t2" t2 using key where t2.subkey == 42',
                exact=7) == [{"key": 5, "subkey": 42},
                             {"key": 10, "subkey": 42}]

    @authors("max42")
    def test_join_under_different_names(self):
        # CHYT-270.
        create(
            "table", "//tmp/t1", attributes={"schema": [{"name": "key1", "type": "int64", "sort_order": "ascending"}]}
        )
        create(
            "table", "//tmp/t2", attributes={"schema": [{"name": "key2", "type": "int64", "sort_order": "ascending"}]}
        )
        for i in range(3):
            write_table("<append=%true>//tmp/t1", [{"key1": i}])
            write_table("<append=%true>//tmp/t2", [{"key2": i}])
        with Clique(1) as clique:
            assert (
                len(
                    clique.make_query(
                        'select * from "//tmp/t1" A inner join "//tmp/t2" B on A.key1 = B.key2 where '
                        "A.key1 in (1, 2) and B.key2 in (2, 3)"
                    )
                )
                == 1
            )

    @authors("max42")
    def test_tricky_join2(self):
        # CHYT-273.
        create(
            "table", "//tmp/t1", attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]}
        )
        create(
            "table", "//tmp/t2", attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]}
        )
        write_table("//tmp/t1", [{"key": 0, "key": 1}])
        write_table("//tmp/t2", [{"key": 4, "key": 5}])
        with Clique(1) as clique:
            assert (
                len(
                    clique.make_query(
                        'select * from "//tmp/t1" A inner join "//tmp/t2" B on A.key = B.key where ' "A.key = 1"
                    )
                )
                == 0
            )

    @authors("max42")
    def test_forbidden_non_primitive_join(self):
        # CHYT-323.
        create(
            "table", "//tmp/t1", attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]}
        )
        create(
            "table", "//tmp/t2", attributes={"schema": [{"name": "key", "type": "int64", "sort_order": "ascending"}]}
        )
        write_table("//tmp/t1", [{"key": 42}])
        write_table("//tmp/t2", [{"key": 43}])
        with Clique(1) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query('select * from "//tmp/t1" A inner join "//tmp/t2" B on A.key + 1 = B.key')

    @authors("max42")
    def test_cross_join(self):
        # CHYT-445.
        create(
            "table",
            "//tmp/t1_sorted",
            attributes={"schema": [{"name": "key1", "type": "int64", "sort_order": "ascending"}]},
        )
        create("table", "//tmp/t1_not_sorted", attributes={"schema": [{"name": "key1", "type": "int64"}]})
        create(
            "table", "//tmp/t2", attributes={"schema": [{"name": "key2", "type": "int64", "sort_order": "ascending"}]}
        )
        write_table("<append=%true>//tmp/t1_sorted", [{"key1": 1}])
        write_table("<append=%true>//tmp/t1_sorted", [{"key1": 2}])
        write_table("<append=%true>//tmp/t1_not_sorted", [{"key1": 1}])
        write_table("<append=%true>//tmp/t1_not_sorted", [{"key1": 2}])
        write_table("//tmp/t2", [{"key2": 3}, {"key2": 4}])

        def expected_result(left_rows):
            result = []
            for row in left_rows:
                for key2 in (3, 4):
                    result_row = copy.deepcopy(row)
                    result_row["key2"] = key2
                    result.append(result_row)
            return result

        with Clique(1) as clique:
            for tp in ("sorted", "not_sorted"):
                assert clique.make_query(
                    "select * from `//tmp/t1_{}` t1 cross join `//tmp/t2` t2 order by (key1, key2)".format(tp)
                ) == expected_result([{"key1": 1}, {"key1": 2}])
                assert clique.make_query(
                    "select * from `//tmp/t1_{}` t1 cross join `//tmp/t2` t2 where key1 == 1 order by (key1, key2)".format(
                        tp
                    )
                ) == expected_result([{"key1": 1}])


class TestClickHouseHttpProxy(ClickHouseTestBase):
    DELTA_PROXY_CONFIG = {
        "clickhouse": {
            "operation_cache": {
                "refresh_time": 100,
            },
            "permission_cache": {
                "refresh_time": 100,
                "failure_expiration_time": 100,
            },
            "force_enqueue_profiling": True,
        },
    }

    def setup(self):
        self._setup()

    def _get_proxy_metric(self, metric_name):
        return Metric.at_proxy(self.Env.get_http_proxy_address(), metric_name)

    @authors("evgenstf")
    def test_instance_choice(self):
        with Clique(5, spec={"alias": "*test_alias"}) as clique:
            for job_cookie in range(5):
                proxy_response = clique.make_query_via_proxy(
                    "select * from system.clique", database="*test_alias@" + str(job_cookie)
                )
                for instanse_response in proxy_response:
                    assert (
                        instanse_response["self"] == 1
                        if instanse_response["job_cookie"] == job_cookie
                        else instanse_response["self"] == 0
                    )

                proxy_response = clique.make_query_via_proxy(
                    "select * from system.clique", database=clique.op.id + "@" + str(job_cookie)
                )
                for instanse_response in proxy_response:
                    assert (
                        instanse_response["self"] == 1
                        if instanse_response["job_cookie"] == job_cookie
                        else instanse_response["self"] == 0
                    )

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select * from system.clique", database="*test_alias@aaa")

    @authors("dakovalkov")
    def test_http_proxy(self):
        with Clique(1) as clique:
            proxy_response = clique.make_query_via_proxy("select * from system.clique")
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response == response

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 1

            clique.resize(0, [jobs[0]])
            clique.resize(1)

            proxy_response = clique.make_query_via_proxy("select * from system.clique")
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response == response

    @authors("dakovalkov")
    def test_ban_dead_instance_in_proxy(self):
        patch = {
            "yt": {
                "discovery": {
                    # Set big value to prevent unlocking node.
                    "transaction_timeout": 1000000,
                }
            }
        }

        cache_missed_count = self._get_proxy_metric("clickhouse_proxy/discovery_cache/missed")
        force_update_count = self._get_proxy_metric("clickhouse_proxy/force_update_count")
        banned_count = self._get_proxy_metric("clickhouse_proxy/banned_count")

        with Clique(2, config_patch=patch) as clique:
            wait(lambda: clique.get_active_instance_count() == 2)

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            abort_job(jobs[0])
            wait(lambda: len(clique.op.get_running_jobs()) == 1)

            for instance in clique.get_active_instances():
                if str(instance) == jobs[0]:
                    continue
                else:
                    assert clique.make_direct_query(instance, "select 1") == [{"1": 1}]

            proxy_responses = []
            for i in range(50):
                print_debug("Iteration:", i)
                proxy_responses += [clique.make_query_via_proxy("select 1", full_response=True)]
                assert proxy_responses[i].status_code == 200
                assert proxy_responses[i].json()["data"] == proxy_responses[i - 1].json()["data"]
                time.sleep(0.05)
                if banned_count.update().get(verbose=True) == 1:
                    break

            assert proxy_responses[0].json()["data"] == [{"1": 1}]
            assert clique.get_active_instance_count() == 2

        assert cache_missed_count.update().get(verbose=True) == 1
        assert force_update_count.update().get(verbose=True) == 1
        assert banned_count.update().get(verbose=True) == 1

    @authors("dakovalkov")
    def test_ban_stopped_instance_in_proxy(self):
        patch = {
            "interruption_graceful_timeout": 100000,
        }

        cache_missed_count = self._get_proxy_metric("clickhouse_proxy/discovery_cache/missed")
        force_update_count = self._get_proxy_metric("clickhouse_proxy/force_update_count")
        banned_count = self._get_proxy_metric("clickhouse_proxy/banned_count")

        with Clique(2, config_patch=patch) as clique:
            # Add clique into the cache.
            proxy_responses = []
            proxy_responses += [clique.make_query_via_proxy("select 1", full_response=True)]

            instances = clique.get_active_instances()
            assert len(instances) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            self._signal_instance(instances[0].attributes["pid"], "INT")

            with raises_yt_error(QueryFailedError):
                clique.make_direct_query(instances[0], "select 1")
            assert clique.make_direct_query(instances[1], "select 1") == [{"1": 1}]

            for i in range(50):
                print_debug("Iteration:", i)
                proxy_responses += [clique.make_query_via_proxy("select 1", full_response=True)]
                assert proxy_responses[i + 1].status_code == 200
                assert proxy_responses[i].json()["data"] == proxy_responses[i + 1].json()["data"]
                time.sleep(0.05)
                if banned_count.update().get(verbose=True) == 1:
                    break

            assert proxy_responses[0].json()["data"] == [{"1": 1}]
            assert clique.get_active_instance_count() == 1

        assert cache_missed_count.update().get(verbose=True) == 1
        assert force_update_count.update().get(verbose=True) == 1
        assert banned_count.update().get(verbose=True) == 1

    @authors("dakovalkov")
    @pytest.mark.skipif(True, reason="whatever")
    def test_clique_availability(self):
        create("table", "//tmp/table", attributes={"schema": [{"name": "i", "type": "int64"}]})
        write_table("//tmp/table", [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}])
        patch = {
            "interruption_graceful_timeout": 600,
        }

        cache_missed_counter = self._get_proxy_metric("clickhouse_proxy/discovery_cache/missed")
        force_update_counter = self._get_proxy_metric("clickhouse_proxy/force_update_count")
        banned_count = self._get_proxy_metric("clickhouse_proxy/banned_count")

        with Clique(2, max_failed_job_count=2, config_patch=patch) as clique:
            running = True

            def pinger():
                while running:
                    full_response = clique.make_query_via_proxy('select * from "//tmp/table"', full_response=True)
                    print_debug(full_response)
                    print_debug(full_response.json())
                    assert full_response.status_code == 200
                    response = sorted(full_response.json()["data"])
                    assert response == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]
                    time.sleep(0.1)

            ping_thread = threading.Thread(target=pinger)
            ping_thread.start()
            time.sleep(1)

            instances = clique.get_active_instances()
            assert len(instances) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))

            self._signal_instance(instances[0].attributes["pid"], "INT")

            wait(lambda: clique.get_active_instance_count() == 1, iter=10)
            clique.resize(2)

            new_instances = clique.get_active_instances()
            assert len(new_instances) == 2
            assert new_instances != instances

            assert ping_thread.is_alive()
            running = False
            ping_thread.join()

        assert cache_missed_counter.update().get(verbose=True) == 1
        assert force_update_counter.update().get(verbose=True) == 1
        assert banned_count.update().get(verbose=True) == 1

    @authors("max42")
    def test_database_specification(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        with Clique(1, spec={"alias": "*alias"}) as clique:
            assert clique.make_query_via_proxy("select 1 as a", database="*alias")[0] == {"a": 1}
            assert clique.make_query_via_proxy("select 1 as a", database=clique.op.id)[0] == {"a": 1}

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*alia")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="**")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="")

            with raises_yt_error(1915):  # NoSuchOperation
                clique.make_query_via_proxy("select 1 as a", database="1-2-3-4")

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="1-2-3-x")

            clique.op.suspend()

            wait(lambda: get(clique.op.get_path() + "/@suspended"))
            time.sleep(1)

            with raises_yt_error(QueryFailedError):
                clique.make_query_via_proxy("select 1 as a", database="*alias")

            clique.op.resume()

            wait(lambda: not get(clique.op.get_path() + "/@suspended"))
            time.sleep(1)

            assert clique.make_query_via_proxy("select 1 as a", database=clique.op.id)[0] == {"a": 1}

        wait(lambda: clique.get_active_instance_count() == 0)

        time.sleep(1)

        with raises_yt_error(QueryFailedError):
            assert clique.make_query_via_proxy("select 1 as a", database="*alias")[0] == {"a": 1}

        with raises_yt_error(QueryFailedError):
            assert clique.make_query_via_proxy("select 1 as a", database=clique.op.id)[0] == {"a": 1}

    @authors("max42")
    def test_operation_acl_validation(self):
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

        create_user("u1")
        create_user("u2")
        create_user("u3")
        create_group("g")
        add_member("u1", "g")
        add_member("u2", "g")

        allow_g = {"subjects": ["g"], "action": "allow", "permissions": ["read"]}
        deny_u2 = {"subjects": ["u2"], "action": "deny", "permissions": ["read"]}

        with Clique(1, spec={"alias": "*alias", "acl": [allow_g, deny_u2]}) as clique:
            for user in ("u1", "root"):
                assert clique.make_query_via_proxy("select 1 as a", user=user)[0] == {"a": 1}
            for user in ("u2", "u3"):
                with raises_yt_error(901):  # AuthorizationError
                    assert clique.make_query_via_proxy("select 1 as a", user=user)

            update_op_parameters(clique.op.id, parameters={"acl": [allow_g]})
            time.sleep(1)

            for user in ("u1", "u2", "root"):
                assert clique.make_query_via_proxy("select 1 as a", user=user)[0] == {"a": 1}
            for user in ("u3",):
                with raises_yt_error(901):  # AuthorizationError
                    assert clique.make_query_via_proxy("select 1 as a", user=user)

            update_op_parameters(clique.op.id, parameters={"acl": []})
            time.sleep(1)

            for user in ("root",):
                assert clique.make_query_via_proxy("select 1 as a", user=user)[0] == {"a": 1}
            for user in ("u1", "u2", "u3"):
                with raises_yt_error(901):  # AuthorizationError
                    assert clique.make_query_via_proxy("select 1 as a", user=user)


def is_tracing_enabled():
    return "YT_TRACE_DUMP_DIR" in os.environ


class TestTracing(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    @pytest.mark.parametrize("trace_method", ["x-yt-sampled", "traceparent", "chyt.enable_tracing"])
    def test_tracing_via_http_proxy(self, trace_method):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"a": 2 * i}, {"a": 2 * i + 1}])

            settings = None
            headers = {}
            if trace_method == "x-yt-sampled":
                headers["X-Yt-Sampled"] = "1"
            elif trace_method == "traceparent":
                headers["traceparent"] = "11111111222222223333333344444444-5555555566666666-01"
            else:
                settings = {"chyt.enable_tracing": 1}

            result = clique.make_query_via_proxy(
                'select avg(a) from "//tmp/t"',
                headers=headers,
                full_response=True,
                settings=settings,
            )
            assert abs(result.json()["data"][0]["avg(a)"] - 4.5) < 1e-6
            query_id = result.headers["X-ClickHouse-Query-Id"]
            print_debug("Query id =", query_id)

            if trace_method == "traceparent":
                assert query_id.startswith("33333333-44444444")

            if is_tracing_enabled():
                # TODO(max42): this seems broken after moving to Porto.

                # Check presence of one of the middle parts of query id in the binary trace file.
                # It looks like a good evidence of that everything works fine. Don't tell prime@ that
                # I rely on tracing binary protobuf representation, though :)
                query_id_part = query_id.split("-")[2].rjust(8, "0")
                query_id_part_binary = "".join(
                    chr(int(a, 16) * 16 + int(b, 16)) for a, b in reversed(zip(query_id_part[::2], query_id_part[1::2]))
                )

                time.sleep(2)

                pid = clique.make_query("select pid from system.clique")[0]["pid"]
                tracing_file = open(os.path.join(os.environ["YT_TRACE_DUMP_DIR"], "ytserver-clickhouse." + str(pid)))
                content = tracing_file.read()
                assert query_id_part_binary in content
            else:
                pytest.skip("Rest of this test is not working because YT_TRACE_DUMP_DIR is not in env")

    @authors("max42")
    @pytest.mark.skipif(not is_tracing_enabled(), reason="YT_TRACE_DUMP_DIR should be in env")
    def test_large_tracing(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        write_table("//tmp/t", [{"a": "a"}])
        merge(in_=["//tmp/t"] * 100, out="//tmp/t")

        with Clique(5) as clique:
            assert clique.make_query('select count(*) from "//tmp/t"')[0]["count()"] == 100


class TestClickHouseWithLogTailer(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("gritukan")
    def test_log_tailer(self):
        clique_index = Clique.clique_index

        # Prepare log tailer config and upload it to Cypress.
        log_tailer_config = get_log_tailer_config()
        log_file_path = os.path.join(
            self.path_to_run, "logs", "clickhouse-{}".format(clique_index), "clickhouse-{}.debug.log".format(0)
        )

        log_table = "//sys/clickhouse/logs/log"
        log_tailer_config["log_tailer"]["log_files"] = [{"path": log_file_path, "tables": [{"path": log_table}]}]

        log_tailer_config["logging"]["writers"]["debug"]["file_name"] = os.path.join(
            self.path_to_run, "logs", "clickhouse-{}".format(clique_index), "log_tailer-{}.debug.log".format(0)
        )
        log_tailer_config["cluster_connection"] = self.__class__.Env.configs["driver"]
        log_tailer_config_filename = "//sys/clickhouse/log_tailer_config.yson"
        create("file", log_tailer_config_filename)
        write_file(log_tailer_config_filename, yson.dumps(log_tailer_config, yson_format="pretty"))

        # Create dynamic tables for logs.
        create_tablet_cell_bundle("sys")
        sync_create_cells(1, tablet_cell_bundle="sys")

        create("map_node", "//sys/clickhouse/logs")

        create(
            "table",
            log_table,
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "timestamp", "type": "string", "sort_order": "ascending"},
                    {"name": "increment", "type": "uint64", "sort_order": "ascending"},
                    {"name": "category", "type": "string"},
                    {"name": "message", "type": "string"},
                    {"name": "log_level", "type": "string"},
                    {"name": "thread_id", "type": "string"},
                    {"name": "fiber_id", "type": "string"},
                    {"name": "trace_id", "type": "string"},
                    {"name": "job_id", "type": "string"},
                    {"name": "operation_id", "type": "string"},
                ],
                "tablet_cell_bundle": "sys",
                "atomicity": "none",
            },
        )

        sync_mount_table(log_table)

        # Create log tailer user and make it superuser.
        create_user("yt-log-tailer")
        add_member("yt-log-tailer", "superusers")

        # Create clique with log tailer enabled.
        with Clique(
            instance_count=1,
            cypress_ytserver_log_tailer_config_path=log_tailer_config_filename,
            host_ytserver_log_tailer_path=HOST_PATHS["ytserver-log-tailer"],
            enable_log_tailer=True,
        ) as clique:

            # Make some queries.
            create(
                "table",
                "//tmp/t",
                attributes={
                    "schema": [
                        {"name": "key1", "type": "string"},
                        {"name": "key2", "type": "string"},
                        {"name": "value", "type": "int64"},
                    ]
                },
            )
            for i in range(5):
                write_table(
                    "<append=%true>//tmp/t",
                    [{"key1": "dream", "key2": "theater", "value": i * 5 + j} for j in range(5)],
                )
            total = 24 * 25 // 2

            for _ in range(10):
                result = clique.make_query('select key1, key2, sum(value) from "//tmp/t" group by key1, key2')
                assert result == [{"key1": "dream", "key2": "theater", "sum(value)": total}]

        # Freeze table to flush logs.
        freeze_table(log_table)
        wait_for_tablet_state(log_table, "frozen")

        # Check whether log was written.
        try:
            assert len(read_table(log_table)) > 0
        except:
            remove(log_table)
            raise
        remove(log_table)


class TestClickHouseDynamicTables(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def _get_config_patch(self):
        return {
            "yt": {
                "enable_dynamic_tables": True,
                "subquery": {
                    "min_data_weight_per_thread": 0,
                },
            },
        }

    def _create_simple_dynamic_table(self, path, sort_order="ascending", **attributes):
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    @authors("max42")
    def test_simple(self):
        create(
            "table",
            "//tmp/dt",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "enable_dynamic_store_read": True,
            },
        )
        sync_mount_table("//tmp/dt")

        data = [{"key": i, "value": "foo" + str(i)} for i in range(10)]

        for i in range(10):
            insert_rows("//tmp/dt", [data[i]])

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select * from `//tmp/dt` order by key") == data
            assert clique.make_query("select value from `//tmp/dt` where key == 5 order by key") == [{"value": "foo5"}]
            assert clique.make_query("select key from `//tmp/dt` where value == 'foo7' order by key") == [{"key": 7}]

            sync_unmount_table("//tmp/dt")

            assert clique.make_query("select * from `//tmp/dt` order by key") == data
            assert clique.make_query("select value from `//tmp/dt` where key == 5 order by key") == [{"value": "foo5"}]
            assert clique.make_query("select key from `//tmp/dt` where value == 'foo7' order by key") == [{"key": 7}]

            sync_mount_table("//tmp/dt")

            assert clique.make_query("select * from `//tmp/dt` order by key") == data
            assert clique.make_query("select value from `//tmp/dt` where key == 5 order by key") == [{"value": "foo5"}]
            assert clique.make_query("select key from `//tmp/dt` where value == 'foo7' order by key") == [{"key": 7}]

            with raises_yt_error("CHYT-462"):
                assert clique.make_query("select value from `//tmp/dt` prewhere key == 5 order by key") == [
                    {"value": "foo5"}
                ]

    # Tests below are obtained from similar already existing tests on dynamic tables.

    @authors("max42", "savrus")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_map_on_dynamic_table(self, instance_count):
        self._create_simple_dynamic_table("//tmp/t", sort_order="ascending")
        set("//tmp/t/@min_compaction_store_count", 5)

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        with Clique(instance_count, config_patch=self._get_config_patch()) as clique:
            assert_items_equal(clique.make_query("select * from `//tmp/t`"), rows)

        rows1 = [{"key": i, "value": str(i + 1)} for i in range(3)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows1)
        sync_unmount_table("//tmp/t")

        rows2 = [{"key": i, "value": str(i + 2)} for i in range(2, 6)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows2)
        sync_unmount_table("//tmp/t")

        rows3 = [{"key": i, "value": str(i + 3)} for i in range(7, 8)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows3)
        sync_unmount_table("//tmp/t")

        assert len(get("//tmp/t/@chunk_ids")) == 4

        def update(new):
            def update_row(row):
                for r in rows:
                    if r["key"] == row["key"]:
                        r["value"] = row["value"]
                        return
                rows.append(row)

            for row in new:
                update_row(row)

        update(rows1)
        update(rows2)
        update(rows3)

        with Clique(instance_count, config_patch=self._get_config_patch()) as clique:
            assert_items_equal(clique.make_query("select * from `//tmp/t`"), rows)

    @authors("max42", "savrus")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_dynamic_table_timestamp(self, instance_count):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=False)

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)

        time.sleep(1)
        ts = generate_timestamp()

        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": str(i + 1)} for i in range(2)])
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        settings = {"chyt.dynamic_table.enable_dynamic_store_read": 0}

        with Clique(instance_count, config_patch=self._get_config_patch()) as clique:
            assert_items_equal(clique.make_query("select * from `<timestamp=%s>//tmp/t`" % ts, settings=settings), rows)

            # TODO(max42): these checks are not working for now but TBH I can't imagine
            # anybody using dynamic table timestamps in CHYT.
            # To make them work, invoke ValidateDynamicTableTimestamp helper somewhere
            # in table preparation pipeline. Note that required table attributes are
            # taken from cache, thus are stale.
            #
            # with pytest.raises(YtError):
            #     clique.make_query("select * from `<timestamp=%s>//tmp/t`" % MinTimestamp)
            # insert_rows("//tmp/t", rows)
            # with pytest.raises(YtError):
            #     clique.make_query("select * from `<timestamp=%s>//tmp/t`" % generate_timestamp())

    @authors("max42", "ifsmirnov")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_basic_read1(self, instance_count):
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(3000)]

        insert_rows("//tmp/t", rows[:1500])

        with Clique(instance_count, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select * from `//tmp/t` order by key", verbose=False) == rows[:1500]
            assert clique.make_query("select * from `//tmp/t[100:500]` order by key", verbose=False) == rows[100:500]

            ts = generate_timestamp()
            ypath_with_ts = "<timestamp={}>//tmp/t".format(ts)

            insert_rows("//tmp/t", rows[1500:])

            assert clique.make_query("select * from `//tmp/t` order by key", verbose=False) == rows
            assert (
                clique.make_query("select * from `{}` order by key".format(ypath_with_ts), verbose=False) == rows[:1500]
            )

            sync_freeze_table("//tmp/t")
            assert clique.make_query("select * from `//tmp/t` order by key", verbose=False) == rows

    @authors("dakovalkov")
    def test_enable_dynamic_store_read(self):
        schema = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        rows = [{"key": i, "value": str(i)} for i in range(30)]

        create("table", "//tmp/t", attributes={"schema": schema}, force=True)
        write_table("//tmp/t", rows)

        create_dynamic_table(
            "//tmp/dyn_on",
            schema=schema,
            enable_dynamic_store_read=True,
            dynamic_store_auto_flush_period=yson.YsonEntity(),
        )
        sync_mount_table("//tmp/dyn_on")
        insert_rows("//tmp/dyn_on", rows)

        create_dynamic_table(
            "//tmp/dyn_off",
            schema=schema,
            enable_dynamic_store_read=False,
            dynamic_store_auto_flush_period=yson.YsonEntity(),
        )
        sync_mount_table("//tmp/dyn_off")
        insert_rows("//tmp/dyn_off", rows)

        settings = {"chyt.dynamic_table.enable_dynamic_store_read": 0}

        with Clique(2, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select * from `//tmp/t`") == rows
            assert clique.make_query("select * from `//tmp/t`", settings=settings) == rows

            assert clique.make_query("select * from `//tmp/dyn_on`") == rows
            assert clique.make_query("select * from `//tmp/dyn_on`", settings=settings) == []

            with pytest.raises(YtError):
                clique.make_query("select * from `//tmp/dyn_off`")
            assert clique.make_query("select * from `//tmp/dyn_off`", settings=settings) == []

    @authors("dakovalkov")
    def test_write_to_dynamic_table(self):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            clique.make_query("insert into `//tmp/t` select number as key, toString(number) as value from numbers(10)")
            assert sorted(read_table("//tmp/t")) == [{"key": i, "value": str(i)} for i in range(10)]

            clique.make_query(
                "insert into `//tmp/t` select number as key, toString(number) as value from numbers(500000)"
            )
            rows = [{"key": i, "value": str(i)} for i in range(500000)]
            # Somehow select * from ... works faster than read_table.
            written_rows = sorted(clique.make_query("select * from `//tmp/t`", verbose=False))
            # "assert rows == written_rows" is a bad idea. In case of error printing diff will take too long.
            # These checks can detect simple failures and avoid printing 500'000 rows.
            assert len(rows) == len(written_rows)
            assert rows[0] == written_rows[0]
            assert rows[-1] == written_rows[-1]
            rows_are_equeal = rows == written_rows
            assert rows_are_equeal

    @authors("dakovalkov")
    def test_write_to_unmounted_dynamic_table(self):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            # To speed up the test we disable the backoff timeout.
            fast_error_settings = {"chyt.dynamic_table.write_retry_backoff": 0}
            with pytest.raises(YtError):
                clique.make_query(
                    "insert into `//tmp/t` select number as key, toString(number) as value from numbers(10)",
                    settings=fast_error_settings,
                )

            t = clique.make_async_query(
                "insert into `//tmp/t` select number as key, toString(number) as value from numbers(20)"
            )
            time.sleep(2)
            sync_mount_table("//tmp/t")
            t.join()
            assert sorted(read_table("//tmp/t")) == [{"key": i, "value": str(i)} for i in range(20)]


class TestColumnarRead(ClickHouseTestBase):
    CONFIG_PATCH = {
        "yt": {
            "settings": {
                "enable_columnar_read": True,
            },
            "table_attribute_cache": {
                "read_from": "follower",
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "refresh_time": 0,
                "expire_after_access_time": 0,
            },
        }
    }

    def setup(self):
        self._setup()

    def _check_single_column(self, clique, type, required, values):
        create(
            "table",
            "//tmp/t",
            attributes={"schema": [{"name": "x", "type": type, "required": required}], "optimize_for": "scan"},
            force=True,
        )
        write_table("//tmp/t", [{"x": value} for value in values])

        data = [row["x"] for row in clique.make_query("select * from `//tmp/t`")]
        assert data == values

    @authors("babenko")
    def test_integer(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            for type in ["int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"]:
                self._check_single_column(clique, type, True, [i for i in xrange(10)])
                self._check_single_column(clique, type, True, [77] * 1000)
                self._check_single_column(clique, type, True, [1, 2, 3] * 100)
                self._check_single_column(clique, type, True, [11] * 1000 + [22] * 1000 + [33] * 1000)

                self._check_single_column(clique, type, False, [i if i % 3 == 0 else None for i in xrange(10)])
                self._check_single_column(clique, type, False, [None] * 1000)
                self._check_single_column(clique, type, False, [1, 2, 3, None] * 100)
                self._check_single_column(clique, type, False, [1] * 1000 + [None] * 1000 + [2] * 1000)

    @authors("babenko")
    def test_signed_integer(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            for type in [
                "int8",
                "int16",
                "int32",
                "int64",
            ]:
                self._check_single_column(clique, type, True, [-i for i in xrange(10)])

    @authors("babenko")
    def test_boolean(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            self._check_single_column(clique, "boolean", True, [i % 2 == 0 for i in xrange(10)])
            self._check_single_column(clique, "boolean", True, [False] * 100 + [True] * 200 + [True] * 100)

            self._check_single_column(clique, "boolean", False, [True, None, False, None, True, True, None, False])

    @authors("babenko")
    def test_floating_point(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            for type in ["double", "float"]:
                self._check_single_column(clique, type, True, [1.0, 2.0, 3.14, 2.7])

                self._check_single_column(clique, type, False, [1.0, 2.0, None, 3.14, 2.7, None])

    @authors("babenko")
    def test_string(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            self._check_single_column(clique, "string", True, ["hello", "world"])
            self._check_single_column(clique, "string", True, ["hello", "world"] * 100)
            self._check_single_column(clique, "string", True, ["hello"] * 1000)

            self._check_single_column(clique, "string", True, ["\x00" * 10, "some\x00nulls\x00inside", ""])

            self._check_single_column(clique, "string", False, ["hello", None, "world"])
            self._check_single_column(clique, "string", False, ["hello", "world", None] * 100)
            self._check_single_column(clique, "string", False, ["hello"] * 1000 + [None] * 1000 + ["world"] * 1000)

    @authors("babenko")
    def test_yson(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            create(
                "table",
                "//tmp/t",
                attributes={"schema": [{"name": "x", "type": "any"}], "optimize_for": "scan"},
                force=True,
            )
            row = {"x": {"some": {"yson": True}}}
            write_table("//tmp/t", [row])
            assert yson.loads(clique.make_query("select * from `//tmp/t`")[0]["x"]) == row["x"]

    @authors("babenko")
    def test_any_upcast(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            schema = [
                {"name": "int64", "type": "int64"},
                {"name": "uint64", "type": "uint64"},
                {"name": "boolean", "type": "boolean"},
                {"name": "double", "type": "double"},
                {"name": "float", "type": "float"},
                {"name": "string", "type": "string"},
                {"name": "datetime", "type": "datetime"},
                {"name": "date", "type": "date"},
                {"name": "timestamp", "type": "timestamp"},
                {"name": "interval", "type": "interval"},
                {"name": "any", "type": "any"},
            ]
            create("table", "//tmp/s1", attributes={"schema": schema, "optimize_for": "scan"})
            create(
                "table",
                "//tmp/s2",
                attributes={"schema": [{"name": x["name"], "type": "any"} for x in schema], "optimize_for": "scan"},
            )
            row = {
                "int64": -123,
                "uint64": 456,
                "boolean": True,
                "double": 3.14,
                "float": -2.0,
                "string": "text",
                "datetime": 1,
                "date": 2,
                "timestamp": 3,
                "interval": 4,
                "any": {"hello": "world"},
            }
            null_row = {key: None for key, value in row.iteritems()}
            write_table("//tmp/s1", [row, null_row])
            merge(in_="//tmp/s1", out="//tmp/s2")
            fields = ""
            for column in schema:
                if len(fields) > 0:
                    fields += ", "
                fields += "ConvertYson({}, 'text') as {}".format(column["name"], column["name"])
            results = clique.make_query("select {} from `//tmp/s2`".format(fields))
            for key, value in results[0].iteritems():
                assert yson.loads(value) == row[key]
            for key, value in results[1].iteritems():
                assert value is None

    @authors("babenko")
    def test_missing_column_becomes_null(self):
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            create("table", "//tmp/s1", attributes={"schema": [{"name": "a", "type": "int64"}], "optimize_for": "scan"})
            create(
                "table",
                "//tmp/s2",
                attributes={
                    "schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}],
                    "optimize_for": "scan",
                },
            )
            write_table("//tmp/s1", [{"a": 123}])
            merge(in_="//tmp/s1", out="//tmp/s2")
            assert clique.make_query("select * from `//tmp/s2`")[0] == {"a": 123, "b": None}

    @authors("babenko")
    def test_date_types(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "datetime", "type": "datetime"},
                    {"name": "date", "type": "date"},
                    {"name": "timestamp", "type": "timestamp"},
                    {"name": "interval_", "type": "interval"},
                ],
                "optimize_for": "scan",
            },
        )
        write_table(
            "//tmp/t1",
            [
                {
                    "datetime": 1,
                    "date": 2,
                    "timestamp": 3,
                    "interval_": 4,
                },
            ],
        )
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            assert clique.make_query(
                "select toTimeZone(datetime, 'UTC') as datetime, date, timestamp, interval_ from \"//tmp/t1\""
            ) == [
                {
                    "datetime": "1970-01-01 00:00:01",
                    "date": "1970-01-03",
                    "timestamp": 3,
                    "interval_": 4,
                }
            ]

    @authors("babenko")
    def test_nonuniform_nullability(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "x", "type": "string", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t1", [{"x": "hello"}])
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "x", "type": "string", "required": False},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t2", [{"x": None}, {"x": "world"}])
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            assert clique.make_query('select * from concatYtTables("//tmp/t1", "//tmp/t2") order by x') == [
                {"x": "hello"},
                {"x": "world"},
                {"x": None},
            ]

    @authors("babenko")
    def test_integral_upcast(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "x", "type": "int32", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t1", [{"x": 1}])
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "x", "type": "int64", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        write_table("//tmp/t2", [{"x": 2}])
        create(
            "table",
            "//tmp/t",
            attributes={
                "schema": [
                    {"name": "x", "type": "int64", "required": True},
                ],
                "optimize_for": "scan",
            },
        )
        merge(in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t")
        with Clique(1, config_patch=self.CONFIG_PATCH) as clique:
            assert clique.make_query('select * from "//tmp/t" order by x') == [{"x": 1}, {"x": 2}]


class TestCustomSettings(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_simple(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])
        with Clique(1) as clique:
            for throw_testing_exception_in_distributor in (None, False, True):
                for throw_testing_exception_in_subquery in (None, False, True):
                    settings = {}
                    if throw_testing_exception_in_distributor is not None:
                        settings["chyt_throw_testing_exception_in_distributor"] = int(
                            throw_testing_exception_in_distributor
                        )
                    if throw_testing_exception_in_subquery is not None:
                        settings["chyt_throw_testing_exception_in_subquery"] = int(throw_testing_exception_in_subquery)
                    if throw_testing_exception_in_subquery is not None:
                        assert clique.make_query(
                            "select CAST(getSetting('chyt_throw_testing_exception_in_subquery') as Int64) as v",
                            settings=settings,
                        ) == [{"v": int(throw_testing_exception_in_subquery)}]
                    if throw_testing_exception_in_distributor is not None:
                        assert clique.make_query(
                            "select CAST(getSetting('chyt_throw_testing_exception_in_distributor') as Int64) as v",
                            settings=settings,
                        ) == [{"v": int(throw_testing_exception_in_distributor)}]
                    if not bool(throw_testing_exception_in_distributor) and not bool(
                        throw_testing_exception_in_subquery
                    ):
                        assert clique.make_query("select * from `//tmp/t`", settings=settings) == [{"a": 1}]
                    else:
                        if bool(throw_testing_exception_in_distributor):
                            error_substr = "Testing exception in distributor"
                        else:
                            error_substr = "Testing exception in subquery"
                        with raises_yt_error(error_substr):
                            clique.make_query("select * from `//tmp/t`", settings=settings)

    @authors("max42")
    def test_defaults(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])
        for default_value in (None, False, True):
            default_settings = (
                {"throw_testing_exception_in_distributor": int(default_value)} if default_value is not None else {}
            )
            with Clique(1, config_patch={"yt": {"settings": default_settings}}) as clique:
                for override_value in (None, False, True):
                    value = False
                    if default_value is not None:
                        value = default_value
                    if override_value is not None:
                        value = override_value
                    settings = (
                        {"chyt_throw_testing_exception_in_distributor": int(override_value)}
                        if override_value is not None
                        else {}
                    )
                    if value:
                        with raises_yt_error("Testing exception in distributor"):
                            clique.make_query("select * from `//tmp/t`", settings=settings)
                    else:
                        assert clique.make_query("select * from `//tmp/t`", settings=settings) == [{"a": 1}]

