from helpers import get_scheduling_options

from yt_commands import (create, write_file, ls, start_op, get, exists, update_op_parameters, abort_job, create_user,
                         sync_create_cells, print_debug, read_file)

from yt.clickhouse import get_clique_spec_builder
from yt.clickhouse.test_helpers import get_host_paths, get_clickhouse_server_config

from yt.environment import arcadia_interop

from yt_env_setup import YTEnvSetup, is_asan_build

from yt.wrapper.common import simplify_structure

from yt.common import update, update_inplace, YtError, wait, parts_to_uuid

import yt.packages.requests as requests

import yt.yson as yson

from threading import Thread
import os
import errno
import copy
import time
import json
import random

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
            except OSError as e:
                if e.errno == errno.EEXIST:
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

    def make_request(self, url, query, headers, format="JSON", params=None, verbose=False,
                     only_rows=True, full_response=False):
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
                    ) != "running"
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

    def resize(self, size, jobs_to_abort=None):
        jobs_to_abort = jobs_to_abort or []
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

    @staticmethod
    def _signal_instance(pid, signal):
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
        Clique.base_config["cluster_connection"] = self.Env.configs["driver"]
        Clique.proxy_address = self._get_proxy_address()
