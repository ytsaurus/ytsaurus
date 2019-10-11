from yt_commands import *

from yt_env_setup import wait, YTEnvSetup, is_asan_build, is_gcc_build
from yt.wrapper.clickhouse import get_clickhouse_clique_spec_builder
from yt.wrapper.common import simplify_structure
import yt.packages.requests as requests
from yt.packages.six.moves import map as imap

import yt.yson as yson

from yt.common import update, parts_to_uuid

from distutils.spawn import find_executable

import json
import pytest
import random
import os
import copy
import threading

TEST_DIR = os.path.join(os.path.dirname(__file__))

YTSERVER_CLICKHOUSE_BINARY = os.environ.get("YTSERVER_CLICKHOUSE_PATH")
if YTSERVER_CLICKHOUSE_BINARY is None:
    YTSERVER_CLICKHOUSE_BINARY = find_executable("ytserver-clickhouse")

DEFAULTS = {
    "memory_footprint": 2 * 1000**3,
    "memory_limit": 5 * 1000**3,
    "host_ytserver_clickhouse_path": YTSERVER_CLICKHOUSE_BINARY,
    "cpu_limit": 1,
    "enable_monitoring": False,
    "clickhouse_config": {},
}

QUERY_TYPES_WITH_OUTPUT = ("describe", "select", "show")


class Clique(object):
    base_config = None
    clique_index = 0
    query_index = 0
    path_to_run = None
    core_dump_path = None

    def __init__(self, instance_count, max_failed_job_count=0, config_patch=None, **kwargs):
        config = update(Clique.base_config, config_patch) if config_patch is not None else copy.deepcopy(Clique.base_config)
        spec = {"pool": None}
        self.is_tracing = False
        if "YT_TRACE_DUMP_DIR" in os.environ:
            self.is_tracing = True
            spec["tasks"] = {"instances": {"environment" : {"YT_TRACE_DUMP_DIR": os.environ["YT_TRACE_DUMP_DIR"]}}}
        if "spec" in kwargs:
            spec = update(spec, kwargs.pop("spec"))

        self.log_root = os.path.join(self.path_to_run, "logs", "clickhouse-{}".format(Clique.clique_index))
        for writer_key, writer in config["logging"]["writers"].iteritems():
            if writer["type"] == "file":
                writer["file_name"] = os.path.join(self.log_root, writer["file_name"])
        os.mkdir(self.log_root)
        os.chmod(self.log_root, 0777)

        filename = "//sys/clickhouse/config-{}.yson".format(Clique.clique_index)
        Clique.clique_index += 1
        create("file", filename)
        write_file(filename, yson.dumps(config, yson_format="pretty"))
        spec_builder = get_clickhouse_clique_spec_builder(instance_count,
                                                          cypress_config_path=filename,
                                                          max_failed_job_count=max_failed_job_count,
                                                          defaults=DEFAULTS,
                                                          spec=spec,
                                                          core_dump_destination=Clique.core_dump_path,
                                                          **kwargs)
        self.spec = simplify_structure(spec_builder.build())
        if not is_asan_build():
            self.spec["tasks"]["instances"]["force_core_dump"] = True
        self.instance_count = instance_count

    def get_active_instances(self):
        if exists("//sys/clickhouse/cliques/{0}".format(self.op.id), verbose=False):
            instances = ls("//sys/clickhouse/cliques/{0}".format(self.op.id), attributes=["locks", "host", "http_port"])

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

    def _print_progress(self):
        print_debug(self.op.build_progress(), "(active instance count: {})".format(self.get_active_instance_count()))

    def __enter__(self):
        self.op = start_op("vanilla",
                           spec=self.spec,
                           dont_track=True)

        self.log_root_alternative = os.path.realpath(os.path.join(self.log_root, "..",
                                                                  "clickhouse-{}".format(self.op.id)))
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
            elif state == "running" and self.get_active_instance_count() == self.instance_count:
                break
            elif counter % 30 == 0:
                self._print_progress()
            elif counter >= MAX_COUNTER_VALUE:
                raise YtError("Clique did not start in time, clique directory: {}".format(get("//sys/clickhouse/cliques/{0}".format(self.op.id), verbose=False)))

            time.sleep(self.op._poll_frequency)
            counter += 1

        self._print_progress()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
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
                raise YtError("ClickHouse request failed and resulted in clique failure", inner_errors=[original_error, clique_error])
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
        return "\n".join(result)

    def make_direct_query(self, instance, query, user="root", format="JSON", verbose=True, only_rows=True, full_response=False):
        # Make some improvements to query: strip trailing semicolon, add format if needed.
        query = query.strip()
        assert "format" not in query.lower()
        query_type = query.strip().split(' ', 1)[0]
        if query.endswith(";"):
            query = query[:-1]
        output_present = query_type in QUERY_TYPES_WITH_OUTPUT
        if output_present:
            query = query + " format " + format

        host = instance.attributes["host"]
        port = instance.attributes["http_port"]

        query_id = parts_to_uuid(random.randint(0, 2**64 - 1), (Clique.clique_index << 32) | Clique.query_index)
        Clique.query_index += 1

        print_debug()
        print_debug("Querying {0}:{1} with the following data:\n> {2}".format(host, port, query))
        print_debug("Query id: {}".format(query_id))
        result = requests.post("http://{}:{}/query?output_format_json_quote_64bit_integers=0".format(host, port),
                               data=query,
                               headers={"X-ClickHouse-User": user,
                                        "X-Yt-Trace-Id": query_id,
                                        "X-Yt-Span-Id": "0",
                                        "X-Yt-Sampled": str(int(self.is_tracing))})
        output = ""
        if result.status_code != 200:
            output += "Query failed, HTTP code: {}\n".format(result.status_code)
            error = self._parse_error(result.text).strip()
            if error:
                output += "Error: {}\n".format(error)
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
            raise YtError("ClickHouse query failed\n" + output, attributes={"query": query, "query_id": query_id})
        else:
            if output_present:
                if format == "JSON":
                    result = result.json()
                if only_rows and format == "JSON":
                    result = result["data"]
                return result
            else:
                return None

    def make_query(self, query, user="root", format="JSON", verbose=True, only_rows=True, full_response=False):
        instances = self.get_active_instances()
        assert len(instances) > 0
        instance = random.choice(instances)
        return self.make_direct_query(instance, query, user, format, verbose, only_rows, full_response)

    def resize(self, size, jobs_to_abort=[]):
        update_op_parameters(self.op.id, parameters=get_scheduling_options(user_slots=size))
        for job in jobs_to_abort:
            abort_job(job)
        wait(lambda: self.get_active_instance_count() == size)

class ClickHouseTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1
    NODE_PORT_SET_SIZE = 5
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True

    ENABLE_HTTP_PROXY = True

    DELTA_PROXY_CONFIG = {
        "proxy": {
            "clickhouse": {
                "clique_cache": {
                    "soft_age_threshold": 500,
                    "hard_age_threshold": 500,
                    "master_cache_expire_time": 500,
                },
            },
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "memory_watchdog_period": 100,
                    "supported_cgroups": ["cpuacct", "blkio", "cpu"],
                },
            },
            "job_controller": {
                "resource_limits": {
                    "memory": 10 * 2**30,
                },
            },
        }
    }

    def _read_local_config_file(self, name):
        return open(os.path.join(TEST_DIR, "test_clickhouse", name)).read()

    def _setup(self):
        Clique.path_to_run = self.path_to_run
        Clique.core_dump_path = os.path.join(self.path_to_run, "core_dumps")
        if not os.path.exists(Clique.core_dump_path):
            os.mkdir(Clique.core_dump_path)
            os.chmod(Clique.core_dump_path, 0777)

        if YTSERVER_CLICKHOUSE_BINARY is None:
            pytest.skip("This test requires ytserver-clickhouse binary being built")

        if exists("//sys/clickhouse"):
            return
        create("map_node", "//sys/clickhouse")

        # We need to inject cluster_connection into yson config.
        Clique.base_config = yson.loads(self._read_local_config_file("config.yson"))
        Clique.base_config["cluster_connection"] = self.__class__.Env.configs["driver"]

def get_scheduling_options(user_slots):
    return {
        "scheduling_options_per_pool_tree": {
            "default": {
                "resource_limits": {
                    "user_slots": user_slots
                }
            }
        }
    }

class TestClickHouseCommon(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    @pytest.mark.parametrize("instance_count", [1, 5])
    def test_avg(self, instance_count):
        with Clique(instance_count) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"a": 2 * i}, {"a": 2 * i + 1}])

            assert abs(clique.make_query('select avg(a) from "//tmp/t"')[0]["avg(a)"] - 4.5) < 1e-6
            with pytest.raises(YtError):
                clique.make_query('select avg(b) from "//tmp/t"')

            assert abs(clique.make_query('select avg(a) from "//tmp/t[#2:#9]"')[0]["avg(a)"] - 5.0) < 1e-6

    # YT-9497
    @authors("max42")
    def test_aggregation_with_multiple_string_columns(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "key1", "type": "string"},
                                                              {"name": "key2", "type": "string"},
                                                              {"name": "value", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"key1": "dream", "key2": "theater", "value": i * 5 + j} for j in range(5)])
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
            assert result == [{"CAST(a, 'datetime')": "2012-12-12 20:00:00"}]

    @authors("max42")
    def test_settings(self):
        with Clique(1) as clique:
            # I took some random option from the documentation and changed it in config.yson.
            # Let's see if it changed in internal table with settings.

            result = clique.make_query("select * from system.settings where name = 'max_temporary_non_const_columns'")
            assert result[0]["value"] == "1234"
            assert result[0]["changed"] == 1

    @authors("max42")
    def test_schema_caching(self):
        with Clique(1) as clique:
            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            write_table("//tmp/t", [{"a": 1}])
            old_description = clique.make_query('describe "//tmp/t"')
            assert old_description[0]["name"] == "a"
            remove("//tmp/t")
            create("table", "//tmp/t", attributes={"schema": [{"name": "b", "type": "int64"}]})
            write_table("//tmp/t", [{"b": 1}])
            new_description = clique.make_query('describe "//tmp/t"')
            assert new_description[0]["name"] == "b"

    @authors("max42")
    def test_concat_inside_link(self):
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
            "discovery": {
                # Set big value to prevent unlocking node.
                "transaction_timeout": 1000000,
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
            # One instnace is dead, but the lock should be alive.
            assert len(instances) == 3

            for instance in instances:
                if instance in old_instances:
                    # Avoid sending request to the dead instance.
                    continue
                response = clique.make_direct_query(instance, "select * from system.clique")
                assert len(response) == 2

    @authors("dakovalkov")
    def test_single_interrupt(self):
        patch = {
            "interruption_graceful_timeout": 1000,
        }
        with Clique(1, max_failed_job_count=2, config_patch=patch) as clique:
            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=0))
            instances = clique.get_active_instances()
            assert len(instances) == 1
            signal_job(instances[0], "SIGINT")
            time.sleep(0.5)
            assert len(clique.get_active_instances()) == 0
            assert clique.make_direct_query(instances[0], "select 1", full_response=True).status_code == 301
            time.sleep(0.6)
            with pytest.raises(Exception):
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
            signal_job(instances[0], "SIGINT")
            time.sleep(0.2)
            assert len(clique.get_active_instances()) == 0
            assert clique.make_direct_query(instances[0], "select 1", full_response=True).status_code == 301
            signal_job(instances[0], "SIGINT")
            time.sleep(0.2)
            with pytest.raises(Exception):
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
                time.sleep(0.2)
                signal_job(instances[0], "SIGINT")
            signal_thread = threading.Thread(target=signal_job_later)
            signal_thread.start()

            assert clique.make_direct_query(instances[0], "select sleep(3)") == [{"sleep(3)": 0}]
            time.sleep(0.2)
            with pytest.raises(Exception):
                clique.make_direct_query(instances[0], "select 1")

            clique.resize(1)

            new_instances = clique.get_active_instances()
            assert len(new_instances) == 1
            assert new_instances != instances

    @authors("dakovalkov")
    def test_convert_yson(self):
        create("table", "//tmp/table", attributes={"schema": [{"name": "i", "type": "any"}, {"name": "fmt", "type": "string"}]})
        value1 = 1
        value2 = [1, 2]
        value3 = {"key": "value"}
        write_table("//tmp/table", [{"i": value1, "fmt": "binary"}, {"i": value2, "fmt": "pretty"}, {"i": value3, "fmt": "text"}])
        with Clique(1) as clique:
            value = {"key": [1, 2]}
            func = "ConvertYson('" + yson.dumps(value, yson_format='text') + "', 'pretty')"
            assert clique.make_query("select " + func) == [{func: yson.dumps(value, yson_format='pretty')}]
            func = "ConvertYson(NULL, 'text')"
            assert clique.make_query("select " + func) == [{func: None}]
            func = "ConvertYson(i, 'text')"
            assert clique.make_query("select " + func + " from \"//tmp/table\"") == [
                {func: yson.dumps(value1, yson_format='text')},
                {func: yson.dumps(value2, yson_format='text')},
                {func: yson.dumps(value3, yson_format='text')}]
            func = "ConvertYson(i, fmt)"
            assert clique.make_query("select " + func + " from \"//tmp/table\"") == [
                {func: yson.dumps(value1, yson_format='binary')},
                {func: yson.dumps(value2, yson_format='pretty')},
                {func: yson.dumps(value3, yson_format='text')}]
            with pytest.raises(YtError):
                clique.make_query("select ConvertYson('{key=[1;2]}', 'xxx')")
            with pytest.raises(YtError):
                clique.make_query("select ConvertYson('{{{{', 'binary')")
            with pytest.raises(YtError):
                clique.make_query("select ConvertYson(1, 'text')")

    @authors("dakovalkov")
    def test_reject_request(self):
        with Clique(1) as clique:
            instance = clique.get_active_instances()[0]

            host = instance.attributes["host"]
            port = instance.attributes["http_port"]
            query_id = parts_to_uuid(random.randint(0, 2**64 - 1), random.randint(0, 2**64 - 1))

            result = requests.post("http://{}:{}/query?query_id={}".format(host, port, query_id),
                                data="select 1",
                                headers={"X-ClickHouse-User": "root",
                                        "X-Yt-Request-Id": query_id,
                                        "X-Clique-Id": "wrong-id"})
            print_debug(result.text)
            assert result.status_code == 301

            result = requests.post("http://{}:{}/query?query_id={}".format(host, port, query_id),
                                data="select 1",
                                headers={"X-ClickHouse-User": "root",
                                        "X-Yt-Request-Id": query_id,
                                        "X-Clique-Id": clique.op.id})
            print_debug(result.text)
            assert result.status_code == 200

            signal_job(instance, "SIGINT")

            result = requests.post("http://{}:{}/query?query_id={}".format(host, port, query_id),
                                data="select 1",
                                headers={"X-ClickHouse-User": "root",
                                        "X-Yt-Request-Id": query_id,
                                        "X-Clique-Id": clique.op.id})
            print_debug(result.text)
            assert result.status_code == 301

class TestJobInput(ClickHouseTestBase):
    def setup(self):
        self._setup()

    def _expect_row_count(self, clique, query, exact=None, min=None, max=None, verbose=True):
        result = clique.make_query(query, verbose=verbose, only_rows=False)
        assert (exact is not None) ^ (min is not None and max is not None)
        if exact is not None:
            assert result["statistics"]["rows_read"] == exact
        else:
            assert min <= result["statistics"]["rows_read"] <= max

    @authors("max42")
    def test_chunk_filter(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64", "sort_order": "ascending"}]})
        for i in xrange(10):
            write_table("<append=%true>//tmp/t", [{"i": i}])
        with Clique(1) as clique:
            self._expect_row_count(clique, 'select * from "//tmp/t" where i >= 3', exact=7)
            self._expect_row_count(clique, 'select * from "//tmp/t" where i < 2', exact=2)
            self._expect_row_count(clique, 'select * from "//tmp/t" where 5 <= i and i <= 8', exact=4)
            self._expect_row_count(clique, 'select * from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=2)

    @authors("max42")
    @pytest.mark.xfail(run="False", reason="Chunk slicing is temporarily not supported")
    def test_chunk_slicing(self):
        create("table",
               "//tmp/t",
               attributes={
                   "chunk_writer": {"block_size": 1024},
                   "compression_codec": "none",
                   # TODO(max42): investigate what happens when both columns are sorted.
                   "schema": [{"name": "i", "type": "int64", "sort_order": "ascending"},
                              {"name": "s", "type": "string"}]
               })

        write_table("//tmp/t", [{"i": i, "s": str(i) * (10 * 1024)} for i in range(10)], verbose=False)
        chunk_id = get_singular_chunk_id("//tmp/t")
        assert get("#" + chunk_id + "/@compressed_data_size") > 100 * 1024
        assert get("#" + chunk_id + "/@max_block_size") < 20 * 1024

        with Clique(1) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            self._expect_row_count(clique, 'select i from "//tmp/t" where i >= 3', min=7, max=8)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i < 2', min=3, max=4)
            self._expect_row_count(clique, 'select i from "//tmp/t" where 5 <= i and i <= 8', min=4, max=6)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', min=2, max=4)

        # Forcefully disable chunk slicing.
        with Clique(1, config_patch={"engine": {"subquery": {"max_sliced_chunk_count": 0}}}) as clique:
            # Due to inclusiveness issues each of the row counts should be correct with some error.
            self._expect_row_count(clique, 'select i from "//tmp/t" where i >= 3', exact=10)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i < 2', exact=10)
            self._expect_row_count(clique, 'select i from "//tmp/t" where 5 <= i and i <= 8', exact=10)
            self._expect_row_count(clique, 'select i from "//tmp/t" where i in (-1, 2, 8, 8, 15)', exact=10)

    @authors("max42")
    def test_sampling(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": i} for i in range(1000)], verbose=False)
        with Clique(1) as clique:
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0.1', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 100', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 2/20', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0.1 offset 42', min=85, max=115, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 10000', exact=1000, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 10000', exact=1000, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0', exact=0, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 0.000000000001', exact=0, verbose=False)
            self._expect_row_count(clique, 'select a from "//tmp/t" sample 1/100000000000', exact=0, verbose=False)

    @authors("max42")
    def test_CHYT_143(self):
        # Issues with chunk name table ids, read schema ids and unversioned value row indices.
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "b", "type": "string"}, {"name": "a", "type": "int64"}]})
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
            result = clique.make_query("select * from concatYtTables('//tmp/t') union all select * from concatYtTables('//tmp/t')")
            assert result == [{"a": 42}, {"a": 42}]

class TestMutations(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_insert_values(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                          {"name": "ui64", "type": "uint64"},
                                                          {"name": "str", "type": "string"},
                                                          {"name": "dbl", "type": "double"},
                                                          {"name": "bool", "type": "boolean"}]})
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t"(i64) values (1), (-2)')
            clique.make_query('insert into "//tmp/t"(ui64) values (7), (8)')
            with pytest.raises(YtError):
                clique.make_query('insert into "//tmp/t"(str) values (2)')
            clique.make_query('insert into "//tmp/t"(i64, ui64, str, dbl, bool) values (-1, 1, \'abc\', 3.14, 1)')
            clique.make_query('insert into "//tmp/t"(i64, ui64, str, dbl, bool) values (NULL, NULL, NULL, NULL, NULL)')
            with pytest.raises(YtError):
                clique.make_query('insert into "//tmp/t"(bool) values (3)')
            # This insert leads to NULL insertion.
            clique.make_query('insert into "//tmp/t"(bool) values (2.4)')
            assert read_table("//tmp/t") == [
                {"i64": 1, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": -2, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 7, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": 8, "str": None, "dbl": None, "bool": None},
                {"i64": -1, "ui64": 1, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": None, "ui64": None, "str": None, "dbl": None, "bool": None},
                {"i64": None, "ui64": None, "str": None, "dbl": None, "bool": None},
            ]
            assert get("//tmp/t/@chunk_count") == 5
            clique.make_query('insert into "<append=%false>//tmp/t" values (-2, 2, \'xyz\', 2.71, 0)')
            assert read_table("//tmp/t") == [
                {"i64": -2, "ui64": 2, "str": "xyz", "dbl": 2.71, "bool": False},
            ]
            assert get("//tmp/t/@chunk_count") == 1

    @authors("max42")
    def test_insert_select(self):
        create("table", "//tmp/s1", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                           {"name": "ui64", "type": "uint64"},
                                                           {"name": "str", "type": "string"},
                                                           {"name": "dbl", "type": "double"},
                                                           {"name": "bool", "type": "boolean"}]})
        write_table("//tmp/s1", [
            {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
            {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
        ])

        # Table with different order of columns.
        create("table", "//tmp/s2", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                           {"name": "str", "type": "string"},
                                                           {"name": "dbl", "type": "double"},
                                                           {"name": "ui64", "type": "uint64"},
                                                           {"name": "bool", "type": "boolean"}]})
        write_table("//tmp/s2", [
            {"i64": 4, "ui64": 9, "str": "def", "dbl": 12.3, "bool": False},
            {"i64": -5, "ui64": 5, "str": "ijk", "dbl": -3.1, "bool": True},
        ])

        create("table", "//tmp/t", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                          {"name": "ui64", "type": "uint64"},
                                                          {"name": "str", "type": "string"},
                                                          {"name": "dbl", "type": "double"},
                                                          {"name": "bool", "type": "boolean"}]})
        with Clique(1) as clique:
            clique.make_query('insert into "//tmp/t" select * from "//tmp/s1"')
            assert read_table("//tmp/t") == [
                {"i64": 2, "ui64": 3, "str": "abc", "dbl": 3.14, "bool": True},
                {"i64": -1, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
            ]

            # Number of columns does not match.
            with pytest.raises(YtError):
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
        with Clique(1, config_patch={"engine": {"create_table_default_attributes": {"foo": 42}}}) as clique:
            clique.make_query('create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, i32 Int32) '
                              'engine YtTable() order by (str, i64)')
            assert normalize_schema(get("//tmp/t/@schema")) == make_schema([
                {"name": "str", "type": "string", "sort_order": "ascending", "required": False},
                {"name": "i64", "type": "int64", "sort_order": "ascending", "required": False},
                {"name": "ui64", "type": "uint64", "required": False},
                {"name": "dbl", "type": "double", "required": False},
                {"name": "i32", "type": "int64", "required": False},
            ], strict=True, unique_keys=False)

            # Table already exists.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t"(i64 Int64, ui64 UInt64, str String, dbl Float64, i32 Int32) '
                                  'engine YtTable() order by (str, i64)')

            # No non-trivial expressions.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2"(i64 Int64) engine YtTable() order by (i64 * i64)')

            # No fancy types.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2"(d DateTime) engine YtTable()')

            # Missing key column.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2"(i Int64) engine YtTable() order by j')

            clique.make_query('create table "//tmp/t_snappy"(i Int64) engine YtTable(\'{compression_codec=snappy}\')')
            assert get("//tmp/t_snappy/@compression_codec") == "snappy"

            # Default optimize_for should be scan.
            assert get("//tmp/t_snappy/@optimize_for") == "scan"

            assert get("//tmp/t_snappy/@foo") == 42

            # Empty schema.
            with pytest.raises(YtError):
                clique.make_query('create table "//tmp/t2" engine YtTable()')

            # Underscore indicates that the columns should be ignored and schema from attributes should
            # be taken.
            clique.make_query('create table "//tmp/t2"(_ UInt8) engine YtTable(\'{schema=[{name=a;type=int64}]}\')')
            assert get("//tmp/t2/@schema/0/name") == "a"

            # Column list has higher priority.
            clique.make_query('create table "//tmp/t3"(b String) engine YtTable(\'{schema=[{name=a;type=int64}]}\')')
            assert get("//tmp/t3/@schema/0/name") == "b"

    @authors("max42")
    def test_create_table_as_select(self):
        create("table", "//tmp/s1", attributes={"schema": [{"name": "i64", "type": "int64"},
                                                           {"name": "ui64", "type": "uint64"},
                                                           {"name": "str", "type": "string"},
                                                           {"name": "dbl", "type": "double"},
                                                           {"name": "bool", "type": "boolean"}]})
        write_table("//tmp/s1", [
            {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": True},
            {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": False},
        ])

        with Clique(1) as clique:
            clique.make_query('create table "//tmp/t1" engine YtTable() order by i64 as select * from "//tmp/s1"')

            assert read_table("//tmp/t1") == [
                {"i64": -1, "ui64": 3, "str": "def", "dbl": 3.14, "bool": 1},
                {"i64": 2, "ui64": 7, "str": "xyz", "dbl": 2.78, "bool": 0},
            ]

    @authors("max42")
    def test_create_table_as_table(self):
        schema = [{"name": "i64", "type": "int64", "required": False, "sort_order": "ascending"},
                  {"name": "ui64", "type": "uint64", "required": False},
                  {"name": "str", "type": "string", "required": False},
                  {"name": "dbl", "type": "double", "required": False},
                  {"name": "bool", "type": "boolean", "required": False}]
        schema_copied = copy.deepcopy(schema)
        schema_copied[4]["type"] = "uint64"
        create("table", "//tmp/s1", attributes={"schema": schema,
                                                "compression_codec": "snappy"})

        with Clique(1) as clique:
            clique.make_query('show create table "//tmp/s1"')
            clique.make_query('create table "//tmp/s2" as "//tmp/s1" engine YtTable() order by i64')
            assert normalize_schema(get("//tmp/s2/@schema")) == make_schema(schema_copied, strict=True, unique_keys=False)

            # This is wrong.
            # assert get("//tmp/s2/@compression_codec") == "snappy"


class TestCompositeTypes(ClickHouseTestBase):
    def setup(self):
        self._setup()

        create("table", "//tmp/t", attributes={"schema": [{"name": "i", "type": "int64"}, {"name": "v", "type": "any"}]})
        write_table("//tmp/t", [
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
            },
            {
                "i": 1,
                "v": {
                    "i64": "xyz",  # Wrong type.
                },
            },
            {
                "i": 2,
                "v": {
                    "i64": yson.YsonUint64(2**63 + 42),  # Out of range for getting value as i64.
                },
            },
            {
                "i": 3,
                "v": {},  # Key i64 is missing.
            },
            {
                "i": 4,
                "v": {
                    "i64": 57,
                },
            }
        ])

    @authors("max42")
    def test_read_int64_strict(self):
        with Clique(1) as clique:
            for i in xrange(4):
                query = "select YPathInt64Strict(v, '/i64') from \"//tmp/t\" where i = {}".format(i)
                if i != 0:
                    with pytest.raises(YtError):
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

    @authors("max42")
    def test_read_int64_non_strict(self):
        with Clique(1) as clique:
            query = "select YPathInt64(v, '/i64') from \"//tmp/t\""
            result = clique.make_query(query)
            for i, item in enumerate(result):
                if i == 0:
                    assert item.popitem()[1] == -42
                elif i == 4:
                    assert item.popitem()[1] == 57
                else:
                    assert item.popitem()[1] == 0

    @authors("max42")
    def test_read_all_types_strict(self):
        query = "select YPathInt64Strict(v, '/i64') as i64, YPathUInt64Strict(v, '/ui64') as ui64, " \
                "YPathDoubleStrict(v, '/dbl') as dbl, YPathBooleanStrict(v, '/bool') as bool, " \
                "YPathStringStrict(v, '/str') as str, YPathArrayInt64Strict(v, '/arr_i64') as arr_i64, " \
                "YPathArrayUInt64Strict(v, '/arr_ui64') as arr_ui64, YPathArrayDoubleStrict(v, '/arr_dbl') as arr_dbl, " \
                "YPathArrayBooleanStrict(v, '/arr_bool') as arr_bool from \"//tmp/t\" where i = 0"
        with Clique(1) as clique:
            result = clique.make_query(query)
        assert result == [{
            "i64": -42,
            "ui64": 23,
            "bool": True,
            "dbl": 3.14,
            "str": "xyz",
            "arr_i64": [-1, 0, 1],
            "arr_ui64": [1, 1, 2, 3, 5],
            "arr_dbl": [-1.1, 2.71],
            "arr_bool": [False, True, False],
        }]

    @authors("max42")
    def test_read_all_types_non_strict(self):
        query = "select YPathInt64(v, '/i64') as i64, YPathUInt64(v, '/ui64') as ui64, " \
                "YPathDouble(v, '/dbl') as dbl, YPathBoolean(v, '/bool') as bool, " \
                "YPathString(v, '/str') as str, YPathArrayInt64(v, '/arr_i64') as arr_i64, " \
                "YPathArrayUInt64(v, '/arr_ui64') as arr_ui64, YPathArrayDouble(v, '/arr_dbl') as arr_dbl, " \
                "YPathArrayBoolean(v, '/arr_bool') as arr_bool from \"//tmp/t\" where i = 3"
        with Clique(1) as clique:
            result = clique.make_query(query)
        assert result == [{
            "i64": 0,
            "ui64": 0,
            "bool": False,
            "dbl": 0.0,
            "str": "",
            "arr_i64": [],
            "arr_ui64": [],
            "arr_dbl": [],
            "arr_bool": [],
        }]

    @authors("max42")
    def test_const_args(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathString('{a=[1;2;{b=xyz}]}', '/a/2/b') as str")
        assert result == [{"str": "xyz"}]

    @authors("max42")
    def test_nulls(self):
        with Clique(1) as clique:
            result = clique.make_query("select YPathString(NULL, NULL) as a, YPathString(NULL, '/x') as b, "
                                       "YPathString('{a=1}', NULL) as c")
        assert result == [{"a": None, "b": None, "c": None}]

    # CHYT-157.
    @authors("max42")
    def test_int64_as_any(self):
        create("table", "//tmp/s1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        create("table", "//tmp/s2", attributes={"schema": [{"name": "a", "type": "any"}]})
        lst = [{"a": -2**63},
               {"a": -42},
               {"a": 123456789123456789},
               {"a": 2**63 - 1}]
        write_table("//tmp/s1", lst)
        merge(in_="//tmp/s1",
              out="//tmp/s2")

        with Clique(1) as clique:
            result = clique.make_query("select YPathInt64(a, '') as i from \"//tmp/s2\" order by i")
            assert result == [{"i": row["a"]} for row in lst]

class TestYtDictionaries(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_int_key_flat(self):
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64", "required": True},
                                                             {"name": "value_str", "type": "string", "required": True},
                                                             {"name": "value_i64", "type": "int64", "required": True}]})
        write_table("//tmp/dict", [
            {"key": i, "value_str": "str" + str(i), "value_i64": i * i} for i in [1, 3, 5]
        ])

        with Clique(1, config_patch={
            "engine": {"dictionaries": [
                {
                    "name": "dict",
                    "layout": {"flat": {}},
                    "structure": {
                        "id": {"name": "key"},
                        "attribute": [{"name": "value_str", "type": "String", "null_value": "n/a"},
                                      {"name": "value_i64", "type": "Int64", "null_value": 42}]
                    },
                    "lifetime": 0,
                    "source": {
                        "yt": {
                            "path": "//tmp/dict"
                        }
                    }
                }
            ]}}) as clique:
            result = clique.make_query("select number, dictGetString('dict', 'value_str', number) as str, "
                                       "dictGetInt64('dict', 'value_i64', number) as i64 from numbers(5)")
        assert result == [
            {"number": 0, "str": "n/a", "i64": 42},
            {"number": 1, "str": "str1", "i64": 1},
            {"number": 2, "str": "n/a", "i64": 42},
            {"number": 3, "str": "str3", "i64": 9},
            {"number": 4, "str": "n/a", "i64": 42},
        ]

    @authors("max42")
    def test_composite_key_hashed(self):
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "string", "required": True},
                                                             {"name": "subkey", "type": "int64", "required": True},
                                                             {"name": "value", "type": "string", "required": True}]})
        write_table("//tmp/dict", [
            {"key": "a", "subkey": 1, "value": "a1"},
            {"key": "a", "subkey": 2, "value": "a2"},
            {"key": "b", "subkey": 1, "value": "b1"},
        ])

        create("table", "//tmp/queries", attributes={"schema": [{"name": "key", "type": "string", "required": True},
                                                                {"name": "subkey", "type": "int64", "required": True}]})
        write_table("//tmp/queries", [{"key": "a", "subkey": 1},
                                      {"key": "a", "subkey": 2},
                                      {"key": "b", "subkey": 1},
                                      {"key": "b", "subkey": 2}])

        with Clique(1, config_patch={
            "engine": {"dictionaries": [
                {
                    "name": "dict",
                    "layout": {"complex_key_hashed": {}},
                    "structure": {
                        "key": {"attribute": [{"name": "key", "type": "String"},
                                              {"name": "subkey", "type": "Int64"}]},
                        "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}]
                    },
                    "lifetime": 0,
                    "source": {
                        "yt": {
                            "path": "//tmp/dict"
                        }
                    }
                }
            ]}}) as clique:
            result = clique.make_query("select dictGetString('dict', 'value', tuple(key, subkey)) as value from \"//tmp/queries\"")
        assert result == [{"value": "a1"}, {"value": "a2"}, {"value": "b1"}, {"value": "n/a"}]

    @authors("max42")
    def test_lifetime(self):
        create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64", "required": True},
                                                             {"name": "value", "type": "string", "required": True}]})
        write_table("//tmp/dict", [{"key": 42, "value": "x"}])

        with Clique(1, config_patch={
            "engine": {"dictionaries": [
                {
                    "name": "dict",
                    "layout": {"flat": {}},
                    "structure": {
                        "id": {"name": "key"},
                        "attribute": [{"name": "value", "type": "String", "null_value": "n/a"}]
                    },
                    "lifetime": 1,
                    "source": {
                        "yt": {
                            "path": "//tmp/dict"
                        }
                    }
                }
            ]}}) as clique:
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"] == "x"

            write_table("//tmp/dict", [{"key": 42, "value": "y"}])
            # TODO(max42): make update time customizable in CH and reduce this constant.
            time.sleep(7)
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"] == "y"

            remove("//tmp/dict")
            time.sleep(7)
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"] == "y"

            create("table", "//tmp/dict", attributes={"schema": [{"name": "key", "type": "uint64"},
                                                                 {"name": "value", "type": "string"}]})
            write_table("//tmp/dict", [{"key": 42, "value": "z"}])
            time.sleep(7)
            assert clique.make_query("select dictGetString('dict', 'value', CAST(42 as UInt64)) as value")[0]["value"] == "z"


class TestClickHouseSchema(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_missing_schema(self):
        create("table", "//tmp/t")
        write_table("//tmp/t", [{"key": 42, "value": "x"}])

        with Clique(1) as clique:
            with pytest.raises(YtError):
                clique.make_query("select * from \"//tmp/t\"")

    def _strip_description(self, rows):
        return [{key: value for key, value in row.iteritems() if key in ("name", "type")} for row in rows]

    @authors("max42")
    def test_common_schema_unsorted(self):
        create("table", "//tmp/t1", attributes={"schema": [
            {"name": "a", "type": "int64"},
            {"name": "b", "type": "string"},
            {"name": "c", "type": "double"},
        ]})
        create("table", "//tmp/t2", attributes={"schema": [
            {"name": "a", "type": "int64"},
            {"name": "d", "type": "double"},
        ]})
        create("table", "//tmp/t3", attributes={"schema": [
            {"name": "a", "type": "string"}
        ]})

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})
        write_table("//tmp/t3", {"a": "y"})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\")")) == \
                [{"name": "a", "type": "Nullable(Int64)"}]
            assert clique.make_query("select * from concatYtTables(\"//tmp/t1\", \"//tmp/t2\") order by a") == \
                [{"a": 17}, {"a": 42}]

            with pytest.raises(YtError):
                clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\", \"//tmp/t3\")")

    @authors("max42")
    def test_common_schema_sorted(self):
        create("table", "//tmp/t1", attributes={"schema": [
            {"name": "a", "type": "int64", "sort_order": "ascending"},
            {"name": "b", "type": "string", "sort_order": "ascending"},
            {"name": "c", "type": "double"},
        ]})
        create("table", "//tmp/t2", attributes={"schema": [
            {"name": "a", "type": "int64", "sort_order": "ascending"},
            {"name": "d", "type": "double"},
        ]})
        create("table", "//tmp/t3", attributes={"schema": [
            {"name": "a", "type": "int64"},
            {"name": "d", "type": "double"},
        ]})

        write_table("//tmp/t1", {"a": 42, "b": "x", "c": 3.14})
        write_table("//tmp/t2", {"a": 17, "d": 2.71})

        with Clique(1) as clique:
            assert self._strip_description(clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t2\")")) == \
                [{"name": "a", "type": "Nullable(Int64)"}]
            assert self._strip_description(clique.make_query("describe concatYtTables(\"//tmp/t2\", \"//tmp/t1\")")) == \
                [{"name": "a", "type": "Nullable(Int64)"}]
            with pytest.raises(YtError):
                clique.make_query("describe concatYtTables(\"//tmp/t1\", \"//tmp/t3\")")

    @authors("max42")
    def test_nulls_in_primary_key(self):
        create("table", "//tmp/t", attributes={"schema": [
            {"name": "a", "type": "int64", "sort_order": "ascending"}
        ]})

        content = [{"a": None}, {"a": -1}, {"a": 42}]
        write_table("//tmp/t", content)

        with Clique(1) as clique:
            for source in ["\"//tmp/t\"", "concatYtTables('//tmp/t')"]:
                assert clique.make_query("select * from {}".format(source)) == content
                assert clique.make_query("select * from {} where isNull(a)".format(source)) == [{"a": None}]
                assert clique.make_query("select * from {} where isNotNull(a)".format(source)) == [{"a": -1}, {"a": 42}]

class TestClickHouseAccess(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_clique_access(self):
        create_user("u")
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        write_table("//tmp/t", [{"a": 1}])

        with Clique(1, config_patch={"validate_operation_access": False}) as clique:
            assert len(clique.make_query("select * from \"//tmp/t\"", user="u")) == 1

        with Clique(1, config_patch={"validate_operation_access": True}) as clique:
            with pytest.raises(YtError):
                clique.make_query("select * from \"//tmp/t\"", user="u")

        with Clique(1,
                    config_patch={"validate_operation_access": True, "operation_acl_update_period": 100},
                    spec={
                        "acl": [{
                            "subjects": ["u"],
                            "action": "allow",
                            "permissions": ["read"]
                        }]
                    }) as clique:
            assert len(clique.make_query("select * from \"//tmp/t\"", user="u")) == 1
            update_op_parameters(clique.op.id, parameters={"acl": []})
            time.sleep(1)
            with pytest.raises(YtError):
                clique.make_query("select * from \"//tmp/t\"", user="u")


class TestQueryLog(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_query_log(self):
        with Clique(1, config_patch={"engine": {"settings": {"log_queries": 1}}}) as clique:
            clique.make_query("select 1")
            wait(lambda: len(clique.make_query("select * from system.tables where database = 'system' and "
                                               "name = 'query_log';")) >= 1)
            wait(lambda: len(clique.make_query("select * from system.query_log")) >= 1)


class TestQueryRegistry(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    def test_codicils(self):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "string"}]})
        # Normal footprint at start of clique should be around 1.5 Gb
        s = 'x' * (4 * 1024**2)  # 4 Mb.
        write_table("//tmp/t", [{"a": s}])
        merge(in_=["//tmp/t"] * 200,
              out="//tmp/t",
              spec={"force_transform": True})  # 800 Mb.
        with pytest.raises(YtError):
            with Clique(1, config_patch={"memory_watchdog": {"memory_limit": 2 * 1024**3, "period": 50}}) as clique:
                clique.make_query("select a from \"//tmp/t\" order by a", verbose=False)
        assert "OOM" in str(clique.op.get_error())


class TestJoinAndIn(ClickHouseTestBase):
    def setup(self):
        self._setup()

    @authors("max42")
    @pytest.mark.skipif(is_gcc_build(), reason="https://github.com/yandex/ClickHouse/issues/6187")
    def test_global_join(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "b", "type": "string"}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "c", "type": "int64"}, {"name": "d", "type": "string"}]})
        create("table", "//tmp/t3", attributes={"schema": [{"name": "a", "type": "int64"}, {"name": "e", "type": "double"}]})
        write_table("//tmp/t1", [{"a": 42, "b": "qwe"}, {"a": 27, "b": "xyz"}])
        write_table("//tmp/t2", [{"c": 42, "d": "asd"}, {"c": -1, "d": "xyz"}])
        write_table("//tmp/t3", [{"a": 42, "e": 3.14}, {"a": 27, "e": 2.718}])
        with Clique(1) as clique:
            expected = [{"a": 42, "b": "qwe", "c": 42, "d": "asd"}]
            assert clique.make_query("select * from \"//tmp/t1\" global join \"//tmp/t2\" on a = c") == expected
            # TODO(max42): uncomment next line when https://github.com/yandex/ClickHouse/issues/5976 is fixed.
            # assert clique.make_query("select * from \"//tmp/t1\" global join \"//tmp/t2\" on c = a") == expected
            assert clique.make_query("select * from \"//tmp/t1\" t1 global join \"//tmp/t2\" t2 on t1.a = t2.c") == expected
            assert clique.make_query("select * from \"//tmp/t1\" t1 global join \"//tmp/t2\" t2 on t2.c = t1.a") == expected

            expected_on = [{"a": 27, "b": "xyz", "t3.a": 27, "e": 2.718},
                           {"a": 42, "b": "qwe", "t3.a": 42, "e": 3.14}]
            expected_using = [{"a": 27, "b": "xyz", "e": 2.718},
                              {"a": 42, "b": "qwe", "e": 3.14}]
            assert clique.make_query("select * from \"//tmp/t1\" t1 global join \"//tmp/t3\" t3 using a order by a") == expected_using
            assert clique.make_query("select * from \"//tmp/t1\" t1 global join \"//tmp/t3\" t3 on t1.a = t3.a order by a") == expected_on
            assert clique.make_query("select * from \"//tmp/t1\" t1 global join \"//tmp/t3\" t3 on t3.a = t1.a order by a") == expected_on
            assert clique.make_query("select * from \"//tmp/t1\" t1 global join \"//tmp/t3\" t3 on t1.a = t3.a order by t1.a") == expected_on
            assert clique.make_query("select * from \"//tmp/t1\" t1 global join \"//tmp/t3\" t3 on t3.a = t1.a order by t3.a") == expected_on

    @authors("max42")
    def test_global_in(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64", "required": True}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "a", "type": "int64", "required": True}]})
        write_table("//tmp/t1", [{"a": 1}, {"a": 3}, {"a": -42}])
        write_table("//tmp/t2", [{"a": 5}, {"a": 42}, {"a": 3}, {"a": 1}])
        with Clique(1) as clique:
            expected = [{"a": 1}, {"a": 3}]
            assert clique.make_query("select a from \"//tmp/t1\" where a global in (select * from \"//tmp/t2\") order by a") == expected
            assert clique.make_query("select a from \"//tmp/t2\" where a global in (select * from \"//tmp/t1\") order by a") == expected

            assert clique.make_query("select toInt64(42) global in (select * from \"//tmp/t2\")")[0].values() == [1]
            assert clique.make_query("select toInt64(43) global in (select * from \"//tmp/t2\")")[0].values() == [0]

            assert clique.make_query("select toInt64(42) global in (select * from \"//tmp/t2\") from \"//tmp/t1\" limit 1")[0].values() == [1]
            assert clique.make_query("select toInt64(43) global in (select * from \"//tmp/t2\") from \"//tmp/t1\" limit 1")[0].values() == [0]

    @authors("max42")
    @pytest.mark.skipif(is_gcc_build(), reason="https://github.com/yandex/ClickHouse/issues/6187")
    def test_sorted_join_simple(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                                                           {"name": "lhs", "type": "string", "required": True}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                                                           {"name": "rhs", "type": "string", "required": True}]})
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
            expected = [{"key": 1, "lhs": "foo1", "rhs": "bar1"},
                        {"key": 2, "lhs": "foo2", "rhs": "bar2"},
                        {"key": 3, "lhs": "foo3", "rhs": "bar3"},
                        {"key": 4, "lhs": "foo4", "rhs": "bar4"}]
            assert clique.make_query("select key, lhs, rhs from \"//tmp/t1\" join \"//tmp/t2\" using key order by key") == expected
            assert clique.make_query("select key, lhs, rhs from \"//tmp/t1\" t1 join \"//tmp/t2\" t2 on t1.key = t2.key order by key") == expected

    @authors("max42")
    @pytest.mark.skipif(is_gcc_build(), reason="https://github.com/yandex/ClickHouse/issues/6187")
    def test_right_or_full_join_simple(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                                                           {"name": "lhs", "type": "string", "required": True}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                                                           {"name": "rhs", "type": "string", "required": True}]})
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
            expected_right = [{"key": 0, "lhs": "foo0", "rhs": "bar0"},
                              {"key": 0, "lhs": "foo0", "rhs": "bar0"},
                              {"key": 2, "lhs": None, "rhs": "bar2"},
                              {"key": 4, "lhs": None, "rhs": "bar4"},
                              {"key": 9, "lhs": None, "rhs": "bar9"}]
            expected_full = [{"key": 0, "lhs": "foo0", "rhs": "bar0"},
                             {"key": 0, "lhs": "foo0", "rhs": "bar0"},
                             {"key": 1, "lhs": "foo1", "rhs": None},
                             {"key": 2, "lhs": None, "rhs": "bar2"},
                             {"key": 3, "lhs": "foo3", "rhs": None},
                             {"key": 4, "lhs": None, "rhs": "bar4"},
                             {"key": 7, "lhs": "foo7", "rhs": None},
                             {"key": 8, "lhs": "foo8", "rhs": None},
                             {"key": 9, "lhs": None, "rhs": "bar9"}]
            assert clique.make_query("select key, lhs, rhs from \"//tmp/t1\" global right join \"//tmp/t2\" using key order by key") == expected_right
            assert clique.make_query("select key, lhs, rhs from \"//tmp/t1\" global full join \"//tmp/t2\" using key order by key") == expected_full

    @authors("max42")
    @pytest.mark.skipif(is_gcc_build(), reason="https://github.com/yandex/ClickHouse/issues/6187")
    def test_sorted_join_stress(self):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                                                           {"name": "lhs", "type": "string", "required": True}]})
        create("table", "//tmp/t2", attributes={"schema": [{"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                                                           {"name": "rhs", "type": "string", "required": True}]})
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
                rows.append([{"key": key, payload_column_name: "%s%03d" % (payload_value, key)} for key in keys[delimiters[i]:delimiters[i + 1]]])
            return rows

        def write_multiple_chunks(path, row_batches):
            for row_batch in row_batches:
                write_table("<append=%true>" + path, row_batch)
            print_debug("Table {}".format(path))
            chunk_ids = get(path + "/@chunk_ids", verbose=False)
            for chunk_id in chunk_ids:
                attrs = get("#" + chunk_id + "/@", attributes=["min_key", "max_key", "row_count"], verbose=False)
                print_debug("{}: rc = {}, bk = ({}, {})".format(chunk_id, attrs["row_count"], attrs["min_key"], attrs["max_key"]))


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
        for instance_count in range(1, 6):
            with Clique(instance_count) as clique:
                for lhs_arg in ("\"//tmp/t1\"", "(select * from \"//tmp/t1\")"):
                    for rhs_arg in ("\"//tmp/t2\"", "(select * from \"//tmp/t2\")"):
                        for globalness in ("", "global"):
                            for kind in ("inner", "left", "right", "full"):
                                query = \
                                    "select key, lhs, rhs from {lhs_arg} {globalness} {kind} join {rhs_arg} " \
                                    "using key order by key, lhs, rhs nulls first".format(**locals())
                                result = list(imap(sanitize_dict, clique.make_query(query, verbose=False)))

                                expected = expected_results[kind]
                                print_debug("Query #{}: '{}' produced {} rows, expected {} rows".format(index, query, len(result), len(expected)))
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

class TestHttpProxy(ClickHouseTestBase):
    def setup(self):
        self._setup()
        create_user("yt-clickhouse")

    def _get_proxy_address(self):
        return "http://" + self.Env.get_proxy_address()

    @authors("dakovalkov")
    def test_http_proxy(self):
        with Clique(1) as clique:
            url = self._get_proxy_address() + "/query?database={}".format(clique.op.id)
            proxy_response = requests.post(url, data="select * from system.clique format JSON")
            print_debug(proxy_response)
            print_debug(proxy_response.text)
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response.json()["data"] == response

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 1

            clique.resize(0, [jobs[0]])
            clique.resize(1)

            proxy_response = requests.post(url, data="select * from system.clique format JSON")
            response = clique.make_query("select * from system.clique")
            assert len(response) == 1
            assert proxy_response.json()["data"] == response

    @authors("dakovalkov")
    def test_ban_dead_instance_in_proxy(self):
        patch = {
            "discovery": {
                # Set big value to prevent unlocking node.
                "transaction_timeout": 1000000,
            }
        }
        with Clique(2, config_patch=patch) as clique:
            url = self._get_proxy_address() + "/query?database={}".format(clique.op.id)

            wait(lambda: clique.get_active_instance_count() == 2)

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            abort_job(jobs[0])
            wait(lambda: len(clique.op.get_running_jobs()) == 1)

            for instance in clique.get_active_instances():
                if str(instance) == jobs[0]:
                    with pytest.raises(Exception):
                        clique.make_direct_query(instance, "select 1")
                else:
                    assert clique.make_direct_query(instance, "select 1") == [{"1": 1}]

            proxy_responses = []
            for i in range(5):
                proxy_responses += [requests.post(url, data="select 1 format JSON")]
                assert proxy_responses[i].status_code == 200
                assert proxy_responses[i].json()["data"] == proxy_responses[i - 1].json()["data"]

            assert proxy_responses[0].json()["data"] == [{"1": 1}]
            assert clique.get_active_instance_count() == 2

    @authors("dakovalkov")
    def test_ban_stopped_instance_in_proxy(self):
        patch = {
            "interruption_graceful_timeout": 100000,
        }
        with Clique(2, config_patch=patch) as clique:
            url = self._get_proxy_address() + "/query?database={}".format(clique.op.id)

            # Add clique into the cache.
            proxy_responses = []
            proxy_responses += [requests.post(url, data="select 1 format JSON")]

            jobs = list(clique.op.get_running_jobs())
            assert len(jobs) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            signal_job(jobs[0], "SIGINT")

            for instance in clique.get_active_instances():
                if str(instance) == jobs[0]:
                    with pytest.raises(Exception):
                        clique.make_direct_query(instance, "select 1")
                else:
                    assert clique.make_direct_query(instance, "select 1") == [{"1": 1}]

            for i in range(5):
                proxy_responses += [requests.post(url, data="select 1 format JSON")]
                assert proxy_responses[i + 1].status_code == 200
                assert proxy_responses[i].json()["data"] == proxy_responses[i + 1].json()["data"]

            assert proxy_responses[0].json()["data"] == [{"1": 1}]
            assert clique.get_active_instance_count() == 1

    @authors("dakovalkov")
    def test_clique_availability(self):
        create("table", "//tmp/table", attributes={"schema": [{"name": "i", "type": "int64"}]})
        write_table("//tmp/table", [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}])
        patch = {
            "interruption_graceful_timeout": 600,
        }
        with Clique(2, max_failed_job_count=2, config_patch=patch) as clique:
            url = self._get_proxy_address() + "/query?output_format_json_quote_64bit_integers=0&database={}".format(clique.op.id)
            running = True
            def pinger():
                while running:
                    full_response = requests.post(url, data="select * from \"//tmp/table\" format JSON")
                    print_debug(full_response)
                    print_debug(full_response.json())
                    assert full_response.status_code == 200
                    response = sorted(full_response.json()["data"])
                    assert response == [{"i": 0}, {"i": 1}, {"i": 2}, {"i": 3}]

            ping_thread = threading.Thread(target=pinger)
            ping_thread.start()
            time.sleep(1)
            
            instances = clique.get_active_instances()
            assert len(instances) == 2

            update_op_parameters(clique.op.id, parameters=get_scheduling_options(user_slots=1))
            
            signal_job(instances[0], "SIGINT")

            wait(lambda: clique.get_active_instance_count() == 1, iter=10)
            clique.resize(2)

            new_instances = clique.get_active_instances()
            assert len(new_instances) == 2
            assert new_instances != instances

            assert ping_thread.is_alive()
            running = False
            ping_thread.join()

    @authors("max42")
    def test_tracing_via_http_proxy(self):
        with Clique(5) as clique:
            url = self._get_proxy_address() + "/query?database={}".format(clique.op.id)

            create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
            for i in range(5):
                write_table("<append=%true>//tmp/t", [{"a": 2 * i}, {"a": 2 * i + 1}])

            assert abs(requests.post(url,
                                     data="select avg(a) from \"//tmp/t\"",
                                     headers={"X-Yt-Sampled": "1"}).json() - 4.5) < 1e-6

