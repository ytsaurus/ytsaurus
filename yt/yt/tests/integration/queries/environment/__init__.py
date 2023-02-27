from yt_commands import (
    create, create_user, remove_user, remove, add_member, sync_create_cells, sync_remove_tablet_cells, ls,
    execute_command, set, get_connection_config, wait, get, print_debug)

from yt.common import YtError

import time
import pytest

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

from yt.environment import YTInstance
from yt.environment.configs_provider import init_singletons, init_jaeger_collector, _init_logging


class QueryTracker:
    BINARY = "ytserver-query-tracker"
    LOWERCASE_NAME = "query_tracker"
    DASHED_NAME = "query-tracker"
    HUMAN_READABLE_NAME = "query trackers"

    def __init__(self, env: YTInstance, count: int):
        self.env = env
        self.pids = []
        configs, addresses = self.build_configs(count, env.yt_config, env._cluster_configuration["cluster_connection"],
                                                env._open_port_iterator, env.logs_path)
        self.addresses = addresses
        self.config_paths = env.prepare_external_component(
            self.BINARY,
            self.LOWERCASE_NAME,
            self.HUMAN_READABLE_NAME,
            configs)
        create("document", "//sys/query_tracker/config", recursive=True, force=True, attributes={"value": {}})

    @staticmethod
    def get_default_config():
        return {
            "user": "query_tracker",
            "create_state_tables_on_startup": True,
        }

    def build_configs(self, count, yt_config, cluster_connection, ports_generator, logs_dir):
        configs = []
        addresses = []

        for index in xrange(count):
            config = self.get_default_config()

            init_singletons(config, yt_config, index)

            init_jaeger_collector(config, "query_tracker", {"query_tracker_index": str(index)})

            config["cluster_connection"] = cluster_connection
            config["rpc_port"] = next(ports_generator)
            config["monitoring_port"] = next(ports_generator)
            config["logging"] = _init_logging(logs_dir,
                                              "query-tracker-" + str(index),
                                              yt_config,
                                              has_structured_logs=False)

            configs.append(config)
            addresses.append("{}:{}".format(yt_config.fqdn, config["rpc_port"]))

        return configs, addresses

    def __enter__(self):
        self.pids = self.env.run_yt_component(self.DASHED_NAME, self.config_paths, name=self.LOWERCASE_NAME)
        for address in self.addresses:
            wait(lambda: get(f"//sys/query_tracker/instances/{address}/orchid/service/version",
                             verbose=False, verbose_error=False),
                 ignore_exceptions=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.env.kill_service("query_tracker")
        remove("//sys/query_tracker/instances", recursive=True, force=True)


@pytest.fixture
def query_tracker_environment():
    create_user("query_tracker")
    add_member("query_tracker", "superusers")
    sync_create_cells(1)
    query_tracker_config = {
        "stages": {
            "production": {},
        },
    }
    set("//sys/@cluster_connection/query_tracker", query_tracker_config)
    set("//sys/clusters/primary/query_tracker", query_tracker_config)
    wait(lambda: get_connection_config(verbose=False)
         .get("query_tracker").get("stages").get("production") is not None)
    yield
    remove("//sys/@cluster_connection/query_tracker")
    remove("//sys/clusters/primary/query_tracker")
    sync_remove_tablet_cells(ls("//sys/tablet_cells"))
    remove_user("query_tracker")
    remove("//sys/query_tracker", recursive=True, force=True)


@pytest.fixture
def query_tracker(request, query_tracker_environment):
    cls = request.cls
    count = getattr(cls, "NUM_QUERY_TRACKERS", 1)
    with QueryTracker(cls.Env, count) as query_tracker:
        yield query_tracker


class Query:
    def __init__(self, query_id):
        self.query_id = query_id
        self._poll_frequency = 0.1

    def get(self, **kwargs):
        return get_query(self.query_id, **kwargs)

    def abort(self, **kwargs):
        return abort_query(self.query_id, **kwargs)

    def track(self):
        counter = 0
        while True:
            query = self.get(attributes=["state", "error"], verbose=False)
            if counter % 10 == 0 or query["state"] in ("failed", "aborted", "completed"):
                print_debug(f"Query {self.query_id}: {query['state']}")
            if query["state"] in ("failed", "aborted"):
                raise YtError.from_dict(query["error"])
            elif query["state"] == "completed":
                return
            time.sleep(self._poll_frequency)
            counter += 1

    def get_state(self):
        return self.get(attributes=["state"])["state"]

    def get_error(self):
        return YtError.from_dict(self.get(attributes=["error"])["error"])


def start_query(engine, query, **kwargs):
    kwargs["engine"] = engine
    kwargs["query"] = query
    query_id = execute_command("start_query", kwargs, parse_yson=True)
    return Query(query_id)


def get_query(query_id, **kwargs):
    kwargs["query_id"] = query_id
    return execute_command("get_query", kwargs, parse_yson=True, unwrap_v4_result=False)


def list_queries(**kwargs):
    return execute_command("list_queries", kwargs, parse_yson=True)


def abort_query(query_id, **kwargs):
    kwargs["query_id"] = query_id
    return execute_command("abort_query", kwargs)
