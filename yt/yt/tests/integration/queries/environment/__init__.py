from yt_commands import (
    create, create_user, remove_user, remove, add_member, sync_create_cells, sync_remove_tablet_cells, ls,
    execute_command, execute_command_with_output_format, set, get_connection_config, wait, get, print_debug)

from yt.common import YtError

import time
import pytest

from yt.environment import ExternalComponent


class QueryTracker(ExternalComponent):
    LOWERCASE_NAME = "query_tracker"
    DASHED_NAME = "query-tracker"
    PLURAL_HUMAN_READABLE_NAME = "query trackers"

    @staticmethod
    def get_default_config():
        return {
            "user": "query_tracker",
            "create_state_tables_on_startup": True,
        }

    def wait_for_readiness(self, address):
        wait(lambda: get(f"//sys/query_tracker/instances/{address}/orchid/service/version",
                         verbose=False, verbose_error=False),
             ignore_exceptions=True)

    def on_finish(self):
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
    set("//sys/clusters/primary/query_tracker", query_tracker_config)
    create("document", "//sys/query_tracker/config", recursive=True, force=True, attributes={"value": {}})
    wait(lambda: get_connection_config(verbose=False)
         .get("query_tracker", {}).get("stages", {}).get("production") is not None)
    yield
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
    def __init__(self, id):
        self.id = id
        self._poll_frequency = 0.1

    def get(self, **kwargs):
        return get_query(self.id, **kwargs)

    def get_result(self, result_index, **kwargs):
        return get_query_result(self.id, result_index=result_index, **kwargs)

    def read_result(self, result_index, **kwargs):
        return read_query_result(self.id, result_index=result_index, **kwargs)

    def abort(self, **kwargs):
        return abort_query(self.id, **kwargs)

    def alter(self, **kwargs):
        return alter_query(self.id, **kwargs)

    def track(self, ensure_state_order=True, raise_on_unsuccess=True):
        counter = 0
        previous_state = None

        while True:
            query = self.get(attributes=["state", "error"], verbose=False)
            state = query["state"]
            if ensure_state_order:
                self._validate_state_order(previous_state, state)
            if counter % 10 == 0 or state in ("failed", "aborted", "completed") or state != previous_state:
                print_debug(f"Query {self.id}: {state}")
            if state in ("failed", "aborted"):
                if raise_on_unsuccess:
                    raise YtError.from_dict(query["error"])
                else:
                    return
            elif state == "completed":
                return
            time.sleep(self._poll_frequency)
            counter += 1
            previous_state = state

    def get_state(self):
        return self.get(attributes=["state"])["state"]

    def get_error(self):
        return YtError.from_dict(self.get(attributes=["error"])["error"])

    @staticmethod
    def _validate_state_order(previous_state, state):
        if previous_state is None:
            return
        if previous_state == state:
            return

        def fail():
            assert False, f"Invalid state transition {previous_state} -> {state}"

        if previous_state == "aborting" and state != "aborted":
            fail()
        if previous_state == "failing" and state != "failed":
            fail()
        if previous_state == "completing" and state != "completed":
            fail()


def start_query(engine, query, **kwargs):
    kwargs["engine"] = engine
    kwargs["query"] = query
    id = execute_command("start_query", kwargs, parse_yson=True)
    return Query(id)


def get_query(id, **kwargs):
    kwargs["query_id"] = id
    return execute_command("get_query", kwargs, parse_yson=True, unwrap_v4_result=False)


def get_query_result(id, result_index=0, **kwargs):
    kwargs["query_id"] = id
    kwargs["result_index"] = result_index
    return execute_command("get_query_result", kwargs, parse_yson=True, unwrap_v4_result=False)


def read_query_result(id, result_index=0, **kwargs):
    kwargs["query_id"] = id
    kwargs["result_index"] = result_index
    return execute_command_with_output_format("read_query_result", kwargs)


def list_queries(**kwargs):
    return execute_command("list_queries", kwargs, parse_yson=True)


def abort_query(id, **kwargs):
    kwargs["query_id"] = id
    return execute_command("abort_query", kwargs)


def alter_query(id, **kwargs):
    kwargs["query_id"] = id
    execute_command("alter_query", kwargs)
