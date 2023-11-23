from yt_commands import (
    create, create_user, remove_user, remove, add_member, sync_create_cells, sync_remove_tablet_cells, ls,
    set, get_connection_config, wait, get)
from yt.environment import ExternalComponent

import yt.environment.init_query_tracker_state as init_query_tracker_state

import pytest


pytest_plugins = [
    "yt.test_helpers.authors",
    "yt.test_helpers.set_timeouts",
    "yt.test_helpers.filter_by_category",
    "yt.test_helpers.fork_class"
]


class QueryTracker(ExternalComponent):
    LOWERCASE_NAME = "query_tracker"
    DASHED_NAME = "query-tracker"
    PLURAL_HUMAN_READABLE_NAME = "query trackers"

    def __init__(self, env, count):
        super().__init__(env, count)

        yt_client = env.create_native_client()
        init_query_tracker_state.create_tables_latest_version(yt_client)

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
