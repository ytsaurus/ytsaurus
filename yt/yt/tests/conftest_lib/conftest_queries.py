from yt_commands import (
    sync_create_cells, sync_remove_tablet_cells, ls, set, get)

from yt.environment.components.query_tracker import QueryTracker as QueryTrackerComponent

from yt.environment.helpers import wait_for_dynamic_config_update

import pytest


pytest_plugins = [
    "yt.test_helpers.authors",
    "yt.test_helpers.set_timeouts",
    "yt.test_helpers.filter_by_category",
    "yt.test_helpers.fork_class"
]


class QueryTracker:
    def __init__(self, env, count):
        sync_create_cells(1)
        self.query_tracker = QueryTrackerComponent()
        self.query_tracker.prepare(env, config={"count": count, "native_client_supported": True})

    def __enter__(self):
        self.query_tracker.run()
        self.query_tracker.wait()
        self.query_tracker.init()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.query_tracker.stop()
        sync_remove_tablet_cells(ls("//sys/tablet_cells"))


def update_query_tracker_environment(cls, query_tracker):
    if hasattr(cls, "QUERY_TRACKER_DYNAMIC_CONFIG"):
        dynconfig = getattr(cls, "QUERY_TRACKER_DYNAMIC_CONFIG")

        config = get("//sys/query_tracker/config")
        config["query_tracker"] = dynconfig
        set("//sys/query_tracker/config", config)

        wait_for_dynamic_config_update(query_tracker.query_tracker.client, config, "//sys/query_tracker/instances")


@pytest.fixture
def query_tracker(request):
    cls = request.cls
    count = getattr(cls, "NUM_QUERY_TRACKERS", 1)
    with QueryTracker(cls.Env, count) as query_tracker:
        update_query_tracker_environment(cls, query_tracker)
        yield query_tracker
