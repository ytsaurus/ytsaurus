from yt.common import wait

from yt.environment import YTServerComponentBase, YTComponent

import yt.environment.init_query_tracker_state as init_query_tracker_state

from yt.wrapper.common import YtError

import logging
import os

logger = logging.getLogger("YtLocal")


class QueryTracker(YTServerComponentBase, YTComponent):
    LOWERCASE_NAME = "query_tracker"
    DASHED_NAME = "query-tracker"
    PLURAL_HUMAN_READABLE_NAME = "query trackers"

    USER_NAME = "query_tracker"

    def __init__(self):
        super(QueryTracker, self).__init__()
        self.client = None

    def prepare(self, env, config):
        logger.info("Preparing query tracker")

        if env.get_ytserver_all_path() is not None:
            ytserver_all_path = os.path.abspath(env.get_ytserver_all_path())
            os.symlink(ytserver_all_path, os.path.join(env.get_bin_path(), f"ytserver-{self.DASHED_NAME}"))

        super(QueryTracker, self).prepare(env, config)

        if config.get("native_client_supported", False):
            self.client = env.create_native_client()
        else:
            self.client = env.create_client()

        self.client.create("user", attributes={"name": self.USER_NAME})

        self.client.add_member(self.USER_NAME, "superusers")

        self.client.create("document", "//sys/query_tracker/config", recursive=True, force=True, attributes={"value": {}})

        def wait_tablet_cell_initialization():
            tablet_cells = self.client.get("//sys/tablet_cells", attributes=["health"])
            if not tablet_cells:
                raise YtError("No tablet cells, world isn't initialized")
            return list(tablet_cells.values())[0].attributes["health"] == "good"

        wait(wait_tablet_cell_initialization)

        init_query_tracker_state.create_tables_latest_version(self.client)

        logger.info("Query tracker prepared")

    def run(self):
        logger.info("Starting query tracker")
        super(QueryTracker, self).run()

        query_tracker_config = {
            "stages": {
                "production": {
                    "channel": {
                        "addresses": self.addresses,
                    }
                },
                "testing": {
                    "channel": {
                        "addresses": self.addresses,
                    }
                },
            },
        }
        self.client.set(f"//sys/clusters/{self.env.id}/query_tracker", query_tracker_config)

        logger.info("Query tracker started")

    def wait(self):
        logger.info("Waiting for query tracker to become ready")
        wait(lambda: self.client.list_queries(),
             ignore_exceptions=True)

        logger.info("Query tracker ready")

    def init(self):
        logger.info("Initialization for query tracker started")

        self.client.create("access_control_object_namespace", attributes={"name": "queries"})
        self.client.create("access_control_object", attributes={"name": "nobody", "namespace": "queries"})
        for name in ["everyone", "everyone-share"]:
            self.client.create("access_control_object", attributes={
                "name": name,
                "namespace": "queries",
                "principal_acl": [
                    {"action": "allow", "subjects": ["everyone"], "permissions": ["read"], "inheritance_mode": "object_and_descendants"},
                    {"action": "allow", "subjects": ["everyone"], "permissions": ["use"], "inheritance_mode": "object_and_descendants"},
                ],
            })

        logger.info("Initialization for query tracker completed")

    def get_default_config(self):
        return {
            "user": self.USER_NAME,
            "create_state_tables_on_startup": True,
            "solomon_exporter": {
                "export_summary_as_avg": True,
                "read_delay": 1000,
                "shards": {
                    "default": {
                        "filter": ["yt/"],
                    },
                },
                "thread_pool_size": 8,
            },
        }

    def wait_for_readiness(self, address):
        wait(lambda: self.client.get(f"//sys/query_tracker/instances/{address}/orchid/service/version"),
             ignore_exceptions=True)

    def stop(self):
        logger.info("Stopping query tracker")
        super(QueryTracker, self).stop()

        self.client.remove(f"//sys/clusters/{self.env.id}/query_tracker", recursive=True, force=True)
        self.client.remove("//sys/query_tracker", recursive=True, force=True)

        self.client.remove("//sys/access_control_object_namespaces/queries", recursive=True, force=True)
        self.client.remove(f"//sys/users/{self.USER_NAME}")

        logger.info("Query tracker stopped")
