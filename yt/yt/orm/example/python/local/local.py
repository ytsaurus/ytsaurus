from yt.yt.orm.example.python.admin.db_operations import (
    ACTUAL_DB_VERSION,
    init_yt_cluster,
)
from yt.yt.orm.example.python.admin.data_model_traits import EXAMPLE_DATA_MODEL_TRAITS

from yt.yt.orm.example.python.client.client import ExampleClient

from yt.orm.local.local import OrmInstance

from yt.common import update

from copy import deepcopy


EXAMPLE_RPC_PROXY_CONNECTION_CONFIG_TEMPLATE = {
    "yt_connector": {
        "rpc_proxy_connection": {
            "enable_retries": True,
            "table_mount_cache": {
                "expire_after_successful_update_time": "10s",
                "expire_after_failed_update_time": "0.1s",
                "expire_after_access_time": "10s",
                "refresh_time": "0.1s",
            },
            "retrying_channel": {
                "retry_backoff_time": "0.1s",
                "retry_attempts": 3,
            },
        },
    },
}


EXAMPLE_FEDERATED_CONNECTION_CONFIG_TEMPLATE = {
    "yt_connector": {
        "federated_connection": {
            "rpc_proxy_connections": [],
        },
    },
}

EXAMPLE_MASTER_CONFIG_TEMPLATE = {
    "object_manager": {
        "default_parents_table_mode": "dont_write_to_common_table",
    },
}


class ExampleInstance(OrmInstance):
    def __init__(self, path, yt_manager=None, options=None):
        if options is None:
            options = dict()

        self._use_federated_connection = options.get("use_federated_connection", False)
        self._use_native_connection = options.get("use_native_connection", False)
        self._use_rpc_proxy_connection = not (self._use_native_connection or self._use_federated_connection)
        assert not (self._use_native_connection and self._use_federated_connection), (
            "Cannot simultaneously use native connection and federated connection"
        )

        super(ExampleInstance, self).__init__(
            path,
            yt_manager=yt_manager,
            data_model_traits=EXAMPLE_DATA_MODEL_TRAITS,
            db_version=options.get("db_version", ACTUAL_DB_VERSION),
            init_yt_cluster=init_yt_cluster,
            master_config=options.get("example_master_config"),
            local_yt_options=options.get("local_yt_options"),
            enable_ssl=options.get("enable_ssl"),
            port_locks_path=options.get("port_locks_path"),
            enable_debug_logging=options.get("enable_debug_logging", True),
            require_authentication=self._use_native_connection,
            replica_instance_count=options.get("replica_instance_count", 0),
            avoid_migrations=options.get("avoid_migrations", True),
            master_count=options.get("master_count", 1),
        )

    def get_arcadia_master_binary_path(self):
        return "yt/yt/orm/example/server/bin/ytserver-orm-example"

    def make_client(self, address, config, transport, **kwargs):
        return ExampleClient(address, config=config, transport=transport, **kwargs)

    def get_config_template(self):
        example_config = super(ExampleInstance, self).get_config_template()
        if self._use_rpc_proxy_connection or self._use_federated_connection:
            example_config = update(
                example_config, deepcopy(EXAMPLE_RPC_PROXY_CONNECTION_CONFIG_TEMPLATE)
            )
            example_config["yt_connector"]["rpc_proxy_connection"][
                "cluster_url"
            ] = self.yt_instance.get_http_proxy_address()
        else:
            example_config["rpc_proxy_collocation"] = {}

        if self._use_federated_connection:
            example_config = update(
                example_config, deepcopy(EXAMPLE_FEDERATED_CONNECTION_CONFIG_TEMPLATE)
            )
            example_config["yt_connector"]["federated_connection"]["rpc_proxy_connections"].append(
                deepcopy(example_config["yt_connector"]["rpc_proxy_connection"])
            )

        example_config = update(
            example_config, deepcopy(EXAMPLE_MASTER_CONFIG_TEMPLATE)
        )

        return example_config

    def start(self, example_master_binary_path=None, stop_yt_on_failure=True):
        return super(ExampleInstance, self).start(
            master_binary_path=example_master_binary_path,
            stop_yt_on_failure=stop_yt_on_failure
        )
