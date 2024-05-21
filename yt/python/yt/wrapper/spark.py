from .cypress_commands import list as yt_list, create
from .ypath import YPath
from .errors import YtHttpResponseError
from .operation_commands import get_operation_url
from .common import require

import os
import logging


class SparkCluster(object):
    def __init__(self, master_endpoint, master_web_ui_url, master_rest_endpoint, operation_id, shs_url):
        self.master_endpoint = master_endpoint
        self.master_web_ui_url = master_web_ui_url
        self.master_rest_endpoint = master_rest_endpoint
        self.operation_id = operation_id
        self.shs_url = shs_url

    def operation_url(self, client=None):
        return get_operation_url(self.operation_id, client=client)


class SparkDiscovery(object):
    def __init__(self, discovery_path=None):
        discovery_path = discovery_path or os.environ.get("SPARK_YT_DISCOVERY_PATH")
        require(discovery_path, "Discovery path argument is not set. Provide either python argument, or CLI argument, "
                                "or SPARK_YT_DISCOVERY_PATH environment variable")
        self.base_discovery_path = YPath(discovery_path)

    @staticmethod
    def get(path, client=None):
        try:
            return yt_list(path, client=client)[0]
        except YtHttpResponseError as err:
            logging.warning("Failed to list {}, error {}".format(path, err.message))
            for inner in err.inner_errors:
                logging.warning("Failed to list {}, inner error {}".format(path, inner["message"]))

    def create(self, client):
        create("map_node", self.discovery(), recursive=True, ignore_existing=True, client=client)
        create("map_node", self.event_log(), recursive=True, ignore_existing=True, client=client)

    def discovery(self):
        return self.base_discovery_path.join("discovery")

    def operation(self):
        return self.discovery().join("operation")

    def logs(self):
        return self.base_discovery_path.join("logs")

    def event_log(self):
        return self.logs().join("event_log")

    def master_spark(self):
        return self.discovery().join("spark_address")

    def master_webui(self):
        return self.discovery().join("webui")

    def master_rest(self):
        return self.discovery().join("rest")

    def shs(self):
        return self.discovery().join("shs")

    def stderr(self):
        return self.logs().join("stderr")


def find_spark_cluster(discovery_path=None, client=None):
    """Print Spark urls
    :param discovery_path: Cypress path for discovery files and logs
    :param client: YtClient
    :return: None
    """
    discovery = SparkDiscovery(discovery_path=discovery_path)
    return SparkCluster(
        master_endpoint=SparkDiscovery.get(discovery.master_spark(), client=client),
        master_web_ui_url=SparkDiscovery.get(discovery.master_webui(), client=client),
        master_rest_endpoint=SparkDiscovery.get(discovery.master_rest(), client=client),
        operation_id=SparkDiscovery.get(discovery.operation(), client=client),
        shs_url=SparkDiscovery.get(discovery.shs(), client=client)
    )
