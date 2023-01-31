from yt.wrapper.common import YtError

import yt.wrapper.clickhouse_ctl as chyt_ctl
import yt.wrapper as yt


def _get_cluster_proxy(cluster_proxy):
    return cluster_proxy if cluster_proxy else yt.config["proxy"]["url"]


def _get_result_or_raise(response):
    if "result" in response:
        return response["result"]

    if "error" in response:
        if "to_print" in response:
            raise YtError(response["to_print"], attributes={"error": response["error"]})
        else:
            raise YtError("Error was received from controller API", attributes={"error": response["error"]})

    return response


class ChytControllerClient(object):
    def __init__(self, address=None, cluster_proxy=None):
        self.address = address
        self.cluster_proxy = _get_cluster_proxy(cluster_proxy)

    def execute(self, command_name, params):
        for response in chyt_ctl.make_request_generator(
            command_name=command_name,
            params=params,
            address=self.address,
            cluster_proxy=self.cluster_proxy):
            result = _get_result_or_raise(response)
        return result

    def list(self):
        return self.execute("list", params={})

    def create(self, alias):
        self.execute("create", params={"alias": alias})

    def remove(self, alias):
        self.execute("remove", params={"alias": alias})

    def exists(self, alias):
        return self.execute("exists", params={"alias": alias})

    def status(self, alias):
        return self.execute("status", params={"alias": alias})

    def get_option(self, alias, key):
        return self.execute("get_option", params={
            "alias": alias,
            "key": key,
        })

    def set_option(self, alias, key, value):
        self.execute("set_option", params={
            "alias": alias,
            "key": key,
            "value": value,
        })

    def remove_option(self, alias, key):
        self.execute("remove_option", params={
            "alias": alias,
            "key": key,
        })

    def get_speclet(self, alias):
        return self.execute("get_speclet", params={"alias": alias})

    def set_speclet(self, alias, speclet):
        self.execute("set_speclet", params={
            "alias": alias,
            "speclet": speclet,
        })

    def start(self, alias, untracked=False):
        return self.execute("start", params={
            "alias": alias,
            "untracked": untracked,
        })

    def stop(self, alias):
        self.execute("stop", params={"alias": alias})
