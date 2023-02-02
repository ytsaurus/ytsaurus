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
            raise YtError("Error was received from CHYT controller API", attributes={"error": response["error"]})

    return response


class ChytClient(object):
    def __init__(self, address=None, cluster_proxy=None):
        self.address = address
        self.cluster_proxy = _get_cluster_proxy(cluster_proxy)

    def make_controller_request(self, command_name, params):
        for response in chyt_ctl.make_request_generator(
            command_name=command_name,
            params=params,
            address=self.address,
            cluster_proxy=self.cluster_proxy):
            result = _get_result_or_raise(response)
        return result

    def list(self):
        return self.make_controller_request("list", params={})

    def create(self, alias):
        self.make_controller_request("create", params={"alias": alias})

    def remove(self, alias):
        self.make_controller_request("remove", params={"alias": alias})

    def exists(self, alias):
        return self.make_controller_request("exists", params={"alias": alias})

    def status(self, alias):
        return self.make_controller_request("status", params={"alias": alias})

    def get_option(self, alias, key):
        return self.make_controller_request("get_option", params={
            "alias": alias,
            "key": key,
        })

    def set_option(self, alias, key, value):
        self.make_controller_request("set_option", params={
            "alias": alias,
            "key": key,
            "value": value,
        })

    def remove_option(self, alias, key):
        self.make_controller_request("remove_option", params={
            "alias": alias,
            "key": key,
        })

    def get_speclet(self, alias):
        return self.make_controller_request("get_speclet", params={"alias": alias})

    def set_speclet(self, alias, speclet):
        self.make_controller_request("set_speclet", params={
            "alias": alias,
            "speclet": speclet,
        })

    def start(self, alias, untracked=False):
        return self.make_controller_request("start", params={
            "alias": alias,
            "untracked": untracked,
        })

    def stop(self, alias):
        self.make_controller_request("stop", params={"alias": alias})
