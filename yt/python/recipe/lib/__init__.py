import json

import yatest.common
from yt import wrapper as yt

__all__ = ["get_yt_cluster"]

_YT_CLUSTER_INFO_FILE = "yt_cluster_info.json"


class _YtServer:
    def __init__(self, cfg):
        self.yt_id = cfg["id"]
        self.server = cfg["server"]
        self.client = yt.YtClient(proxy=self.server)

    def get_yt_client(self):
        return self.client

    def get_server(self):
        return self.server


class _YtCluster:
    """Contain information about whole yt cluster.

    Allows to get server info by sequence number and by id.
    cluster[0] - first server in cluster
    cluster["primary"] - server with id "primary"
    cluster.primary  - alternative variant to get primary cluster
    """

    def __init__(self, config):
        servers = {}
        for i, x in enumerate(config):
            server = _YtServer(x)
            servers[i] = server
            servers[server.yt_id] = server
        self._servers = servers
        self._count = len(config)

    @property
    def master(self):
        """Returns master server."""
        return self._servers[0]

    @property
    def replicas(self):
        """Returns list of slave servers."""
        return [self._servers[i] for i in range(1, self._count)]

    def __len__(self):
        return self._count

    def __getitem__(self, item):
        return self._servers[item]

    def __getattr__(self, item):
        return self._servers[item]


def get_yt_cluster():
    with open(yatest.common.output_path(_YT_CLUSTER_INFO_FILE), "r") as fd:
        return _YtCluster(json.load(fd))


def dump_yt_cluster(servers):
    """Saves yt cluster information in json file (for internal use only)."""
    server_list = []
    for server in servers:
        server_list.append({"id": server.yt_id, "server": "localhost:%d" % server.yt_proxy_port})

    with open(yatest.common.output_path(_YT_CLUSTER_INFO_FILE), "w") as fd:
        json.dump(server_list, fd)
