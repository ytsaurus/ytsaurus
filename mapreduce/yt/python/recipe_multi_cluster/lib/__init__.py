from mapreduce.yt.python import yt_stuff


class _YtStuffCluster(object):
    def __init__(self, name, index, work_dir, config):
        self._yt_config = yt_stuff.YtConfig(yt_id=name, cell_tag=index, yt_work_dir=work_dir, **config)
        self._cluster = yt_stuff.YtStuff(config=self._yt_config)

    def start_local_yt(self):
        self._cluster.start_local_yt()

    def stop_local_yt(self):
        self._cluster.stop_local_yt()

    def get_cluster_config(self):
        return self._cluster.get_cluster_config()

    def get_yt_client(self):
        return self._cluster.get_yt_client()

    def get_proxy_address(self):
        return self._cluster.get_server()

    @property
    def yt_id(self):
        return self._cluster.yt_id

    @property
    def yt_work_dir(self):
        return self._cluster.yt_work_dir

    @property
    def yt_local_exec(self):
        return self._cluster.yt_local_exec

    @property
    def yt_proxy_port(self):
        return self._cluster.yt_proxy_port


def yt_cluster_factory(name, index, work_dir, config):
    return _YtStuffCluster(
        name=name,
        index=index,
        work_dir=work_dir,
        config=config
    )
