from yt.environment import arcadia_interop
import yt.local as yt_local

import errno
import os


class _YtLocalCluster(object):
    def __init__(self, name, index, work_dir, config, package_dir):
        self.yt_id = name
        self.yt_work_dir = None
        self.yt_proxy_port = None
        self.yt_local_exec = None

        self._destination = work_dir
        self._yt_cell_tag = (index + 1) * 10
        self._yt_config = config
        self._yt_instance = None
        self._yt_client = None
        self._yt_cluster_config = dict()
        self._package_dir = package_dir

    def prepare_local_yt(self):
        try:
            os.makedirs(self._destination)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        self.yt_work_dir = arcadia_interop.prepare_yt_environment(self._destination, package_dir=self._package_dir)

        os.environ["PATH"] = os.pathsep.join([self.yt_work_dir, os.environ.get("PATH", "")])

        self._yt_instance = yt_local.start(
            path=self.yt_work_dir,
            id=self.yt_id,
            cell_tag=self._yt_cell_tag,
            prepare_only=True,
            **self._yt_config,
        )

    def start_local_yt(self):
        self._yt_instance.start()
        self.yt_proxy_port = int(self._yt_instance.get_http_proxy_address().split(":")[1])
        self.yt_local_exec = [arcadia_interop.search_binary_path("yt_local")]
        self._yt_client = self._yt_instance.create_client()

    def stop_local_yt(self):
        self._yt_instance.stop()

    def get_yt_client(self):
        return self._yt_client

    def get_proxy_address(self):
        return "localhost:%d" % self.yt_proxy_port

    def get_yt_instance(self):
        return self._yt_instance


def yt_cluster_factory(name, index, work_dir, config, package_dir):
    return _YtLocalCluster(
        name=name,
        index=index,
        work_dir=work_dir,
        config=config,
        package_dir=package_dir,
    )
