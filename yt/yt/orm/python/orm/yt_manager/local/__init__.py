from yt.orm.library.common import try_read_from_file

import yt.local as yt_local

import yt.yson as yson

from yt.wrapper.common import generate_uuid

import yt.logger as logger

from yt.environment import arcadia_interop

import os


class LocalYT(object):
    def __init__(self, path, cell_tag, is_replica=False):
        self._path = path
        self._cell_tag = cell_tag
        self._is_replica = is_replica
        self._id_dump_path = os.path.join(path, "yt_id_{}".format(cell_tag))

        # Indicates that we run inside devtools' ytexec environment.
        # TODO(ignat): it is unused now.
        self._ytrecipe = os.environ.get("YT_OUTPUT") is not None

        self._id = None
        self._yt_instance = None

    def _dump_id(self):
        with open(self._id_dump_path, "w") as fout:
            fout.write(self._id)

    def _dump_configuration(self, path, config):
        with open(os.path.join(path, "local_yt_config"), "wb") as fout:
            yson.dump(config, fout, yson_format="pretty")

    def _load_id(self):
        def _on_error():
            logger.exception(
                "Failed to read from file %s during local YT stop",
                self._id_dump_path,
            )

        self._id = try_read_from_file(self._id_dump_path, _on_error)

    def create_client(self):
        client = self._yt_instance.create_client()
        client.orm_local_yt_options = self._local_yt_options
        return client

    def start(self, yt_manager, local_yt_options, package_dir=None):
        self._id = "yt_replica_" if self._is_replica else "yt_" + generate_uuid()[:4]

        if package_dir is not None:
            os.makedirs(self._path)
            path = arcadia_interop.prepare_yt_environment(
                self._path,
                package_dir=package_dir,
                # TODO(ignat): pass true if no ytrecipe and use tmpfs.
                copy_ytserver_all=False,
            )
            os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])

        self._local_yt_options = local_yt_options

        self._yt_instance = yt_local.start(
            wait_tablet_cell_initialization=True,
            path=self._path,
            id=self._id,
            enable_logging_compression=True,
            log_compression_method="zstd",
            cell_tag=self._cell_tag,
            **local_yt_options
        )
        yt_manager._link(self._yt_instance, self._id)
        self._dump_id()
        self._dump_configuration(
            os.path.join(self._path, self._id),
            config=[local_yt_options, package_dir, {"is_replica": self._is_replica}],
        )

    def stop(self, ignore_lock):
        if self._id is None:
            self._load_id()

        if self._id is not None:
            yt_local.stop(self._id, path=self._path, ignore_lock=ignore_lock)

    # Backward compatibility.
    @property
    def yt_instance(self):
        return self._yt_instance


class LocalYTManager(object):
    def __init__(self, path, validate_single_yt=True):
        self._path = path
        self._max_cell_tag = 0
        self._primary_instances_count = 0
        self._validate_single_yt = validate_single_yt

        self._clusters_config = {}
        self._yt_instances = []
        self._local_yts = []

    def _link(self, yt_instance, yt_id):
        client = yt_instance.create_client()
        cluster_connections = client.get("//sys/@cluster_connection")
        self._clusters_config[yt_id] = cluster_connections
        self._yt_instances.append(yt_instance)

        for instance in self._yt_instances:
            client = instance.create_client()
            client.set("//sys/clusters", self._clusters_config)

    def _patch_yt_options(self, local_yt_options):
        local_yt_options.setdefault("rpc_proxy_count", 1)
        local_yt_options.setdefault("http_proxy_count", 1)

    def start_or_find_started_yt(
        self, local_yt_options, package_dir=None, is_replica=False, used_instances=[]
    ):
        self._patch_yt_options(local_yt_options)

        config = (package_dir, local_yt_options, is_replica)
        for yt_config, local_yt in self._local_yts:
            if config == yt_config and local_yt not in used_instances:
                return local_yt

        self._max_cell_tag += 1
        if not is_replica:
            self._primary_instances_count += 1
            if self._validate_single_yt and self._primary_instances_count > 1:
                # TODO(grigminakov): support more flexible validation policy.
                raise Exception(
                    "Only one yt installation is allowed in the test case. "
                    + "Either check requirements compatibility or explicitly disable validation"
                )

        local_yt = LocalYT(self._path, self._max_cell_tag, is_replica=is_replica)
        local_yt.start(self, local_yt_options=local_yt_options, package_dir=package_dir)
        self._local_yts.append((config, local_yt))
        return local_yt

    def stop_managed_yts(self, ignore_lock=False):
        for _, local_yt in self._local_yts:
            local_yt.stop(ignore_lock)

    @staticmethod
    def get_default_package_dir():
        return arcadia_interop.get_default_package_dir()
