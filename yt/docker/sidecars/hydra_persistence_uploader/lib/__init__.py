# -*- coding: utf-8 -*-
import os
import time
import hashlib
import logging
import subprocess
import socket

from dataclasses import dataclass
from datetime import datetime, timedelta

from yt import yson
from yt.wrapper import YtClient
from yt.wrapper import YtHttpResponseError
from yt import wrapper as yt


log = logging.getLogger("periodic." + __name__.split(".")[-1])


def get_hostname():
    return socket.gethostname()


class MyCellTagFromConfig(object):
    def _read_config(self):
        if not os.path.exists(self.master_config):
            raise RuntimeError(f"Master config '{self.master_config}' not found")

        config = None
        with open(self.master_config, "rb") as fh:
            config = yson.load(fh)
        if config is None:
            raise RuntimeError(f"Can't load master config '{self.master_config}'")

        return config

    def _get_cluster_connection(self):
        config = self._read_config()
        cluster_connection = config.get("cluster_connection", None)
        if cluster_connection is None:
            raise RuntimeError(f"Can't get cluster_connection from master config '{self.master_config}'")
        return cluster_connection

    def _get_cells(self):
        cells = []
        cluster_connection = self._get_cluster_connection()
        primary_master = cluster_connection.get("primary_master")
        cells.append(primary_master)

        secondary_masters = cluster_connection.get("secondary_masters", [])
        cells.extend(secondary_masters)

        return cells

    def _cell_id_to_cell_tag(self, cell_id):
        part = cell_id.split("-")[2]
        cell_tag = int(part[:len(part) - 4] , 16)
        return f"{cell_tag}"

    def __init__(self, master_config):
        self.master_config = master_config

    def get_cell_tag(self):
        my_hostname = socket.gethostname()
        cell_tag = None
        cells = self._get_cells()

        for cell in cells:
            for address in cell.get("addresses"):
                host, _ = address.split(":")
                if host == my_hostname:
                    return self._cell_id_to_cell_tag(cell.get("cell_id"))

        if cell_tag is None:
            raise RuntimeError(f"Can't find cell tag for hostname '{my_hostname}' in master config '{self.master_config}'")


class Setup(object):
    def __init__(self, cell_tag):
        self.cell_tag = cell_tag
        self.hostname = socket.gethostname()

        self.master_binary_path = "/usr/bin/ytserver-master"
        self.snapshots_path = "/yt/master-data/master-snapshots"
        self.changelogs_path = "/yt/master-data/master-changelogs"

        self.cypress_root_path = "//sys/admin/snapshots"
        self.snapshots_cypress_subpath = "snapshots"
        self.changelogs_cypress_subpath = "changelogs"
        self.checksums_cypress_subpath = "checksums"
        self.master_binary_cypress_subpath_prefix = "master_binary"

        self.transaction_timeout = 360 * 1000
        self.skip_files_older_than = 4 * 3600  # 4h

    def __str__(self):
        options = [
            f"cell_tag: '{self.cell_tag}'",
            f"hostname: '{self.hostname}'",
            f"cypress_root_path: '{self.cypress_root_path}'",
            f"snapshots_cypress_subpath: '{self.snapshots_cypress_subpath}'",
            f"changelogs_cypress_subpath: '{self.changelogs_cypress_subpath}'",
            f"checksums_cypress_subpath: '{self.checksums_cypress_subpath}'",
            f"master_binary_cypress_subpath_prefix: '{self.master_binary_cypress_subpath_prefix}'",
            f"transaction_timeout: {self.transaction_timeout}",
            f"skip_files_older_than: {self.skip_files_older_than}",
        ]

        return ", ".join(options)


def get_setup(master_config):
    cell_tag = MyCellTagFromConfig(master_config).get_cell_tag()
    return Setup(cell_tag)


def get_yt_client(proxy, token_path):
    if os.path.exists(token_path):
        yt_client = YtClient(proxy, config={"token_path": token_path})
    else:
        token = os.environ.get("YT_TOKEN")
        assert token, "You must set the environment variables YT_TOKEN_FILE or YT_TOKEN"
        yt_client = YtClient(proxy, token)

    yt_client.config["mount_sandbox_in_tmpfs"] = {
        "enable": True,
        "additional_tmpfs_size": 1073741824,
    }
    yt_client.config["write_parallel"]["enable"] = True

    return yt_client


@dataclass
class MasterHydraPersistenceUploaderConfig:
    upload_snapshots: bool = True
    snapshots_expiration_time_sec: int = 60 * 60 * 24  # 1 day
    upload_changelogs: bool = True
    changelogs_expiration_time_sec: int = 60 * 60 * 24  # 1 day


class MasterHydraPersistenceUploaderJob(object):
    @staticmethod
    def cypress_pathname(path):
        return MasterHydraPersistenceUploaderJob.cypress_path_join(
            *yt.ypath_split(path)[:-1])

    @staticmethod
    def cypress_basename(path):
        return yt.ypath_split(path)[-1]

    @staticmethod
    def cypress_path_join(*args):
        return yt.ypath_join(*args)

    def create_cypress_map_nodes_if_needed(self, *args):
        for path in args:
            if not self._yt_client.exists(path):
                self._yt_client.create("map_node", path, recursive=True)
                self._logger.debug("Created missing directory {0}".format(path))

    def _cell_id_to_cell_tag(self, cell_id):
        part = cell_id.split("-")[2]
        cell_tag = int(part[:len(part) - 4] , 16)
        return f"{cell_tag}"

    def _get_my_cell_tag(self, cell, my_hostname):
        cell_id = cell.get("cell_id")
        for a in cell.get("addresses"):
            host, _ = a.split(":")
            if host == my_hostname:
                return self._cell_id_to_cell_tag(cell_id)
        return None

    def _get_cluster_connection_from_config(self, master_config):
        if not os.path.exists(master_config):
            raise RuntimeError(f"Master config '{master_config}' not found")

        config = None
        with open(master_config, "rb") as fh:
            config = yson.load(fh)

        return config.get("cluster_connection", {})

    def _get_cluster_cells(self, master_config):
        cluster_connection = self._get_cluster_connection_from_config(master_config)
        cells = []

        primary_master = cluster_connection.get("primary_master")
        cells.append(primary_master)

        secondary_masters = cluster_connection.get("secondary_masters", [])
        cells.extend(secondary_masters)

        return cells

    def get_my_cell_tag(self):
        # master_config = "/config/ytserver-master.yson"
        master_config = "/tmp/1.txt"
        cells = self._get_cluster_cells(master_config)
        for cell in cells:
            cell_tag = self._get_my_cell_tag(cell, self._hostname)
            if cell_tag is not None:
                return cell_tag
        return None

    def _is_snapshot_file(self, file_name):
        """
        :type file_name: str, unicode
        :rtype: bool
        """
        return file_name.endswith("snapshot")

    def _is_changelog_file(self, file_name):
        """
        :type file_name: str, unicode
        :rtype: bool
        """
        return file_name.endswith("log")

    def _is_file_fresh(self, file_path):
        """
        :type file_path: str, unicode
        :rtype: bool
        """
        current_time = time.time()
        return (current_time - os.stat(file_path).st_mtime) < self._setup.skip_files_older_than

    def get_files_from_disk(self, files_path, filter):
        files_on_disk = []
        for file in sorted(os.listdir(files_path)):
            if filter(file) is False:
                continue

            files_on_disk.append(file)
        return files_on_disk

    def compute_file_checksum(self, file_path):
        self._logger.debug("Computing snapshot checksum from disk: '{}'".format(file_path))
        hasher = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(2**16), b''):
                hasher.update(chunk)
        return hasher.hexdigest()

    def get_meta_path_from_cypress(self):
        return self.cypress_path_join(self._setup.cypress_root_path, "meta")

    def get_master_path_from_cypress(self, local_version):
        master_binary_cypress_path = self.cypress_path_join(
            self.get_meta_path_from_cypress(),
            self._setup.master_binary_cypress_subpath_prefix)
        master_binary_cypress_path = f"{master_binary_cypress_path}_{local_version}"

        return master_binary_cypress_path

    def get_snapshot_path_on_disk(self, snapshot_name):
        """
        :type snapshot_name: str, unicode
        """
        return os.path.join(self._setup.snapshots_path, snapshot_name)

    def get_files_path_from_cypress(self, parent_dir):
        return self.cypress_path_join(self._setup.cypress_root_path,
                                      self._setup.cell_tag,
                                      parent_dir)

    def get_file_path_from_cypress(self, parent_dir, snapshot_name):
        """
        :type parent_dir: str, unicode
        :type snapshot_name: str, unicode
        :return: str, unicode
        """
        return self.cypress_path_join(self.get_files_path_from_cypress(parent_dir),
                                      snapshot_name)

    def get_local_master_version(self):
        return subprocess.check_output([self._setup.master_binary_path, "--version"]).decode("utf-8").strip()

    def get_snapshots_from_cypress(self):
        """
        :rtype: list
        """
        return self._yt_client.list(self.get_files_path_from_cypress(self._setup.snapshots_cypress_subpath))[::-1]

    def get_changelogs_from_cypress(self):
        """
        :rtype: list
        """
        return self._yt_client.list(self.get_files_path_from_cypress(self._setup.changelogs_cypress_subpath))[::-1]

    def get_snapshot_local_name(self, snapshot_cypress_name):
        delimiter = snapshot_cypress_name.find("_")
        if delimiter < 0:
            return snapshot_cypress_name
        return snapshot_cypress_name[:delimiter]

    def get_snapshot_cypress_name(self, snapshot, snapshot_checksum):
        return "{}_{}".format(snapshot, snapshot_checksum[:8])

    def get_checksum_from_cypress(self, checksums_cypress_path, checksum_cypress_path):
        if not self._yt_client.exists(checksums_cypress_path):
            try:
                self._yt_client.create("file",
                                       checksums_cypress_path,
                                       attributes={"expiration_time": "{}".format(datetime.today() + timedelta(days=2)),
                                                   "checksum_by_host": yt.yson.YsonMap()})
            except YtHttpResponseError:
                # File was created by another host.
                pass

        try:
            return self._yt_client.get(checksum_cypress_path)
        except YtHttpResponseError:
            # Checksum wasn't computed yet.
            return None

    def get_or_compute_snapshot_checksum(self, snapshot):
        checksums_cypress_path = self.cypress_path_join(
            self.get_files_path_from_cypress(self._setup.checksums_cypress_subpath),
            snapshot)
        checksum_cypress_path = "{}/@checksum_by_host/{}".format(checksums_cypress_path, self._setup.hostname)

        checksum = self.get_checksum_from_cypress(checksums_cypress_path, checksum_cypress_path)
        if checksum is None:
            checksum = self.compute_file_checksum(self.get_snapshot_path_on_disk(snapshot))
            self._yt_client.set(checksum_cypress_path, checksum)
        return checksum

    def try_master_binary_upload(self, local_master_version):
        master_binary_cypress_path = self.get_master_path_from_cypress(local_master_version)
        if self._yt_client.exists(master_binary_cypress_path):
            self._logger.debug("Master binary already exists by path '%s'", master_binary_cypress_path)
            return True

        with self._yt_client.Transaction(timeout=self._setup.transaction_timeout):
            try:
                self._yt_client.lock(
                    self.cypress_pathname(master_binary_cypress_path),
                    mode="shared",
                    child_key=self.cypress_basename(master_binary_cypress_path))
            except yt.YtResponseError as err:
                if err.is_cypress_transaction_lock_conflict():
                    return False
                raise

            if self._yt_client.exists(master_binary_cypress_path):
                self._logger.debug("Master binary already exists by path '%s'", master_binary_cypress_path)
                return True

            self._yt_client.create(
                "file",
                master_binary_cypress_path,
                attributes={
                    "executable": True,
                    "master_version": local_master_version,
                    "expiration_time": "{}".format(datetime.today() + timedelta(days=180))})

            self._logger.debug("Trying to to upload master binary to '%s'", master_binary_cypress_path)
            self._yt_client.write_file(master_binary_cypress_path, open(self._setup.master_binary_path, "rb"))
            self._logger.debug("Master binary was successfully uploaded")

        return True

    def try_snapshot_upload(self, snapshot_name, snapshot_cypress_name, snapshot_checksum, local_master_version):
        snapshots_path_in_cypress = self.get_files_path_from_cypress(self._setup.snapshots_cypress_subpath)
        snapshot_path_in_cypress = self.cypress_path_join(snapshots_path_in_cypress, snapshot_cypress_name)

        with self._yt_client.Transaction(timeout=self._setup.transaction_timeout):
            try:
                self._yt_client.lock(
                    snapshots_path_in_cypress,
                    mode="shared",
                    child_key=snapshot_cypress_name)
            except Exception as err:
                self._logger.debug("Failed to upload snapshot '%s' due to taken lock: %s", snapshot_path_in_cypress, err)
                return

            if self._yt_client.exists(snapshot_path_in_cypress):
                self._logger.debug("Snapshot '%s' found in Cypress", snapshot_path_in_cypress)
                return

            self._logger.debug("Uploading snapshot '%s'", snapshot_path_in_cypress)

            self._yt_client.create("file",
                                   snapshot_path_in_cypress,
                                   attributes={"expiration_time": "{}".format(datetime.today() + timedelta(seconds=self._config.snapshots_expiration_time_sec)),
                                               # This field is for debug purposes.
                                               "hostname": self._setup.hostname,
                                               "checksum": snapshot_checksum,
                                               "master_version": local_master_version})
            with open(self.get_snapshot_path_on_disk(snapshot_name), "rb") as fh:
                self._yt_client.write_file(snapshot_path_in_cypress, fh)

            self._logger.debug("Snapshot '%s' was successfully uploaded.", snapshot_path_in_cypress)

    def run_master_snapshots_uploader(self, local_master_version):
        def snapshots_filter(snapshot_file):
            if not self._is_snapshot_file(snapshot_file):
                self._logger.debug("File was skipped: '%s'", snapshot_file)
                return False

            path_to_snapshot_file = os.path.join(self._setup.snapshots_path, snapshot_file)
            if not self._is_file_fresh(path_to_snapshot_file):
                return False

            self._logger.debug("Fresh snapshot found on disk: '%s'", snapshot_file)
            return True

        snapshots_on_disk = self.get_files_from_disk(self._setup.snapshots_path, snapshots_filter)
        if len(snapshots_on_disk) == 0:
            raise RuntimeError("Snapshots directory '{}' is empty!".format(self._setup.snapshots_path))

        snapshots_in_cypress = set(self.get_snapshots_from_cypress())

        for snapshot in snapshots_on_disk:
            snapshot_checksum = self.get_or_compute_snapshot_checksum(snapshot)
            snapshot_cypress_name = self.get_snapshot_cypress_name(snapshot, snapshot_checksum)

            if snapshot_cypress_name not in snapshots_in_cypress:
                self.try_snapshot_upload(snapshot, snapshot_cypress_name, snapshot_checksum, local_master_version)
            else:
                self._logger.debug("Snapshot found in Cypress: '%s'", snapshot_cypress_name)

    def try_changelog_upload(self, changelog_name, local_master_version):
        """
        :type changelog_name: str, unicode
        """
        changelogs_path_in_cypress = self.get_files_path_from_cypress(self._setup.changelogs_cypress_subpath)
        changelog_path_in_cypress = self.cypress_path_join(changelogs_path_in_cypress, changelog_name)

        with self._yt_client.Transaction(timeout=self._setup.transaction_timeout):
            try:
                self._yt_client.lock(
                    changelogs_path_in_cypress,
                    mode="shared",
                    child_key=changelog_name)
            except Exception as err:
                self._logger.debug("Failed to upload changelog '%s' due to taken lock: %s", changelog_path_in_cypress, err)
                return

            if self._yt_client.exists(changelog_path_in_cypress):
                self._logger.debug("Changelog '%s' found in Cypress", changelog_path_in_cypress)
                return

            self._logger.debug("Uploading changelog '%s'", changelog_path_in_cypress)

            self._yt_client.create("file",
                                   changelog_path_in_cypress,
                                   attributes={"expiration_time": "{}".format(datetime.today() + timedelta(seconds=self._config.changelogs_expiration_time_sec)),
                                               # This field is for debug purposes.
                                               "hostname": self._setup.hostname,
                                               "master_version": local_master_version})

            chaneglog_path_on_disk = os.path.join(self._setup.changelogs_path, changelog_name)
            with open(chaneglog_path_on_disk, "rb") as fh:
                self._yt_client.write_file(changelog_path_in_cypress, fh)

            self._logger.debug("Changelog '%s' was successfully uploaded.", changelog_path_in_cypress)

    def run_master_changelogs_uploader(self, local_master_version):
        def changelogs_filter(changelog_file):
            if not self._is_changelog_file(changelog_file):
                self._logger.debug("File was skipped: '%s'", changelog_file)
                return False

            path_to_changelog_file = os.path.join(self._setup.changelogs_path, changelog_file)
            if not self._is_file_fresh(path_to_changelog_file):
                return False

            self._logger.debug("Fresh changelog found on disk: '%s'", changelog_file)
            return True

        # Changelogs are sorted in chronological order, skipping the last one since it may be written to.
        changelogs_on_disk = self.get_files_from_disk(self._setup.changelogs_path, changelogs_filter)[:-1]
        changelogs_in_cypress = set(self.get_changelogs_from_cypress())

        for changelog in changelogs_on_disk:
            if changelog not in changelogs_in_cypress:
                self.try_changelog_upload(changelog, local_master_version)
            else:
                self._logger.debug("Changelog found in Cypress: '%s'", changelog)

    def __call__(self):
        self.run()

    def run(self):
        snapshots_path = self.get_files_path_from_cypress(self._setup.snapshots_cypress_subpath)
        changelogs_path = self.get_files_path_from_cypress(self._setup.changelogs_cypress_subpath)
        checksums_path = self.get_files_path_from_cypress(self._setup.checksums_cypress_subpath)
        meta_path = self.get_meta_path_from_cypress()

        self.create_cypress_map_nodes_if_needed(checksums_path, meta_path)

        if self._config.upload_snapshots:
            self.create_cypress_map_nodes_if_needed(snapshots_path)

        if self._config.upload_changelogs:
            self.create_cypress_map_nodes_if_needed(changelogs_path)

        local_master_version = self.get_local_master_version()

        if not self.try_master_binary_upload(local_master_version):
            self._logger.debug("Hydra persistence upload is skipped due to master binary '%s' being missing and relative lock being taken", local_master_version)
            return False

        if self._config.upload_snapshots:
            self.run_master_snapshots_uploader(local_master_version)
        if self._config.upload_changelogs:
            self.run_master_changelogs_uploader(local_master_version)

    def __init__(self, setup, yt_client, config: MasterHydraPersistenceUploaderConfig = MasterHydraPersistenceUploaderConfig(), logger=None):
        self._setup = setup
        self._yt_client = yt_client
        self._config = config
        self._logger = logger
        if self._logger is None:
            self._logger = log
