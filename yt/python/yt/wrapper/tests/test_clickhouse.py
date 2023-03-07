import yt.wrapper as yt
import yt.clickhouse as chyt
import yt.wrapper.yson as yson
from yt.test_helpers import wait

from yt.clickhouse.test_helpers import get_clickhouse_server_config, get_host_paths

import yt.environment.arcadia_interop as arcadia_interop

import pytest
import os.path

HOST_PATHS = get_host_paths(arcadia_interop, ["ytserver-clickhouse", "clickhouse-trampoline", "ytserver-log-tailer",
                                              "ytserver-dummy"])

DEFAULTS = {
    "memory_footprint": 1 * 1000**3,
    "memory_limit": int(4.5 * 1000**3),
    "host_ytserver_clickhouse_path": HOST_PATHS["ytserver-clickhouse"],
    "host_clickhouse_trampoline_path": HOST_PATHS["clickhouse-trampoline"],
    "host_ytserver_log_tailer_path": HOST_PATHS["ytserver-log-tailer"],
    "cpu_limit": 1,
    "enable_monitoring": False,
    "clickhouse_config": {},
    "uncompressed_block_cache_size": 0,
    "max_instance_count": 100,
    "cypress_log_tailer_config_path": "//sys/clickhouse/log_tailer_config",
    "log_tailer_table_attribute_patch": {"primary_medium": "default"},
    "log_tailer_tablet_count": 1,
}


class ClickhouseTestBase(object):
    def _setup(self):
        yt.create("document", "//sys/clickhouse/defaults", recursive=True, attributes={"value": DEFAULTS})
        yt.create("map_node", "//home/clickhouse-kolkhoz", recursive=True)
        yt.link("//home/clickhouse-kolkhoz", "//sys/clickhouse/kolkhoz", recursive=True)
        yt.create("document", "//sys/clickhouse/log_tailer_config", attributes={"value": get_clickhouse_server_config()})
        cell_id = yt.create("tablet_cell", attributes={"size": 1})
        wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(cell_id)) == "good")
        yt.create("user", attributes={"name": "yt-clickhouse-cache"})
        yt.create("user", attributes={"name": "yt-clickhouse"})
        yt.add_member("yt-clickhouse", "superusers")


@pytest.mark.usefixtures("yt_env")
class TestClickhouseFromHost(ClickhouseTestBase):
    def setup(self):
        self._setup()

    def test_simple(self):
        chyt.start_clique(1, operation_alias="*c")

# Waiting for real ytserver-clickhouse upload is too long, so we
@pytest.mark.usefixtures("yt_env")
class TestClickhouseFromCypress(ClickhouseTestBase):
    def _turbo_write_file(self, destination, path):
        upload_client = yt.YtClient(config=yt.config.get_config(client=None))
        upload_client.config["proxy"]["content_encoding"] = "identity"
        upload_client.config["write_parallel"]["enable"] = False
        upload_client.config["write_retries"]["chunk_size"] = 4 * 1024**3

        yt.create("file", destination, attributes={"replication_factor": 1, "executable": True}, recursive=True)
        upload_client.write_file(destination,
                                 open(path, "rb"),
                                 filename_hint=os.path.basename(path),
                                 file_writer={
                                     "enable_early_finish": True,
                                     "min_upload_replication_factor": 1,
                                     "upload_replication_factor": 1,
                                     "send_window_size": 4 * 1024**3,
                                     "sync_on_close": False,
                                 })

    def setup(self):
        self._setup()
        for destination_bin, source_bin in (
                ("ytserver-clickhouse", "ytserver-dummy"),
                ("clickhouse-trampoline", "clickhouse-trampoline"),
                ("ytserver-log-tailer", "ytserver-dummy"),
        ):
            self._turbo_write_file("//sys/bin/{0}/{0}".format(destination_bin), HOST_PATHS[source_bin])
            yt.remove("//sys/clickhouse/defaults/host_" + destination_bin.replace("-", "_") + "_path")
        yt.set("//sys/bin/ytserver-log-tailer/ytserver-log-tailer/@yt_version", "")

    def test_simple(self):
        chyt.start_clique(1, operation_alias="*c", wait_for_instances=False)
