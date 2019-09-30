from .conftest import Cli

from yp.local import ACTUAL_DB_VERSION, INITIAL_DB_VERSION

from yp.common import YtError

from yt.wrapper import ypath_join
from yt.wrapper.errors import YtTabletNotMounted

import pytest

import os
import re


class YpAdminCli(Cli):
    def __init__(self):
        super(YpAdminCli, self).__init__("python/yp/bin", "yp_admin_make", "yp-admin")


def get_yt_proxy_address(yp_env):
    return yp_env.yp_instance.yt_instance.get_proxy_address()


ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS = dict(start_proxy=True)


@pytest.mark.usefixtures("yp_env_configurable")
class TestAdminCli(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def _get_db_version(self, yp_env, yp_path):
        cli = YpAdminCli()
        output = cli.check_output([
            "get-db-version",
            "--yt-proxy", get_yt_proxy_address(yp_env),
            "--yp-path", yp_path,
        ])
        match = re.match("^Database version: ([0-9]+)$", output)
        assert match is not None
        return int(match.group(1))

    def test_get_db_version(self, yp_env_configurable):
        assert self._get_db_version(yp_env_configurable, "//yp") == ACTUAL_DB_VERSION

    def test_validate_db(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output([
            "validate-db",
            "--address", yp_env_configurable.yp_instance.yp_client_grpc_address,
            "--config", "{enable_ssl=%false}"
        ])
        assert output == ""

    def test_init_and_migrate_db(self, yp_env_configurable):
        yt_client = yp_env_configurable.yp_instance.create_yt_client()
        yp_path = "//yp_init_db_test"
        yt_client.create("map_node", yp_path)

        cli = YpAdminCli()
        output = cli.check_output([
            "init-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
            "--yp-path", yp_path
        ])
        assert output == ""

        def assert_db(version):
            assert self._get_db_version(yp_env_configurable, yp_path) == version
            db_tables = yt_client.list(ypath_join(yp_path, "db"))
            assert all(x in db_tables for x in ("pods", "pod_sets", "node_segments"))

        assert_db(INITIAL_DB_VERSION)

        for current_db_version in range(INITIAL_DB_VERSION, ACTUAL_DB_VERSION):
            next_db_version = current_db_version + 1

            output = cli.check_output([
                "migrate-db",
                "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
                "--yp-path", yp_path,
                "--version", str(next_db_version),
                "--no-backup"
            ])
            assert output == ""

            assert_db(next_db_version)

    def test_diff_db(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output([
            "diff-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
            "--yp-path", "//yp",
            "--src-yp-path", "//yp"
        ])
        assert output == ""

    def test_unmount_mount_db(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output([
            "unmount-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
            "--yp-path", "//yp"
        ])
        assert output == ""

        yp_client = yp_env_configurable.yp_client
        with pytest.raises(YtError):
            yp_client.select_objects("pod", selectors=["/meta/id"])

        output = cli.check_output([
            "mount-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
            "--yp-path", "//yp"
        ])
        assert output == ""

        yp_client.select_objects("pod", selectors=["/meta/id"])

    def test_dump_db(self, yp_env_configurable, tmpdir):
        cli = YpAdminCli()
        dump_dir_path = str(tmpdir.mkdir("yp_dump_db_test"))
        output = cli.check_output([
            "dump-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
            "--yp-path", "//yp",
            "--dump-dir", dump_dir_path,
        ])
        assert output == ""

        tables = os.listdir(os.path.join(dump_dir_path, "tables"))
        assert all(x in tables for x in ("pods", "resources", "groups"))

    def test_backup_without_path_argument(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output([
            "backup",
            "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
            "--yp-path", "//yp",
        ])
        assert output == ""

        yp_client = yp_env_configurable.yp_client
        yp_client.select_objects("account", selectors=["/meta/id"])

    def test_trim_table(self, yp_env_configurable):
        cli = YpAdminCli()
        yt_client = yp_env_configurable.yp_instance.create_yt_client()
        yp_client = yp_env_configurable.yp_client

        yp_client.create_object("pod_set")
        assert len(list(yt_client.select_rows("[object_id] from [//yp/db/pod_sets_watch_log]"))) > 0

        output = cli.check_output([
            "trim-table",
            "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
            "--yp-path", "//yp",
            "pod_sets_watch_log",
        ])
        assert output == ""

        assert len(list(yt_client.select_rows("[object_id] from [//yp/db/pod_sets_watch_log]"))) == 0


@pytest.mark.usefixtures("yp_env_unfreezenable")
class TestAdminCliFreezeUnfreeze(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def test_freeze_unfreeze(self, yp_env_unfreezenable):
        yp_client = yp_env_unfreezenable.yp_client
        cli = YpAdminCli()

        pod_set_id = yp_client.create_object("pod_set")
        output = cli.check_output([
            "freeze-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_unfreezenable),
            "--yp-path", "//yp"
        ])
        assert output == ""

        with pytest.raises(YtTabletNotMounted):
            yp_client.create_object("pod_set")
        assert yp_client.get_object("pod_set", pod_set_id, selectors=["/meta/id"])[0] == pod_set_id

        output = cli.check_output([
            "unfreeze-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_unfreezenable),
            "--yp-path", "//yp"
        ])
        assert output == ""

        assert yp_client.get_object("pod_set", pod_set_id, selectors=["/meta/id"])[0] == pod_set_id
        # Verify that write request after unfreeze-db is working
        yp_client.create_object("pod_set")

    def test_mount_freeze(self, yp_env_unfreezenable):
        yp_client = yp_env_unfreezenable.yp_client
        cli = YpAdminCli()

        pod_set_id = yp_client.create_object("pod_set")
        output = cli.check_output([
            "unmount-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_unfreezenable),
            "--yp-path", "//yp"
        ])
        assert output == ""

        with pytest.raises(YtTabletNotMounted):
            yp_client.create_object("pod_set")
        with pytest.raises(YtTabletNotMounted):
            yp_client.get_object("pod_set", pod_set_id, selectors=["/meta/id"])[0]

        output = cli.check_output([
            "mount-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_unfreezenable),
            "--yp-path", "//yp",
            "--freeze"
        ])
        assert output == ""

        with pytest.raises(YtTabletNotMounted):
            yp_client.create_object("pod_set")
        assert yp_client.get_object("pod_set", pod_set_id, selectors=["/meta/id"])[0] == pod_set_id

        output = cli.check_output([
            "unfreeze-db",
            "--yt-proxy", get_yt_proxy_address(yp_env_unfreezenable),
            "--yp-path", "//yp"
        ])
        assert output == ""

        assert yp_client.get_object("pod_set", pod_set_id, selectors=["/meta/id"])[0] == pod_set_id
        # Verify that write request after unfreeze-db is working
        yp_client.create_object("pod_set")


@pytest.mark.usefixtures("yp_env_configurable")
class TestAdminCliBackupRestore(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS
    START = False

    def test_backup_restore(self, yp_env_configurable):
        cli = YpAdminCli()
        for mode in ("backup", "restore"):
            output = cli.check_output([
                mode,
                "--yt-proxy", get_yt_proxy_address(yp_env_configurable),
                "--yp-path", "//yp",
                "--backup-path", "//yp.backup"
            ])
            assert output == ""
        yp_env_configurable._start()
        yp_env_configurable.yp_client.select_objects("pod", selectors=["/meta/id"])
