from .conftest import Cli

from yp.local import (
    ACTUAL_DB_VERSION,
    INITIAL_DB_VERSION,
    Freezer,
)

from yp.common import YtError, wait

from yt.wrapper import ypath_join
from yt.wrapper.errors import YtTabletNotMounted

import pytest

import os
import re
import time


class YpAdminCli(Cli):
    def __init__(self):
        super(YpAdminCli, self).__init__("yp/python/yp/bin/yp_admin_make/yp-admin")


def get_yt_proxy_address(yp_env):
    return yp_env.yp_instance.yt_instance.get_proxy_address()


ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS = dict(http_proxy_count=1)


def get_db_version(yp_env, yp_path):
    cli = YpAdminCli()
    output = cli.check_output(
        ["get-db-version", "--yt-proxy", get_yt_proxy_address(yp_env), "--yp-path", yp_path]
    )
    match = re.match("^Database version: ([0-9]+)$", output)
    assert match is not None
    return int(match.group(1))


@pytest.mark.usefixtures("yp_env_configurable")
class TestAdminCli(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def test_get_db_version(self, yp_env_configurable):
        assert get_db_version(yp_env_configurable, "//yp") == ACTUAL_DB_VERSION

    def test_validate_db(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output(
            [
                "validate-db",
                "--address",
                yp_env_configurable.yp_instance.yp_client_grpc_address,
                "--config",
                "{enable_ssl=%false}",
            ]
        )
        assert output == ""

    def test_diff_db(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output(
            [
                "diff-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
                "--src-yp-path",
                "//yp",
            ]
        )
        assert output == ""

    def test_dump_db(self, yp_env_configurable, tmpdir):
        cli = YpAdminCli()
        dump_dir_path = str(tmpdir.mkdir("yp_dump_db_test"))
        output = cli.check_output(
            [
                "dump-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
                "--dump-dir",
                dump_dir_path,
            ]
        )
        assert output == ""

        tables = os.listdir(os.path.join(dump_dir_path, "tables"))
        assert all(x in tables for x in ("pods", "resources", "groups"))

    def test_backup_without_path_argument(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output(
            [
                "backup",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
            ]
        )
        assert output == ""

        yp_client = yp_env_configurable.yp_client
        yp_client.select_objects("account", selectors=["/meta/id"])

    def test_trim_table(self, yp_env_configurable):
        cli = YpAdminCli()
        yt_client = yp_env_configurable.yp_instance.create_yt_client()
        yp_client = yp_env_configurable.yp_client

        yp_client.create_object("pod_set")
        assert len(list(yt_client.select_rows("[object_id] from [//yp/db/pod_sets_watch_log]"))) > 0

        output = cli.check_output(
            [
                "trim-table",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
                "pod_sets_watch_log",
            ]
        )
        assert output == ""

        assert (
            len(list(yt_client.select_rows("[object_id] from [//yp/db/pod_sets_watch_log]"))) == 0
        )

    def test_update_finalization_timestamp(self, yp_env_configurable):
        yt_client = yp_env_configurable.yt_client

        def get_timestamp():
            return yt_client.get("//yp/db/@finalization_timestamp")

        timestamp = get_timestamp()

        cli = YpAdminCli()
        cli.check_output(
            [
                "update-finalization-timestamp",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
            ]
        )

        assert get_timestamp() > timestamp


@pytest.mark.usefixtures("yp_env_configurable")
class TestAdminCliInitAndMigrateDb(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def test_init_and_migrate_db(self, yp_env_configurable):
        yt_client = yp_env_configurable.yp_instance.create_yt_client()
        yp_path = "//yp_init_db_test"
        yt_client.create("map_node", yp_path)

        cli = YpAdminCli()
        output = cli.check_output(
            [
                "init-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                yp_path,
            ]
        )
        assert output == ""

        def assert_db(version):
            assert get_db_version(yp_env_configurable, yp_path) == version
            db_tables = yt_client.list(ypath_join(yp_path, "db"))
            assert all(x in db_tables for x in ("pods", "pod_sets", "node_segments"))

        assert_db(INITIAL_DB_VERSION)

        output = cli.check_output(
            [
                "migrate-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                yp_path,
                "--version",
                str(ACTUAL_DB_VERSION),
                "--no-backup",
            ]
        )
        assert output == ""
        assert_db(ACTUAL_DB_VERSION)


class TestAdminCliDryRunMigrateDb(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def test_dry_run_migrate_db(self, yp_env_configurable):
        yt_client = yp_env_configurable.yp_instance.create_yt_client()
        yp_path = "//yp_init_db_test"
        yt_client.create("map_node", yp_path)

        cli = YpAdminCli()
        output = cli.check_output(
            [
                "init-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                yp_path,
            ]
        )
        assert output == ""

        def assert_db(version):
            assert get_db_version(yp_env_configurable, yp_path) == version
            db_tables = yt_client.list(ypath_join(yp_path, "db"))
            assert all(x in db_tables for x in ("pods", "pod_sets", "node_segments"))

        assert_db(INITIAL_DB_VERSION)

        output = cli.check_output(
            [
                "migrate-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                yp_path,
                "--version",
                str(ACTUAL_DB_VERSION),
                "--dry-run",
            ]
        )
        assert output == ""
        assert_db(INITIAL_DB_VERSION)


class TestAdminCliReadOnlyMigrateDb(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def test_read_only_migrate_db(self, yp_env_configurable):
        yt_client = yp_env_configurable.yp_instance.create_yt_client()
        yp_path = "//yp_init_db_test"
        yt_client.create("map_node", yp_path)

        cli = YpAdminCli()
        output = cli.check_output(
            [
                "init-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                yp_path,
            ]
        )
        assert output == ""

        def assert_db(version):
            assert get_db_version(yp_env_configurable, yp_path) == version
            db_tables = yt_client.list(ypath_join(yp_path, "db"))
            assert all(x in db_tables for x in ("pods", "pod_sets", "node_segments"))

        assert_db(INITIAL_DB_VERSION)

        output = cli.check_output(
            [
                "start-readonly-migration",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                yp_path,
                "--version",
                str(ACTUAL_DB_VERSION),
                "--no-backup",
            ]
        )
        advice = output.split()
        assert advice[-2] == "--working-copy-path"
        working_copy_path = advice[-1]
        assert_db(INITIAL_DB_VERSION)

        output = cli.check_output(
            [
                "finish-readonly-migration",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                yp_path,
                "--version",
                str(ACTUAL_DB_VERSION),
                "--working-copy-path",
                working_copy_path,
            ]
        )
        assert output == ""
        assert_db(ACTUAL_DB_VERSION)


@pytest.mark.usefixtures("yp_env_configurable")
class TestAdminCliUnmountMountDb(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def test_unmount_mount_db(self, yp_env_configurable):
        cli = YpAdminCli()
        output = cli.check_output(
            [
                "unmount-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
            ]
        )
        assert output == ""

        yp_client = yp_env_configurable.yp_client
        with pytest.raises(YtError):
            yp_client.select_objects("pod", selectors=["/meta/id"])

        output = cli.check_output(
            [
                "mount-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
            ]
        )
        assert output == ""

        yp_client.select_objects("pod", selectors=["/meta/id"])


@pytest.mark.usefixtures("yp_env_unfreezenable")
class TestAdminCliFreezeUnfreeze(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS

    def _check_unfrozen(self, yp_client, old_id=None):
        new_id = yp_client.create_object("pod_set")
        assert new_id != old_id
        return new_id

    def _check_frozen(self, yp_client, old_id=None):
        with pytest.raises(YtTabletNotMounted):
            yp_client.create_object("pod_set")
        if old_id is not None:
            assert yp_client.get_object("pod_set", old_id, selectors=["/meta/id"])[0] == old_id

    def test_freeze_unfreeze(self, yp_env_unfreezenable):
        yp_client = yp_env_unfreezenable.yp_client
        cli = YpAdminCli()

        pod_set_id = self._check_unfrozen(yp_client)
        output = cli.check_output(
            [
                "freeze-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_unfreezenable),
                "--yp-path",
                "//yp",
            ]
        )
        assert output == ""

        self._check_frozen(yp_client, pod_set_id)

        output = cli.check_output(
            [
                "unfreeze-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_unfreezenable),
                "--yp-path",
                "//yp",
            ]
        )
        assert output == ""

        self._check_unfrozen(yp_client, pod_set_id)

    def test_freezer(self, yp_env_unfreezenable):
        yp_client = yp_env_unfreezenable.yp_client
        pod_set_id = self._check_unfrozen(yp_client)
        with Freezer(yp_env_unfreezenable.yt_client, "//yp"):
            self._check_frozen(yp_client, pod_set_id)
        pod_set_id = self._check_unfrozen(yp_client, pod_set_id)
        with Freezer(yp_env_unfreezenable.yt_client, "//yp") as freezer:
            self._check_frozen(yp_client, pod_set_id)
            freezer.leave_frozen_on_exit()
        self._check_frozen(yp_client, pod_set_id)

    def test_mount_freeze(self, yp_env_unfreezenable):
        yp_client = yp_env_unfreezenable.yp_client
        cli = YpAdminCli()

        pod_set_id = self._check_unfrozen(yp_client)
        output = cli.check_output(
            [
                "unmount-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_unfreezenable),
                "--yp-path",
                "//yp",
            ]
        )
        assert output == ""

        self._check_frozen(yp_client)  # no id check
        with pytest.raises(YtTabletNotMounted):
            yp_client.get_object("pod_set", pod_set_id, selectors=["/meta/id"])[0]

        output = cli.check_output(
            [
                "mount-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_unfreezenable),
                "--yp-path",
                "//yp",
                "--freeze",
            ]
        )
        assert output == ""

        self._check_frozen(yp_client, pod_set_id)

        output = cli.check_output(
            [
                "unfreeze-db",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_unfreezenable),
                "--yp-path",
                "//yp",
            ]
        )
        assert output == ""

        self._check_unfrozen(yp_client, pod_set_id)


@pytest.mark.usefixtures("yp_env_configurable")
class TestAdminCliBackupRestore(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS
    START = False

    def test_backup_restore(self, yp_env_configurable):
        cli = YpAdminCli()
        for mode in ("backup", "restore"):
            output = cli.check_output(
                [
                    mode,
                    "--yt-proxy",
                    get_yt_proxy_address(yp_env_configurable),
                    "--yp-path",
                    "//yp",
                    "--backup-path",
                    "//yp.backup",
                ]
            )
            assert output == ""
        yp_env_configurable._start()
        yp_env_configurable.yp_client.select_objects("pod", selectors=["/meta/id"])


@pytest.mark.usefixtures("yp_env_configurable")
class TestAdminCliCleanupHistory(object):
    LOCAL_YT_OPTIONS = ADMIN_CLI_TESTS_LOCAL_YT_OPTIONS
    START = True

    def _generate_changes(self, yp_client, n=5):
        for i in range(n):
            account_id = yp_client.create_object("account")
            yp_client.create_object(
                object_type="replica_set",
                attributes={
                    "spec": {
                        "account_id": account_id,
                        "revision_id": "42",
                        "replica_count": 32,
                        "deployment_strategy": {
                            "min_available": 21,
                            "max_unavailable": 11,
                            "max_surge": 13,
                        },
                    },
                },
            )

    def _get_timestamp(self):
        return time.time() * 10 ** 6

    def _get_history_events(self, yt_client):
        history_table_path = "//yp/db/history_events"
        return yt_client.select_rows("* from [{}]".format(history_table_path))

    def _count_history_events(self, yt_client):
        return len(list(self._get_history_events(yt_client)))

    def test_cleanup(self, yp_env_configurable):
        yp_client = yp_env_configurable.yp_client
        yt_client = yp_env_configurable.yt_client

        self._generate_changes(yp_client, 4)

        wait(lambda: self._count_history_events(yt_client) == 4)

        start_time = self._get_timestamp()
        self._generate_changes(yp_client, 3)
        finish_time = self._get_timestamp()

        wait(lambda: self._count_history_events(yt_client) == 4 + 3)

        cli = YpAdminCli()
        cli.check_output(
            [
                "cleanup-history",
                "--yt-proxy",
                get_yt_proxy_address(yp_env_configurable),
                "--yp-path",
                "//yp",
                "--start-time",
                str(int(start_time)),
                "--finish-time",
                str(int(finish_time)),
                "--object-type",
                str(13),
            ]
        )

        wait(lambda: self._count_history_events(yt_client) == 4)
