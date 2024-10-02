from yt.orm.library.common import wait

from yt.common import update, update_inplace

from yt.environment import arcadia_interop

from yt.orm.admin.db_operations import Freezer, backup_orm

import yt.subprocess_wrapper as subprocess

from yt.wrapper.common import generate_uuid

import yt.wrapper as yt  # noqa: F401

import yatest.common

import copy
import logging
import os
import sys
import time

yatest_common = arcadia_interop.yatest_common

if yatest_common is None:
    sys.path.insert(0, os.path.abspath("../../python"))


def check_over_time(predicate, iter=20, sleep_backoff=1):
    for _ in range(iter):
        if not predicate():
            return False
        time.sleep(sleep_backoff)
    return True


def assert_over_time(predicate, iter=20, sleep_backoff=1):
    for _ in range(iter):
        assert predicate()
        time.sleep(sleep_backoff)


def create_user(orm_env, orm_client, sync_access_control, id=None, grant_permissions=None, labels=None):
    attributes = dict()
    if id is not None:
        attributes["meta"] = dict(id=id)

    if labels is not None:
        attributes["labels"] = labels

    id = orm_client.create_object("user", attributes=attributes)

    if grant_permissions:
        orm_client.update_objects(
            [
                dict(
                    object_type="schema",
                    object_id=object_type,
                    set_updates=[
                        dict(
                            path="/meta/acl/end",
                            value=dict(
                                action="allow",
                                permissions=[permission],
                                subjects=[id],
                            ),
                        ),
                    ],
                )
                for object_type, permission in grant_permissions
            ]
        )

    sync_access_control()

    return id


def create_user_client(orm_env, orm_client, orm_instance, sync_access_control, superuser=False, grant_permissions=None):
    id = create_user(orm_env, orm_client, sync_access_control, grant_permissions=grant_permissions)
    if superuser:
        orm_client.update_object("group", "superusers", set_updates=[{"path": "/spec/members", "value": [id]}])
    sync_access_control()
    return orm_instance.create_client(config={"user": id})


class Cli(object):
    def __init__(self, binary_path):
        if yatest_common is None:
            self._cli_execute = [os.path.basename(binary_path)]
        else:
            self._cli_execute = [yatest_common.binary_path(binary_path)]
        self._env_patch = None

    def _get_env(self):
        env = copy.deepcopy(os.environ)
        if self._env_patch is not None:
            env = update(env, self._env_patch)

        # Disable using token from ssh session in interaction with YT.
        env["RECEIVE_TOKEN_FROM_SSH_SESSION"] = "0"
        env["YT_LOG_LEVEL"] = "DEBUG"

        return env

    def set_env_patch(self, env_patch):
        self._env_patch = copy.deepcopy(env_patch)

    def get_args(self, args):
        return self._cli_execute + args

    def check_call(self, args, stdout=sys.stdout, stderr=sys.stderr):
        return subprocess.check_call(
            self.get_args(args),
            stdout=stdout,
            env=self._get_env(),
            stderr=stderr,
        )

    def check_output(self, args, stderr=sys.stderr):
        subprocess_args = self.get_args(args)
        logging.info("Running {}".format(subprocess_args))

        return subprocess.check_output(subprocess_args, env=self._get_env(), stderr=stderr).strip()


class UpdateHistoryIndexCli(Cli):
    def __init__(self):
        super(UpdateHistoryIndexCli, self).__init__(
            yatest.common.binary_path("yt/yt/orm/tools/history/update_history_index/update_history_index")
        )

    @classmethod
    def do_update_index(
        cls,
        yt_client,  # type: yt.YtClient
        orm_path,
        history_events,
        history_index,
        object_type,
        selectors,
        time_mode,
    ):
        cli = UpdateHistoryIndexCli()
        backup_path = "{}/tmp/backup_{}".format(orm_path, generate_uuid())
        with Freezer(yt_client, orm_path, cls.DATA_MODEL_TRAITS, table_names=[history_events]):
            backup_orm(
                yt_client,
                orm_path,
                cls.DATA_MODEL_TRAITS,
                backup_paths=[backup_path],
                table_names=[history_events],
            )

        proxy_url = yt_client.config["proxy"]["url"]
        index_table_path = "{}/db/{}".format(orm_path, history_index)
        update_args = [
            "--create",
            "--object-type",
            str(cls.CLIENT_TRAITS.prepare_object_type(object_type)),
            "--history-table-path",
            "{}/db/{}".format(backup_path, history_events),
            "--index-table-path",
            index_table_path,
            "--yt-proxy",
            proxy_url,
            "--intermediate-table-path",
            "{}/history_events_tmp".format(backup_path),
            "--history-enabled-attributes-enabled",
            "false",
            "--db-path",
            "{}/db".format(orm_path),
            "--history-time-mode",
            time_mode,
        ]
        cli.check_output(
            update_args + selectors
        )
        merge_args = [
            "--merge",
            "--index-table-path",
            index_table_path,
            "--yt-proxy",
            proxy_url,
            "--intermediate-table-path",
            "{}/history_events_tmp".format(backup_path),
            "--db-path",
            "{}/db".format(orm_path),
            "--history-time-mode",
            time_mode,
        ]
        cli.check_output(merge_args)


def run_migration_operation(
    migrator_cli,
    orm_env,
    operation_config_patch
):
    yt_client = orm_env.yt_client
    db_path = orm_env.db_manager.get_db_path()

    operation_config_path = yatest.common.work_path("operation_config.json")
    with open(operation_config_path, "wb") as file:
        operation_config = {
            "cluster": yt_client.config["proxy"]["url"],
            "destination_cluster": yt_client.config["proxy"]["url"],
            "slices_table": yt.ypath_join(db_path, "slices_" + generate_uuid()),
        }
        update_inplace(operation_config, operation_config_patch)
        file.write(yt.yson.dumps(operation_config))

    worker_config_path = yatest.common.work_path("worker_config.json")
    with open(worker_config_path, "wb") as file:
        worker_config = {
            "cluster": yt_client.config["proxy"]["url"],
            "job_count": 4,
        }
        file.write(yt.yson.dumps(worker_config))

    migrator = migrator_cli()
    migrator.check_output(
        [
            "--operation-config-path",
            operation_config_path,
            "--worker-config-path",
            worker_config_path,
        ]
    )


class HistoryMigratorCli(Cli):
    def __init__(self):
        super(HistoryMigratorCli, self).__init__(
            yatest.common.binary_path("yt/yt/orm/tools/history/history_migrator/history_migrator")
        )

    @classmethod
    def run(
        cls,
        orm_env,
        dry_run,
        old_table,
        new_table,
    ):
        db_path = orm_env.db_manager.get_db_path()
        operation_config = {
            "source_table": yt.ypath_join(db_path, old_table),
            "destination_table_path": yt.ypath_join(db_path, new_table),
            "source_table_columns_filter": "all",
            "source_time_mode": cls.SOURCE_TIME_MODE,
            "target_time_mode": "logical",
            "change_event_type_sign": cls.CHANGE_EVENT_TYPE_SIGN,
            "dry_run": dry_run,
        }

        run_migration_operation(cls, orm_env, operation_config)


class MigrateAttributesCli(Cli):
    @classmethod
    def run(
        cls,
        orm_env,
        object_type,
        object_key_columns,
        target_attributes,
    ):
        db_path = orm_env.db_manager.get_db_path()
        object_type_value = orm_env.client.client_traits.prepare_object_type(object_type)
        operation_config = {
            "source_table": yt.ypath_join(db_path, object_type + "s"),
            "object_type": object_type_value,
            "object_key_columns": object_key_columns,
            "target_attributes": target_attributes,
            "orm_connection_config": {
                "secure": False,
                "discovery_address": orm_env.orm_instance.orm_client_grpc_address,
            },
        }

        run_migration_operation(cls, orm_env, operation_config)


# Waits for master YT connection caches (table mount, permission, etc) to sync with the actual state.
def sync_master_yt_connection_caches(orm_env, object_type, all_watch_logs=False):
    orm_client = orm_env.client

    watch_requests_options = []
    if all_watch_logs:
        orchid_client = orm_env.orchid_client

        watch_logs = [[]]

        def get_watch_logs():
            watch_logs[0] = orchid_client.get("/object_manager/type_handlers/{}/watch_logs".format(object_type))
            return True

        wait(get_watch_logs, ignore_exceptions=True)
        watch_requests_options = [
            dict(
                watch_log=watch_log["name"],
                filter=watch_log["filter"],
                selectors=watch_log["selector"],
            )
            for watch_log in watch_logs[0].values()
        ]
    else:
        watch_requests_options = [dict()]

    orm_env.logger.debug(
        "Syncing with master YT connection caches (object_type: {}, watch_logs: {})".format(
            object_type,
            watch_requests_options,
        )
    )

    timestamp = orm_client.generate_timestamp()
    for options in watch_requests_options:
        assert "start_timestamp" not in options
        assert "object_type" not in options
        options["object_type"] = object_type
        options["start_timestamp"] = timestamp
        options["skip_trimmed"] = True

    def are_caches_in_sync():
        # Successful select implies successful insert in the context of caches,
        # so it is enough to select primary object table and watch log tables.
        orm_client.select_objects(object_type, selectors=["/meta/id"])
        for options in watch_requests_options:
            orm_client.watch_objects(**options)
        return True

    wait(are_caches_in_sync, ignore_exceptions=True)
