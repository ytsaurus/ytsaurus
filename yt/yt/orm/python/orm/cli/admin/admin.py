from __future__ import print_function

from yt.orm.library.cli_helpers import (
    AliasedSubParsersAction,
    ReplicaArgument,
    SvnVersionAction,
)

from yt.orm.admin.ban_manager import BanManager
from yt.orm.admin.configure_db_bundle import configure_db_bundle
import yt.orm.admin.db_sync as db_sync

from yt.orm.admin.db_operations import (
    Freezer,
    Migrator,
    backup_orm,
    cleanup_history_table,
    clone_db,
    diff_db,
    dump_db,
    freeze_orm,
    get_db_version,
    restore_orm,
    unfreeze_orm,
)

from yt.orm.admin.db_manager import DbManager, update_db_finalization_timestamp

from yt.orm.admin.reshard_db_table import reshard_tables

from yt.wrapper import YtClient
from yt.wrapper.cli_helpers import ParseStructuredArgument

from yt.common import get_value, update

from abc import ABCMeta, abstractmethod
from argparse import ArgumentParser


class AdminCli(object):
    __metaclass__ = ABCMeta

    def __init__(self, data_model_traits):
        self._data_model_traits = data_model_traits

    ############################################################################

    # Overridable functions.

    def validate_db(self, address, transport, config):
        pass

    def add_custom_parsers(self, subparsers, parent_parser):
        pass

    @abstractmethod
    def init_yt_cluster(self, yt_client, path, **kwargs):
        raise NotImplementedError

    @abstractmethod
    def find_token(self):
        raise NotImplementedError

    ############################################################################

    def get_cli_name(self):
        return "{}-admin".format(self._data_model_traits.get_kebab_case_name())

    def get_replica_path_name(self):
        return "{}-path".format(self._data_model_traits.get_kebab_case_name())

    def get_default_path_arg_name(self):
        return "--{}-path".format(self._data_model_traits.get_kebab_case_name())

    def get_source_path_arg_name(self):
        return "--source-{}-path".format(self._data_model_traits.get_kebab_case_name())

    def get_src_path_arg_name(self):
        return "--src-{}-path".format(self._data_model_traits.get_kebab_case_name())

    def get_target_path_arg_name(self):
        return "--target-{}-path".format(self._data_model_traits.get_kebab_case_name())

    def get_orm_config_arg_name(self):
        return "--{}-config".format(self._data_model_traits.get_kebab_case_name())

    ############################################################################

    def get_default_path(self):
        return self._data_model_traits.get_default_path()

    ############################################################################

    def handle_replica_argument(self, kwargs, supported=False):
        supported = supported and self._data_model_traits.supports_replication()
        if supported:
            # NB! replica kwarg can be both None and missing.
            kwargs["replica_clients"] = list(map(
                lambda replica_data: YtClient(replica_data["yt-proxy"]),
                kwargs.pop("replica", None) or [],
            ))
        else:
            # Remove the argument from #kwargs.
            if kwargs.pop("replica", None):
                raise NotImplementedError("argument --replica is not supported yet")

    def _pop_orm_path(self, kwargs):
        return kwargs.pop("orm_path")

    def _pop_src_orm_path(self, kwargs):
        return kwargs.pop("source_orm_path")

    ############################################################################

    def init_db(self, yt_proxy, **kwargs):
        orm_path = self._pop_orm_path(kwargs)
        self.handle_replica_argument(kwargs, supported=True)
        self.init_yt_cluster(YtClient(yt_proxy), orm_path, **kwargs)

    def dry_run_migration(
        self,
        yt_proxy,
        version,
        keep_snapshot,
        forced_compaction,
        preserve_medium,
        preferred_medium,
        via_merge,
        checkpoint_timestamp_delay,
        checkpoint_check_timeout,
        **kwargs
    ):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        migrator = Migrator(
            YtClient(yt_proxy),
            orm_path,
            version,
            self._data_model_traits,
            forced_compaction=forced_compaction,
            preserve_medium=preserve_medium,
            preferred_medium=preferred_medium,
            use_native_backup=not via_merge,
            checkpoint_timestamp_delay=checkpoint_timestamp_delay,
            checkpoint_check_timeout=checkpoint_check_timeout,
        )
        migrator.migrate_dry_run(remove_snapshot=not keep_snapshot, rethrow_error=kwargs.pop("fail_on_error", False))

    def _migrations_without_downtime_notice(self, migrator, yt_proxy, orm_path):
        available_migrations = migrator.list_migrations_without_downtime(quiet=True)
        if available_migrations:
            print("You have unfinished migrations without downtime: {}".format(available_migrations))
            print("After starting the masters, execute")
            print(
                ("{} migrate-without-downtime --yt-proxy {} {} {}").format(
                    self.get_cli_name(),
                    yt_proxy,
                    self.get_default_path_arg_name(),
                    orm_path,
                )
            )

    def start_readonly_migration(
        self,
        yt_proxy,
        version,
        no_backup,
        mount_wait_time,
        expiration_timeout,
        forced_compaction,
        preserve_medium,
        preferred_medium,
        **kwargs
    ):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        migrator = Migrator(
            YtClient(yt_proxy),
            orm_path,
            version,
            self._data_model_traits,
            expiration_timeout=expiration_timeout,
            mount_wait_time=mount_wait_time,
            forced_compaction=forced_compaction,
            preserve_medium=preserve_medium,
            preferred_medium=preferred_medium,
        )
        if no_backup:
            migrator.set_backup_path(None)

        if not migrator.is_migration_needed():
            print("{} is already at version {}".format(self._data_model_traits.get_human_readable_name(), version))
            self._migrations_without_downtime_notice(migrator, yt_proxy, orm_path)
            return

        working_copy_path, backup_path = migrator.start_readonly_migration()

        print("{} is left frozen.".format(self._data_model_traits.get_human_readable_name()))
        print("Migrated copy is at {}".format(working_copy_path))
        if backup_path:
            print("Backup path is at {}".format(backup_path))
        print("After shutting down the masters, execute")
        print(
            ("{} finish-readonly-migration --yt-proxy {} {} {} --version {} --working-copy-path {}").format(
                self.get_cli_name(),
                yt_proxy,
                self.get_default_path_arg_name(),
                orm_path,
                version,
                working_copy_path,
            )
        )
        self._migrations_without_downtime_notice(migrator, yt_proxy, orm_path)

    def finish_readonly_migration(self, yt_proxy, version, working_copy_path, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        yt_client = YtClient(yt_proxy)
        working_copy_version = get_db_version(yt_client, working_copy_path, self._data_model_traits)
        assert (
            version == working_copy_version
        ), "Actual working copy {} version {} must be equal to the expected {}".format(
            working_copy_path, working_copy_version, version
        )
        migrator = Migrator(yt_client, orm_path, version, self._data_model_traits)
        migrator.finish_readonly_migration(working_copy_path)

    def migrate_without_downtime(self, yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        yt_client = YtClient(yt_proxy)
        migrator = Migrator(yt_client, orm_path, None, self._data_model_traits)
        migrator.migrate_without_downtime(**kwargs)

    def db_version(self, yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)

        yt_client = YtClient(yt_proxy)
        orm_path = self._pop_orm_path(kwargs)
        current_version = get_db_version(yt_client, orm_path, self._data_model_traits)
        latest_version = self._data_model_traits.get_actual_db_version()

        migrator = Migrator(yt_client, orm_path, latest_version, self._data_model_traits)
        pending_live_migrations = migrator.list_migrations_without_downtime(quiet=True) or []

        print("Current version: {}".format(current_version))
        print("Latest version: {}".format(latest_version))
        print("Pending live migrations: {}".format(len(pending_live_migrations)))

        for migration_name in pending_live_migrations:
            print("    - {}".format(migration_name))

        if current_version == latest_version and len(pending_live_migrations) == 0:
            print("\nDatabase is up to date")
        else:
            print("\nDatabase migration is required")

    def backup(self, yt_proxy, backup_path, table, expiration_timeout, preserve_medium, preferred_medium, **kwargs):
        self.handle_replica_argument(kwargs)
        yt_client = YtClient(yt_proxy)
        orm_path = self._pop_orm_path(kwargs)
        with Freezer(yt_client, orm_path, self._data_model_traits, table_names=table):
            backup_paths = [backup_path] if backup_path is not None else None
            backup_orm(
                yt_client,
                orm_path,
                self._data_model_traits,
                backup_paths,
                table_names=table,
                expiration_timeout=expiration_timeout,
                preserve_medium=preserve_medium,
                preferred_medium=preferred_medium,
            )

    def restore(self, yt_proxy, backup_path, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        restore_orm(YtClient(yt_proxy), orm_path, self._data_model_traits, backup_path)

    def unmount_db(self, yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        db_manager = DbManager(YtClient(yt_proxy), orm_path, self._data_model_traits)
        db_manager.unmount_all_tables()

    def mount_db(self, yt_proxy, freeze, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        db_manager = DbManager(YtClient(yt_proxy), orm_path, self._data_model_traits)
        db_manager.mount_all_tables(freeze=freeze)

    def dump(self, yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)
        kwargs["yt_client"] = YtClient(yt_proxy)
        kwargs["data_model_traits"] = self._data_model_traits

        dump_db(**kwargs)

    def validate(self, address, config, **kwargs):
        self.handle_replica_argument(kwargs)
        config = get_value(config, {})
        config = update(dict(token=self.find_token()), config)
        self.validate_db(address, "grpc", config)

    def freeze_db(self, yt_proxy, table, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        freeze_orm(YtClient(yt_proxy), orm_path, self._data_model_traits, table_names=table)

    def unfreeze_db(self, yt_proxy, table, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        unfreeze_orm(YtClient(yt_proxy), orm_path, self._data_model_traits, table_names=table)

    def trim_table(self, yt_proxy, name, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        db_manager = DbManager(YtClient(yt_proxy), orm_path, self._data_model_traits)
        db_manager.trim_table(name)

    def clone(self, source_yt_proxy, target_yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)
        kwargs["source_yt_client"] = YtClient(source_yt_proxy)
        kwargs["target_yt_client"] = YtClient(target_yt_proxy)
        kwargs["data_model_traits"] = self._data_model_traits

        clone_db(**kwargs)

    def diff(self, yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)
        kwargs["target_orm_path"] = self._pop_orm_path(kwargs)
        kwargs["yt_client"] = YtClient(yt_proxy)
        kwargs["data_model_traits"] = self._data_model_traits

        diff_db(**kwargs)

    def cleanup_history(self, yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)
        kwargs["yt_client"] = YtClient(yt_proxy)
        kwargs["data_model_traits"] = self._data_model_traits

        cleanup_history_table(**kwargs)

    def update_finalization_timestamp(self, yt_proxy, **kwargs):
        self.handle_replica_argument(kwargs)
        update_db_finalization_timestamp(YtClient(yt_proxy), self._pop_orm_path(kwargs))

    def reshard_tables(self, **kwargs):
        self.handle_replica_argument(kwargs)
        orm_path = self._pop_orm_path(kwargs)
        kwargs["orm_path"] = orm_path
        reshard_tables(**kwargs)

    def configure_db_bundle(self, **kwargs):
        self.handle_replica_argument(kwargs)
        self._pop_orm_path(kwargs)

        configure_db_bundle(
            per_cluster_tablet_balancer_schedule=self._data_model_traits.get_per_cluster_tablet_balancer_schedule(),
            **kwargs
        )

    def ban_masters(self, yt_proxy, fqdn, ignore_banned, **kwargs):
        self.handle_replica_argument(kwargs)
        assert len(fqdn) > 0, "At least one fqdn should be specified"
        yt_client = YtClient(yt_proxy)
        orm_path = self._pop_orm_path(kwargs)
        ban_manager = BanManager(yt_client, orm_path)
        ban_manager.ban(fqdns=fqdn, ignore_banned_masters=ignore_banned)

    def unban_masters(self, yt_proxy, fqdn, ignore_unbanned, **kwargs):
        self.handle_replica_argument(kwargs)
        assert len(fqdn) > 0, "At least one fqdn should be specified"
        yt_client = YtClient(yt_proxy)
        orm_path = self._pop_orm_path(kwargs)
        ban_manager = BanManager(yt_client, orm_path)
        ban_manager.unban(fqdns=fqdn, ignore_unbanned_masters=ignore_unbanned)

    def db_sync(self, **kwargs):
        db_sync.run(data_model_traits=self._data_model_traits, **kwargs)

    ############################################################################

    def add_init_db_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "init",
            aliases=["init-db"],
            help="Initialize database",
            parents=[parent_parser],
        )
        parser.add_argument("--to-actual-version", action='store_true', default=False)
        if getattr(self._data_model_traits, "INIT_DB_INITS_ACCOUNTS", False):
            parser.add_argument("--dont-initialize-account", action='store_false', default=True, dest='initialize_account')
        else:
            parser.add_argument("--initialize-account", action='store_true', default=False)
        parser.set_defaults(func=self.init_db)
        return parser

    def add_dry_run_migration_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "dry-run-migration",
            help="Migrate database with no effect",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--version",
            default=self._data_model_traits.get_actual_db_version(),
            type=int,
            help="Database version",
        )
        parser.add_argument("--keep-snapshot", action="store_true", default=False)
        parser.add_argument("--fail-on-error", action="store_true", default=False)
        parser.add_argument("--forced-compaction", choices=["skip", "request", "wait"], default="skip")
        parser.add_argument("--preserve-medium", type=bool, default=True)
        parser.add_argument("--preferred-medium", type=str, default="default")
        parser.add_argument(
            "--via-merge",
            action="store_true",
            help="Use merge operation instead of create_table_backup",
            default=False
        )
        parser.add_argument("--checkpoint-timestamp-delay", type=str, default=None)
        parser.add_argument("--checkpoint-check-timeout", type=str, default=None)
        parser.set_defaults(func=self.dry_run_migration)
        return parser

    def add_start_readonly_migration_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "start-readonly-migration",
            help="Start migrating database in read-only mode",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--version",
            default=self._data_model_traits.get_actual_db_version(),
            type=int,
            help="Database version",
        )
        parser.add_argument(
            "--no-backup",
            default=False,
            action="store_true",
            help="Do not backup database before migration",
        )
        parser.add_argument(
            "--mount-wait-time",
            default=200,
            type=int,
            help="How many seconds to wait for tables to be mounted",
        )
        parser.add_argument("--expiration-timeout", type=int, default=None, help="Timeout for backups; 0 - unlimited")
        parser.add_argument("--forced-compaction", choices=["skip", "request", "wait"], default="request")
        parser.add_argument("--preserve-medium", type=bool, default=False)
        parser.add_argument("--preferred-medium", type=str, default="default")
        parser.set_defaults(func=self.start_readonly_migration)
        return parser

    def add_finish_readonly_migration_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "finish-readonly-migration",
            help="Finish migrating database in read-only mode",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--version",
            default=self._data_model_traits.get_actual_db_version(),
            type=int,
            help="Database version",
        )
        parser.add_argument(
            "--working-copy-path",
            required=True,
            help="Location of migrated copy",
        )
        parser.set_defaults(func=self.finish_readonly_migration)
        return parser

    def add_migrate_without_downtime(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "migrate-without-downtime",
            help="Migrate live database",
            parents=[parent_parser],
        )
        parser.add_argument("--sleep-between-phases", type=int, default=120, help="Wait time in seconds")
        parser.add_argument("--dry-run", action='store_true', default=False, help="Dry run mode (partial)")
        parser.add_argument("--backup-first", action='store_true', default=False, help="Backup the DB before each migration")
        parser.add_argument("--migration-name", type=str, default=None, help="Run only this migration")
        parser.set_defaults(func=self.migrate_without_downtime)
        return parser

    def add_db_version_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "get-version",
            aliases=["get-db-version"],
            help="Get database version",
            parents=[parent_parser],
        )
        parser.set_defaults(func=self.db_version)
        return parser

    def add_dump_db_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "dump",
            aliases=["dump-db"],
            help="Dump database to the local filesystem",
            parents=[parent_parser],
        )
        parser.add_argument("--dump-dir", required=True, help="Target directory")
        parser.set_defaults(func=self.dump)
        return parser

    def add_clone_db_parser(self, subparsers):
        parser = subparsers.add_parser(
            "clone",
            aliases=["clone-db"],
            help="Clone database to the remote cluster without downtime",
        )
        parser.add_argument("--source-yt-proxy", required=True)
        parser.add_argument(self.get_source_path_arg_name(), dest="source_orm_path", required=True)
        parser.add_argument("--target-yt-proxy", required=True)
        parser.add_argument(self.get_target_path_arg_name(), dest="target_orm_path", required=True)
        parser.add_argument(
            "--remove-if-exists",
            default=False,
            action="store_true",
            help="Remove existent target beforehand",
        )
        parser.add_argument("--preserve-medium", type=bool, default=False)
        parser.add_argument("--preferred-medium", type=str, default="default")
        parser.add_argument("--preferred-remote-medium", type=str, default="default")
        parser.set_defaults(func=self.clone)
        return parser

    def add_backup_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "backup",
            help="Backup database with downtime",
            parents=[parent_parser],
        )
        parser.add_argument("--backup-path")
        parser.add_argument(
            "--table",
            action="append",
            help="Copies only selected tables",
            required=False,
        )
        parser.add_argument("--expiration-timeout", type=int, default=None)
        parser.add_argument("--preserve-medium", type=bool, default=False)
        parser.add_argument("--preferred-medium", type=str, default="default")
        parser.set_defaults(func=self.backup)
        return parser

    def add_restore_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "restore",
            help="Restore database from backup",
            parents=[parent_parser],
        )
        parser.add_argument("--backup-path")
        parser.set_defaults(func=self.restore)
        return parser

    def add_unmount_db_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "unmount",
            aliases=["unmount-db"],
            help="Unmount database",
            parents=[parent_parser],
        )
        parser.set_defaults(func=self.unmount_db)
        return parser

    def add_mount_db_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "mount",
            aliases=["mount-db"],
            help="Mount database",
            parents=[parent_parser],
        )
        parser.add_argument("--freeze", action="store_true", help="Leave in frozen state")
        parser.set_defaults(func=self.mount_db)
        return parser

    def add_validate_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "validate",
            aliases=["validate-db"],
            help="Validate some database invariants",
        )
        parser.add_argument(
            "--address",
            required=True,
            help="Address of {} backend".format(self._data_model_traits.get_human_readable_name()),
        )
        parser.add_argument(
            "--config",
            required=False,
            help="{} client config".format(self._data_model_traits.get_human_readable_name()),
            action=ParseStructuredArgument,
        )
        parser.set_defaults(func=self.validate)
        return parser

    def add_diff_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "diff",
            aliases=["diff-db"],
            help="Calculate diff between databases",
            parents=[parent_parser],
        )
        parser.add_argument(
            self.get_src_path_arg_name(),
            dest="source_orm_path",
            required=True,
            help="Database path",
        )
        parser.set_defaults(func=self.diff)
        return parser

    def add_freeze_db_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "freeze",
            aliases=["freeze-db"],
            help="Freeze database",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--table",
            action="append",
            help="Freezes only selected tables",
            required=False,
        )
        parser.set_defaults(func=self.freeze_db)
        return parser

    def add_unfreeze_db_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "unfreeze",
            aliases=["unfreeze-db"],
            help="Unfreeze database",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--table",
            action="append",
            help="Unfreezes only selected tables",
            required=False,
        )
        parser.set_defaults(func=self.unfreeze_db)
        return parser

    def add_trim_table_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser("trim-table", help="Trim database table", parents=[parent_parser])
        parser.add_argument("name", help="Table name")
        parser.set_defaults(func=self.trim_table)
        return parser

    def add_cleanup_history_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "cleanup-history",
            help="Filter history table by time and object type",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--start-time",
            help="Timestamp of the first record to delete",
            type=int,
        )
        parser.add_argument(
            "--finish-time",
            help="Timestamp of the last record to delete",
            type=int,
        )
        parser.add_argument(
            "--object-type",
            help="Integer object type (see EObjectType)",
            type=int,
        )
        parser.set_defaults(func=self.cleanup_history)
        return parser

    def add_update_finalization_timestamp_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "update-finalization-timestamp",
            help="Update database finalization timestamp",
            parents=[parent_parser],
        )
        parser.set_defaults(func=self.update_finalization_timestamp)
        return parser

    def add_reshard_tables_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "reshard-tables",
            aliases=["reshard"],
            help="Reshard all tables",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--max-tablets-per-table",
            help="Override the maximum number of tablets per table (default is 5 per YT node)",
            type=int,
            default=None,
        )
        parser.add_argument(
            "--min-bytes-per-tablet",
            help="Override the minimum number of bytes per tablet",
            type=int,
            default=None,
        )
        parser.add_argument(
            "--slicing-accuracy",
            help="Fine-tune the slicing algorithm",
            type=float,
            default=0.1,
        )
        parser.add_argument(
            "--only-tables",
            help="Only reshard these tables",
            nargs="*",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--commit",
            action="store_true",
        )
        parser.set_defaults(func=self.reshard_tables)
        return parser

    def add_configure_bundle_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "configure-bundle",
            aliases=["configure-db-bundle"],
            help="Configure database tablet cell bundle",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
        )
        parser.add_argument(
            "--bundle",
            type=str,
            required=True,
            help="Bundle name",
        )
        parser.add_argument(
            "--tablet-balancer-schedule-hours",
            default=None,
            type=int,
            help="Tablet balancer schedule hours from range [0, 24); inferred from YT proxy by default",
        )
        parser.set_defaults(func=self.configure_db_bundle)
        return parser

    def add_ban_masters_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "ban-masters",
            help="Ban masters",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--fqdn",
            metavar="fqdn",
            type=str,
            action="append",
            help="Masters to ban",
        )
        parser.add_argument(
            "--ignore-banned",
            default=False,
            action="store_true",
            help="Do not throw if some of the masters are already banned",
        )
        parser.set_defaults(func=self.ban_masters)
        return parser

    def add_unban_masters_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "unban-masters",
            help="Unban masters",
            parents=[parent_parser],
        )
        parser.add_argument(
            "--fqdn",
            metavar="fqdn",
            type=str,
            action="append",
            help="Masters to unban",
        )
        parser.add_argument(
            "--ignore-unbanned",
            default=False,
            action="store_true",
            help="Do not throw if some of the masters are already unbanned",
        )
        parser.set_defaults(func=self.unban_masters)
        return parser

    def add_db_sync_parser(self, subparsers, parent_parser):
        parser = subparsers.add_parser(
            "db-sync",
            help="Run one of yt_sync scenarios to synchronize database",
            parents=[parent_parser],
        )
        db_sync.register_command(parser)
        parser.set_defaults(func=self.db_sync)

        return parser

    ############################################################################

    def main(self):
        parser = ArgumentParser(
            description="Tool for {} administration".format(
                self._data_model_traits.get_human_readable_name(),
            )
        )
        parser.register("action", "parsers", AliasedSubParsersAction)
        parser.add_argument(
            "--svn-version",
            action=SvnVersionAction,
            help="Show program svn version and exit",
        )

        parent_parser = ArgumentParser(add_help=False)
        parent_parser.add_argument("--yt-proxy", required=True)
        parent_parser.add_argument(
            self.get_default_path_arg_name(),
            dest="orm_path",
            default=self.get_default_path()
        )

        if self._data_model_traits.supports_replication():
            parent_parser.add_argument(
                "--replica",
                action=ReplicaArgument,
                help="Database replica {}".format(ReplicaArgument.format_usage()),
            )

        subparsers = parser.add_subparsers(metavar="command")
        subparsers.required = True

        self.add_init_db_parser(subparsers, parent_parser)
        self.add_dry_run_migration_parser(subparsers, parent_parser)
        self.add_start_readonly_migration_parser(subparsers, parent_parser)
        self.add_finish_readonly_migration_parser(subparsers, parent_parser)
        self.add_migrate_without_downtime(subparsers, parent_parser)
        self.add_db_version_parser(subparsers, parent_parser)
        self.add_dump_db_parser(subparsers, parent_parser)
        self.add_clone_db_parser(subparsers)
        self.add_backup_parser(subparsers, parent_parser)
        self.add_restore_parser(subparsers, parent_parser)
        self.add_unmount_db_parser(subparsers, parent_parser)
        self.add_mount_db_parser(subparsers, parent_parser)
        self.add_validate_parser(subparsers, parent_parser)
        self.add_diff_parser(subparsers, parent_parser)
        self.add_freeze_db_parser(subparsers, parent_parser)
        self.add_unfreeze_db_parser(subparsers, parent_parser)
        self.add_trim_table_parser(subparsers, parent_parser)
        self.add_cleanup_history_parser(subparsers, parent_parser)
        self.add_update_finalization_timestamp_parser(subparsers, parent_parser)
        self.add_reshard_tables_parser(subparsers, parent_parser)
        self.add_configure_bundle_parser(subparsers, parent_parser)
        self.add_ban_masters_parser(subparsers, parent_parser)
        self.add_unban_masters_parser(subparsers, parent_parser)
        self.add_db_sync_parser(subparsers, parent_parser)
        self.add_custom_parsers(subparsers, parent_parser)

        args = parser.parse_args()
        func_args = dict(vars(args))
        func_args.pop("svn_version")
        func_args.pop("func")
        args.func(**func_args)
