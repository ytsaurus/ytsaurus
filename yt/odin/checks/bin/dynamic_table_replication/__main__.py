from yt_odin_checks.lib.check_runner import main

from yt.test_helpers import wait, WaitFailed

from yt.wrapper.client import Yt

from yt.common import datetime_to_string, date_string_to_timestamp, YtError

from datetime import datetime, timedelta
import calendar
import random


COLLOCATION_SIZE = 2

REPLICATED_TABLE_PATH_TEMPLATE = "//sys/admin/odin/dynamic_table_replication/replicated_table_{epoch}_{{index}}"
TABLE_PATH_TEMPLATE = "//sys/admin/odin/dynamic_table_replication/table_{epoch}_{{index}}"

TABLE_SCHEMA = [
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type": "string"},
]

TABLE_EXPIRATION_TIME_DELTA_MINUTES = 60

BANNED_REPLICATION_TIMESTAMP_PATH = "//sys/admin/odin/dynamic_table_replication/@replication_banned_at"
BANNED_REPLICATION_GRACE_PERIOD = 3600 * 3  # 3h


class MetaClusterError(Exception):
    def __init__(self, inner):
        self.inner = inner


class ManuallyDisabledReplicationError(Exception):
    pass


def run_check(yt_client, logger, options, states):
    class YtMetaClusterClientWrapper(object):
        def __init__(self, _yt_client):
            self._yt_client = _yt_client

        def __getattr__(self, name):
            def _yt(method, *args, **kwargs):
                try:
                    joined_args = ', '.join(map(lambda a: "'%s'" % a, args))
                    if kwargs:
                        logger.info('Request to metacluster: yt.{}({}, **{})'.format(method, joined_args, kwargs))
                        return getattr(self._yt_client, method)(*args, **kwargs)
                    else:
                        logger.info('Request to metacluster: yt.{}({})'.format(method, joined_args))
                        return getattr(self._yt_client, method)(*args)
                except Exception as e:
                    logger.warning("Exception upon access to metacluster: {}".format(e))
                    raise MetaClusterError(e)

            return _yt.__get__(name)

    def wait_for_result(func, message, raise_metacluster_error=False):
        try:
            wait(func, error_message="Failed waiting for {}".format(message), sleep_backoff=1, timeout=30)
        except WaitFailed as e:
            logger.error(e)
            if raise_metacluster_error:
                raise MetaClusterError(e)
            raise WaitFailed

    def for_each_table(func):
        for index in range(COLLOCATION_SIZE):
            func(index)

    def is_meta_cluster():
        return options["cluster_name"] == options["metacluster"]

    def is_replica_cluster():
        return options["cluster_name"] in options["replica_clusters"]

    def create_collocation():
        assert is_meta_cluster()

        logger.info("Creating table collocation")
        yt_client.create("table_collocation", attributes={
            "collocation_type": "replication",
            "table_paths": [replicated_table_path_templ.format(index=index) for index in range(COLLOCATION_SIZE)],
        })

    def maybe_create_replicated_table(index, min_sync_replica_count, max_sync_replica_count):
        assert is_meta_cluster()

        created = False
        replicated_table_options = None
        for index in range(COLLOCATION_SIZE):
            replicated_table_path = replicated_table_path_templ.format(index=index)
            if not yt_client.exists(replicated_table_path):
                created = True
                logger.info("Creating replicated table {}".format(replicated_table_path))
                yt_client.create("replicated_table", replicated_table_path, attributes={
                    "dynamic": True,
                    "account": "sys",
                    "tablet_cell_bundle": "sys",
                    "schema": TABLE_SCHEMA,
                    "replicated_table_options": {
                        "enable_replicated_table_tracker": True,
                        "min_sync_replica_count": min_sync_replica_count,
                        "max_sync_replica_count": max_sync_replica_count,
                    },
                    "expiration_time": "{}".format(datetime.utcnow() +
                                                   timedelta(minutes=TABLE_EXPIRATION_TIME_DELTA_MINUTES)),
                })
            elif options is None:
                assert not created
                replicated_table_options = yt_client.get("{}/@replicated_table_options".format(replicated_table_path))
                min_sync_replica_count = replicated_table_options["min_sync_replica_count"]
                max_sync_replica_count = replicated_table_options["max_sync_replica_count"]

            replicas = yt_client.get("{}/@replicas".format(replicated_table_path))
            replica_clusters = [replicas[replica]["cluster_name"] for replica in replicas]

            replica_table_path = replica_table_path_templ.format(index=index)

            for cluster in options["replica_clusters"]:
                if cluster in replica_clusters:
                    continue

                logger.info("Creating table replica for table {} for cluster {}".format(replicated_table_path, cluster))
                yt_client.create("table_replica", attributes={
                    "table_path": replicated_table_path,
                    "cluster_name": cluster,
                    "replica_path": replica_table_path,
                })

            if yt_client.get("{}/@tablet_state".format(replicated_table_path)) != "mounted":
                yt_client.mount_table(replicated_table_path, sync=True)

        if created:
            create_collocation()

    def maybe_create_replica_table(index):
        assert is_replica_cluster()

        replicated_table_path = replicated_table_path_templ.format(index=index)
        replica_table_path = replica_table_path_templ.format(index=index)

        metacluster_client = YtMetaClusterClientWrapper(Yt(proxy=options["metacluster"], token=yt_client.config["token"]))

        # This also ensures that table replica is created.
        if metacluster_client.get("{}/@tablet_state".format(replicated_table_path)) != "mounted":
            logger.warning("Replicated table is not ready")
            raise MetaClusterError(Exception("Replicated table is not ready"))

        replicas = metacluster_client.get("{}/@replicas".format(replicated_table_path))
        replica_ids = list(filter(lambda replica: replicas[replica]["cluster_name"] == options["cluster_name"], replicas))
        assert len(replica_ids) == 1
        replica_id = replica_ids[0]

        if not yt_client.exists(replica_table_path):
            logger.info("Creating replica table {}".format(replica_table_path))
            yt_client.create("table", replica_table_path, attributes={
                "account": "sys",
                "tablet_cell_bundle": "sys",
                "schema": TABLE_SCHEMA,
                "dynamic": True,
                "upstream_replica_id": replica_id,
                "expiration_time": "{}".format(datetime.utcnow() +
                                               timedelta(minutes=TABLE_EXPIRATION_TIME_DELTA_MINUTES)),
            })
        if yt_client.get("{}/@tablet_state".format(replica_table_path)) != "mounted":
            yt_client.mount_table(replica_table_path, sync=True)

        if metacluster_client.get("#{}/@state".format(replica_id)) != "enabled":
            metacluster_client.alter_table_replica(replica_id, enabled=True)

            def _check():
                return metacluster_client.get("#{}/@state".format(replica_id)) == "enabled"
            wait_for_result(_check, "replicas to become enabled", raise_metacluster_error=True)

    def maybe_create_table(index):
        if is_meta_cluster():
            min_sync_replica_count = 1 if is_replica_cluster() else 0
            max_sync_replica_count = random.randint(min_sync_replica_count, 3)
            maybe_create_replicated_table(index, min_sync_replica_count, max_sync_replica_count)
        if is_replica_cluster():
            maybe_create_replica_table(index)

    def mix_replica_modes_up(index):
        assert is_meta_cluster()

        replicated_table_path = replicated_table_path_templ.format(index=index)
        replicas = yt_client.get("{}/@replicas".format(replicated_table_path))

        for replica in replicas.items():
            if replica[1]["state"] != "enabled":
                continue
            mode = random.choice(("sync", "async"))
            yt_client.alter_table_replica(replica[0], mode=mode)
            logger.info("Altering table {} replica {} mode: {} -> {}".format(replicated_table_path,
                                                                             replica[0],
                                                                             replica[1]["mode"],
                                                                             mode))

            def _check():
                return yt_client.get("#{}/@mode".format(replica[0])) == mode
            wait_for_result(_check, "replica {} to update mode".format(replica[0]))

    def check_collocated_tables_synchronized():
        assert is_meta_cluster()

        replicated_table_path = replicated_table_path_templ.format(index=0)
        min_sync_replica_count = yt_client.get("{}/@replicated_table_options/min_sync_replica_count".format(
            replicated_table_path))

        def _check():
            sync_replica_clusters_by_table = []
            for index in range(COLLOCATION_SIZE):
                replicas = yt_client.get("{}/@replicas".format(replicated_table_path_templ.format(index=index)))
                sync_replica_ids = list(filter(lambda replica:
                                               replicas[replica]["mode"] == "sync" and replicas[replica]["state"] == "enabled",
                                               replicas))
                sync_replica_clusters = [replicas[replica_id]["cluster_name"] for replica_id in sync_replica_ids]
                sync_replica_clusters_by_table.append(sync_replica_clusters)
                if len(sync_replica_clusters) < min_sync_replica_count:
                    return False

            for index in range(COLLOCATION_SIZE - 1):
                if set(sync_replica_clusters_by_table[index]) != set(sync_replica_clusters_by_table[index + 1]):
                    return False

            return True

        wait_for_result(_check, "collocated tables to get synchronized")

    def check_replication(index):
        assert is_replica_cluster()

        replicated_table_path = replicated_table_path_templ.format(index=index)
        replica_table_path = replica_table_path_templ.format(index=index)

        metacluster_client = YtMetaClusterClientWrapper(Yt(proxy=options["metacluster"], token=yt_client.config["token"]))
        replicas = metacluster_client.get("{}/@replicas".format(replicated_table_path))

        row = {}
        dt = datetime.utcnow()
        row["key"] = int(calendar.timegm(dt.timetuple()) * 1000000 + dt.microsecond)
        row["value"] = str(42)
        insert_options = {"raw": False}
        if not any(replicas[replica_id]["mode"] == "sync" for replica_id in replicas.keys()):
            insert_options["require_sync_replica"] = False

        logger.info("Writing a row into replicated table {}".format(replicated_table_path))

        def _write():
            try:
                metacluster_client.insert_rows(replicated_table_path, [row], **insert_options)
            except MetaClusterError as e:
                if isinstance(e.inner, YtError):
                    if e.inner.contains_code(1714):  # NoSyncReplicas
                        logger.warning("Encountered NoSyncReplicas error; will retry")
                        return False
                    elif e.inner.contains_code(1724):  # SyncReplicaIsNotWritten
                        logger.warning("Encountered SyncReplicaIsNotWritten error; will retry")
                        return False
                    elif e.inner.contains_code(1723):  # SyncReplicaIsNotInSyncMode
                        logger.warning("Encountered SyncReplicaIsNotInSyncMode error; will retry")
                        return False
                    elif e.inner.contains_code(1732):  # SyncReplicaNotInSync
                        logger.warning("Encountered SyncReplicaNotInSync error; will retry")
                        return False
                raise e
            return True
        wait_for_result(_write, "rows to get written", raise_metacluster_error=True)

        keys = [{"key": row["key"]}]
        rows = [row]

        def _check():
            return list(yt_client.lookup_rows(replica_table_path, keys)) == rows
        wait_for_result(_check, "rows to get replicated")

        def _lookup():
            try:
                assert list(metacluster_client.lookup_rows(replicated_table_path, keys)) == rows
                return True
            except MetaClusterError as e:
                if isinstance(e.inner, YtError):
                    if e.inner.contains_code(1736):  # NoInSyncReplicas
                        logger.warning("Encountered NoInSyncReplicas error; will retry")
                        return False
                raise e
        wait_for_result(_lookup, "lookup from metacluster")

    def check_incoming_replication_enabled():
        assert is_replica_cluster()

        if not yt_client.get("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/enable_incoming_replication"):
            logger.warning("Incoming replication is disabled")
            raise ManuallyDisabledReplicationError

        banned_replica_clusters = yt_client.get("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters")
        if options["cluster_name"] in banned_replica_clusters:
            logger.warning("Replication is self-banned")
            raise ManuallyDisabledReplicationError

    def check_replication_not_banned():
        assert is_meta_cluster()

        has_banned_at_mark = yt_client.exists(BANNED_REPLICATION_TIMESTAMP_PATH)

        banned_replica_clusters = yt_client.get("//sys/@config/tablet_manager/replicated_table_tracker/replicator_hint/banned_replica_clusters")
        if len(banned_replica_clusters) == 0:
            if has_banned_at_mark:
                logger.info("Removing banned replication timestamp attribute {}".format(BANNED_REPLICATION_TIMESTAMP_PATH))
                yt_client.remove(BANNED_REPLICATION_TIMESTAMP_PATH)
            return

        if has_banned_at_mark:
            banned_at = yt_client.get(BANNED_REPLICATION_TIMESTAMP_PATH)
            banned_at_unix = date_string_to_timestamp(banned_at)
            banned_at_utc = datetime.utcfromtimestamp(banned_at_unix)
            timeout = banned_at_utc + timedelta(seconds=BANNED_REPLICATION_GRACE_PERIOD)
            logger.warning("Replication is banned already for {}".format(datetime.utcnow() - banned_at_utc))
            if datetime.utcnow() >= timeout:
                raise ManuallyDisabledReplicationError
        else:
            banned_at = datetime_to_string(datetime.utcnow())
            logger.info("Setting banned replication timestamp attribute {} to {}".format(
                BANNED_REPLICATION_TIMESTAMP_PATH,
                banned_at))
            yt_client.set(BANNED_REPLICATION_TIMESTAMP_PATH, banned_at)

    now = datetime.now()
    # NB: This way we can prevent race between replicated table and replica table creation.
    if is_meta_cluster():
        current_epoch = (now.minute + 60 * now.hour) // 10
    else:
        current_epoch = (now.minute + 5 + 60 * now.hour) // 10 - 1

    replicated_table_path_templ = REPLICATED_TABLE_PATH_TEMPLATE.format(epoch=current_epoch)
    replica_table_path_templ = TABLE_PATH_TEMPLATE.format(epoch=current_epoch)

    try:
        if is_meta_cluster():
            check_replication_not_banned()
        if is_replica_cluster():
            check_incoming_replication_enabled()

        for_each_table(maybe_create_table)

        if is_meta_cluster():
            for_each_table(mix_replica_modes_up)
            check_collocated_tables_synchronized()

        if is_replica_cluster():
            for_each_table(check_replication)

        exit_code = 0
    except MetaClusterError:
        exit_code = 1
        error_message = "Encountered an error upon access to metacluster"
    except WaitFailed:
        exit_code = 2
        error_message = "Wait on inner checks took too long"
    except ManuallyDisabledReplicationError:
        exit_code = 2
        error_message = "Replication is manually disabled"

    if exit_code == 0:
        return states.FULLY_AVAILABLE_STATE, "OK"
    elif exit_code == 1:
        return states.PARTIALLY_AVAILABLE_STATE, error_message
    elif exit_code == 2:
        return states.UNAVAILABLE_STATE, error_message
    else:
        raise RuntimeError("Unexpected exit code '{}' found".format(exit_code))


if __name__ == "__main__":
    main(run_check)
