import yt.wrapper as yt
import yt.logger as logger

from yt.orm.library.common import wait

############################################################################


def _is_ordered_table(client, table_path):
    schema = client.get_attribute(table_path, "schema")
    return all(map(lambda column: "sort_order" not in column, schema))


def _is_shardable_table(client, table_path):
    assert client.exists(table_path), "Table {} does not exist".format(table_path)
    assert "table" == client.get_attribute(table_path, "type"), "Node {} is not a table".format(
        table_path
    )
    assert client.get_attribute(table_path, "dynamic"), "Table {} is not a dynamic table".format(
        table_path
    )
    return not _is_ordered_table(client, table_path)


def _set(client, path, value, commit):
    logger.info("yt set {} {}".format(path, value))
    if commit:
        client.set(path, value)


def _set_auto_reshard(client, table_path, value, commit):
    assert value in (True, False)
    path = yt.ypath_join(table_path, "@tablet_balancer_config/enable_auto_reshard")
    _set(client, path, value, commit)


def _set_forced_compaction_attribute(client, table_path, commit):
    path = yt.ypath_join(table_path, "@forced_compaction_revision")
    _set(client, path, 1, commit)


def _unmount_table(client, table_path, commit, sync):
    logger.info("yt unmount-table {}".format(table_path))
    if commit:
        client.unmount_table(table_path, sync=sync)


def _reshard_table_with_slicing(client, table_path, tablet_count, slicing_accuracy, commit):
    logger.info(
        "yt reshard-table {} tablet_count {} enable_slicing".format(table_path, tablet_count)
    )
    if commit:
        client.reshard_table(
            table_path,
            tablet_count=tablet_count,
            enable_slicing=True,
            slicing_accuracy=slicing_accuracy,
            sync=False,
        )


def _mount_table(client, table_path, commit, sync):
    logger.info("yt mount-table {}".format(table_path))
    if commit:
        client.mount_table(table_path, sync=sync)


############################################################################


def reshard_tables(
    yt_proxy,
    orm_path,
    max_tablets_per_table=None,
    min_bytes_per_tablet=None,
    slicing_accuracy=None,
    only_tables=None,
    dry_run=None,
    commit=None,
):
    if bool(dry_run) == bool(commit):
        logger.warning("Either dry_run or commit must be specified")
        raise Exception()

    client = yt.YtClient(yt_proxy)

    DEFAULT_TABLETS_PER_NODE = 5
    DEFAULT_MIN_BYTES_PER_TABLET = 50000000
    if max_tablets_per_table is None:
        max_tablets_per_table = DEFAULT_TABLETS_PER_NODE * len(client.list("//sys/cluster_nodes"))
    if min_bytes_per_tablet is None:
        min_bytes_per_tablet = DEFAULT_MIN_BYTES_PER_TABLET
    assert max_tablets_per_table > 0, "max_tablets_per_table must be positive"
    assert min_bytes_per_tablet > 0, "min_bytes_per_tablet must be positive"

    db_path = yt.ypath_join(orm_path, "db")

    if only_tables:
        table_paths = [yt.ypath_join(db_path, table) for table in only_tables]
    else:
        table_paths = [table_path for table_path in client.list(db_path, absolute=True)]

    table_paths = [
        table_path for table_path in table_paths if _is_shardable_table(client, table_path)
    ]

    logger.info(
        "Resharding {} tables at {} max tablets per table and {} min bytes per tablet".format(
            len(table_paths), max_tablets_per_table, min_bytes_per_tablet
        )
    )

    for table_path in table_paths:
        _unmount_table(client, table_path, commit, sync=False)

    def check_unmounted():
        logger.info("waiting for unmount")
        return all(
            [client.get(table_path + "/@tablet_state") == "unmounted" for table_path in table_paths]
        )

    if commit:
        wait(check_unmounted, iter=200, sleep_backoff=2)

    for table_path in table_paths:
        data_weight = client.get_attribute(table_path, "data_weight")
        tablet_count = min(max_tablets_per_table, data_weight // min_bytes_per_tablet)
        tablet_count = max(tablet_count, 1)
        _reshard_table_with_slicing(client, table_path, tablet_count, slicing_accuracy, commit)
        _set_auto_reshard(client, table_path, False, commit)
        _set_forced_compaction_attribute(client, table_path, commit)

    def check_resharded():
        logger.info("waiting for reshard")
        return all(
            [client.get(table_path + "/@tablet_state") != "transient" for table_path in table_paths]
        )

    if commit:
        wait(check_resharded, iter=200, sleep_backoff=2)

    for table_path in table_paths:
        _mount_table(client, table_path, commit, False)

    def check_mounted():
        logger.info("waiting for mount")
        return all(
            [client.get(table_path + "/@tablet_state") == "mounted" for table_path in table_paths]
        )

    if commit:
        wait(check_mounted, iter=200, sleep_backoff=2)
