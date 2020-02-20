from __future__ import print_function

import yt.wrapper as yt

import argparse


def get_attribute_path(path, attribute_name):
    return yt.ypath_join(path, "@{}".format(attribute_name))


def get_schema(client, table_path):
    return client.get(get_attribute_path(table_path, "schema"))


def is_ordered_dynamic_table(client, table_path):
    schema = get_schema(client, table_path)
    return all(map(lambda column: "sort_order" not in column, schema))


def is_sorted_dynamic_table(client, table_path):
    return not is_ordered_dynamic_table(client, table_path)


def set_auto_reshard(client, table_path, value, dry_run):
    assert value in (True, False)
    path = get_attribute_path(table_path, "tablet_balancer_config/enable_auto_reshard")
    print("yt set {} {}".format(path, value))
    if not dry_run:
        client.set(path, value)


def pick_pivots_and_reshard(client, table_path, tablet_count, dry_run):
    assert tablet_count >= 1

    assert is_sorted_dynamic_table(client, table_path)

    schema = get_schema(client, table_path)
    key_columns = [col["name"] for col in schema if col.get("sort_order") == "ascending"]

    tmp_table_path = client.create_temp_table()
    print("Sampling keys from {} to {}".format(table_path, tmp_table_path))
    client.run_merge(
        yt.TablePath(table_path, columns=key_columns), tmp_table_path, mode="ordered",
    )

    row_count = client.get(get_attribute_path(tmp_table_path, "row_count"))
    sampling_rate = min(tablet_count * 100.0 / row_count, 1.0)

    rows = [[]] + list(
        client.read_table(tmp_table_path, table_reader=dict(sampling_rate=sampling_rate),),
    )

    if tablet_count > len(rows):
        print("Decreasing tablet count from {} to {}".format(tablet_count, len(rows)))
        tablet_count = len(rows)

    def flatten(key):
        return [key[col] for col in key_columns]

    def get_pivot_index(i):
        return i * len(rows) / tablet_count

    pivots = [[]]
    for i in range(1, tablet_count):
        previous_index = get_pivot_index(i - 1)
        index = get_pivot_index(i)
        assert index > previous_index, "{} > {}".format(index, previous_index)
        pivots.append(flatten(rows[index]))

    print()
    print("yt unmount-table {}".format(table_path))
    print("yt reshard-table {} {}".format(table_path, pivots))
    print("yt mount-table {}".format(table_path))
    print()
    if not dry_run:
        client.unmount_table(table_path, sync=True)
        client.reshard_table(table_path, pivots, sync=True)
        client.mount_table(table_path, sync=True)


def setup_auto_reshard_attributes(client, table_path, dry_run):
    assert is_sorted_dynamic_table(client, table_path)

    attributes = dict(
        desired_partition_data_size=2 * (10 ** 7),
        max_partition_data_size=4 * (10 ** 7),
        min_partition_data_size=1 * (10 ** 7),
        min_partitioning_data_size=1 * (10 ** 7),
        desired_tablet_count=5,
    )

    for attribute_name, attribute_value in attributes.iteritems():
        attribute_path = get_attribute_path(table_path, attribute_name)
        print("yt set {} {}".format(attribute_path, attribute_value))
        if not dry_run:
            client.set(attribute_path, attribute_value)

    print("yt remount-table {}".format(table_path))
    if not dry_run:
        client.remount_table(table_path)


def list_tablet_cell_bundles(client, table_paths):
    tablet_cell_bundles = set()
    for table_path in table_paths:
        tablet_cell_bundles.add(client.get(get_attribute_path(table_path, "tablet_cell_bundle")),)
    return tablet_cell_bundles


def get_db_path(yp_path):
    return yt.ypath_join(yp_path, "db")


def get_db_table_path(yp_path, db_table):
    return yt.ypath_join(get_db_path(yp_path), db_table)


def get_db_table_paths(yp_path, db_tables):
    return map(lambda db_table: get_db_table_path(yp_path, db_table), db_tables)


def main(
    cluster, yp_path, reshard_small_tables, reshard_big_tables, configure_bundles, dry_run,
):
    client = yt.YtClient(cluster)

    db_tables = set(client.list(get_db_path(yp_path)))

    sorted_db_tables = filter(
        lambda db_table: is_sorted_dynamic_table(client, get_db_table_path(yp_path, db_table),),
        db_tables,
    )
    ordered_db_tables = filter(
        lambda db_table: is_ordered_dynamic_table(client, get_db_table_path(yp_path, db_table),),
        db_tables,
    )

    print("Skipping ordered DB tables:\n  {}\n".format("\n  ".join(ordered_db_tables)))

    db_tables = sorted_db_tables

    big_db_tables = set(["dns_record_sets", "endpoints", "nodes", "parents", "pods", "resources"])
    assert all(map(lambda db_table: db_table in db_tables, big_db_tables))
    print("Big DB tables:\n  {}\n".format("\n  ".join(big_db_tables)))

    small_db_tables = filter(lambda db_table: db_table not in big_db_tables, db_tables)
    print("Small DB tables:\n  {}\n".format("\n  ".join(small_db_tables)))

    if reshard_small_tables:
        print("Resharding small tables")

        for db_table in small_db_tables:
            db_table_path = get_db_table_path(yp_path, db_table)
            setup_auto_reshard_attributes(
                client, db_table_path, dry_run,
            )
            set_auto_reshard(client, db_table_path, True, dry_run)

    if reshard_big_tables:
        print("Resharding big tables")

        for db_table in big_db_tables:
            db_table_path = get_db_table_path(yp_path, db_table)
            pick_pivots_and_reshard(client, db_table_path, tablet_count=20, dry_run=dry_run)
            set_auto_reshard(client, db_table_path, False, dry_run)

    if configure_bundles:
        print("Configuring bundles")

        tablet_cell_bundles = list_tablet_cell_bundles(
            client, get_db_table_paths(yp_path, db_tables),
        )

        print("Disabling in memory cell balancer")
        for tablet_cell_bundle in tablet_cell_bundles:
            attribute_path = get_attribute_path(
                "//sys/tablet_cell_bundles/{}".format(tablet_cell_bundle),
                "tablet_balancer_config/enable_in_memory_cell_balancer",
            )
            print("yt set {} %false".format(attribute_path))
            if not dry_run:
                client.set(attribute_path, False)

        def create_tablet_balancer_schedule(hours):
            return "hours == {} && minutes == 0".format(hours)

        tablet_balancer_schedule_per_cluster = {
            "yp-sas-test": create_tablet_balancer_schedule(8),
            "yp-man-pre": create_tablet_balancer_schedule(8),
            "yp-sas": create_tablet_balancer_schedule(9),
            "yp-man": create_tablet_balancer_schedule(10),
            "yp-vla": create_tablet_balancer_schedule(11),
            "yp-xdc": create_tablet_balancer_schedule(12),
            "yp-iva": create_tablet_balancer_schedule(13),
            "yp-myt": create_tablet_balancer_schedule(14),
        }
        assert cluster in tablet_balancer_schedule_per_cluster

        print("Setting up tablet balancer schedule")
        for tablet_cell_bundle in tablet_cell_bundles:
            attribute_path = get_attribute_path(
                "//sys/tablet_cell_bundles/{}".format(tablet_cell_bundle),
                "tablet_balancer_config/tablet_balancer_schedule",
            )
            schedule = tablet_balancer_schedule_per_cluster[cluster]
            print('yt set {} "{}"'.format(attribute_path, schedule))
            if not dry_run:
                client.set(attribute_path, schedule)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Reshards tables for YPADMIN-197")
    parser.add_argument(
        "--cluster", type=str, required=True, help="YT cluster address",
    )
    parser.add_argument(
        "--yp-path", type=str, required=True, help="Path to yp Cypress node",
    )
    parser.add_argument(
        "--reshard-small-tables", action="store_true",
    )
    parser.add_argument(
        "--reshard-big-tables", action="store_true",
    )
    parser.add_argument(
        "--configure-bundles", action="store_true",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
    )
    arguments = parser.parse_args()
    return arguments


if __name__ == "__main__":
    arguments = parse_arguments()
    main(
        arguments.cluster,
        arguments.yp_path,
        arguments.reshard_small_tables,
        arguments.reshard_big_tables,
        arguments.configure_bundles,
        arguments.dry_run,
    )
