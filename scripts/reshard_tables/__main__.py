from __future__ import print_function

import yt.wrapper as yt

import argparse


def get_db_path(yp_path):
    return yt.ypath_join(yp_path, "db")


def get_db_table_path(yp_path, db_table):
    return yt.ypath_join(get_db_path(yp_path), db_table)


def get_attribute_path(path, attribute_name):
    return yt.ypath_join(path, "@{}".format(attribute_name))


def main(cluster, yp_path, db_tables, dry_run):
    client = yt.YtClient(cluster)

    all_db_tables = set(client.list(get_db_path(yp_path)))
    for db_table in db_tables:
        assert db_table in all_db_tables, "Unknown DB table {}".format(db_table)

    attributes = dict(
        desired_partition_data_size=2 * (10 ** 7),
        max_partition_data_size=4 * (10 ** 7),
        min_partition_data_size=1 * (10 ** 7),
        min_partitioning_data_size=1 * (10 ** 7),
        desired_tablet_count=5,
    )

    for db_table in db_tables:
        db_table_path = get_db_table_path(yp_path, db_table)
        for attribute_name, attribute_value in attributes.iteritems():
            attribute_path = get_attribute_path(db_table_path, attribute_name)
            print("yt set {} {}".format(attribute_path, attribute_value))
            if not dry_run:
                client.set(attribute_path, attribute_value)
        print("yt remount-table {}".format(db_table_path))
        if not dry_run:
            client.remount_table(db_table_path)

    tablet_cell_bundles = set()
    for db_table in db_tables:
        db_table_path = get_db_table_path(yp_path, db_table)
        tablet_cell_bundles.add(client.get(get_attribute_path(db_table_path, "tablet_cell_bundle")))

    for tablet_cell_bundle in tablet_cell_bundles:
        attribute_path = "//sys/tablet_cell_bundles/{}/@tablet_balancer_config/enable_in_memory_cell_balancer".format(
            tablet_cell_bundle,
        )
        print("yt set {} %false".format(attribute_path))
        if not dry_run:
            client.set(attribute_path, False)


def parse_arguments():
    parser = argparse.ArgumentParser(description="Reshards tables for YPADMIN-197")
    parser.add_argument(
        "--cluster",
        type=str,
        required=True,
        help="YT cluster address",
    )
    parser.add_argument(
        "--yp-path",
        type=str,
        required=True,
        help="Path to yp Cypress node",
    )
    parser.add_argument(
        "--db-tables",
        nargs="+",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
    )
    arguments = parser.parse_args()
    assert arguments.db_tables, "Specify at least one DB table"
    assert len(arguments.db_tables) == len(set(arguments.db_tables)), "Duplicate DB tables"
    return arguments


if __name__ == "__main__":
    arguments = parse_arguments()
    main(arguments.cluster, arguments.yp_path, arguments.db_tables, arguments.dry_run)
