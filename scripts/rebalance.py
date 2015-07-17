#!/usr/bin/python

"""
This script rebalances given table, splitting large tablets if necessary.
"""

import argparse
import time
import logging
import json

import yt.wrapper as yt
import yt.yson as yson

yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT = "yson"

GB = 1024 * 1024 * 1024


def get_tablet_info(tablet_id):
    tablet_cell_id = yt.get("//sys/tablets/%s/@cell_id" % tablet_id)
    peers = yt.get("//sys/tablet_cells/%s/@peers" % tablet_cell_id)
    leading_peer = None
    for peer in peers:
        if peer["state"] == "leading":
            leading_peer = peer
            break
    assert leading_peer["address"]
    # Due to 0.16-0.17 incompatibilities in YSON we load information in JSON and deserialize it manually.
    info = yt.get("//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s" % (
        leading_peer["address"],
        tablet_cell_id,
        tablet_id), format=yt.JsonFormat())
    info = json.loads(info)
    return info


def combine_partitions(partitions, number_of_key_columns):
    assert number_of_key_columns >= 1

    def _impl():
        previous_index = None
        previous_pivot_key = None
        previous_uncompressed_data_size = None
        for index, pivot_key, uncompressed_data_size in partitions:
            pivot_key = pivot_key[0:number_of_key_columns]
            if previous_pivot_key != pivot_key:
                if previous_pivot_key is not None:
                    yield (previous_index, previous_pivot_key, previous_uncompressed_data_size)
                previous_index = index
                previous_pivot_key = pivot_key
                previous_uncompressed_data_size = uncompressed_data_size
            else:
                previous_uncompressed_data_size += uncompressed_data_size
        if previous_pivot_key is not None:
            yield (previous_index, previous_pivot_key, previous_uncompressed_data_size)
    return list(_impl())


def split_partitions(partitions, desired_tablet_size):
    assert desired_tablet_size >= 2

    def _impl():
        accumulated = 0
        for index, pivot_key, uncompressed_data_size in partitions:
            if index > 0 and accumulated + uncompressed_data_size > desired_tablet_size:
                yield pivot_key
                accumulated = 0
            else:
                accumulated += uncompressed_data_size
    return list(_impl())


def suggest_pivot_keys(tablet_info, number_of_key_columns, desired_tablet_size):
    # Load partitions.
    distribution = map(
        lambda t: (t[0], t[1]["pivot_key"], t[1]["uncompressed_data_size"]),
        enumerate(tablet_info["partitions"]))
    # Adjust desired table size for smoother distribution.
    total_tablet_size = sum(uncompressed_data_size for _, _, uncompressed_data_size in distribution)
    desired_tablet_size = total_tablet_size / int(1 + total_tablet_size / desired_tablet_size)
    # Perform sanity checks.
    if tablet_info["eden"]["uncompressed_data_size"] > 1 * GB:
        logging.warning("Eden is extermely large; likely data is not yet partitioned")
        return None
    if sum(map(lambda p: p[2], distribution)) < desired_tablet_size:
        logging.warning("Scarce partition information prevents from choosing good pivot keys")
        return None
    # Combine partitions with seemingly same pivot keys.
    distribution = combine_partitions(distribution, number_of_key_columns)
    # Split partitions according to desired size.
    distribution = split_partitions(distribution, desired_tablet_size)
    return [tablet_info["pivot_key"]] + distribution


def rebalance(table, number_of_key_columns, desired_tablet_size, yes=False):
    if not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
        logging.error("Cannot extract partition information from unmounted table; please, mount table aforehead")
        logging.error("Aborting")
        return

    tablets = map(
        lambda t: (t[0], t[1]["tablet_id"], t[1]["statistics"]["uncompressed_data_size"]),
        enumerate(yt.get(table + "/@tablets")))

    unbalanced_tablet_indexes = []
    for tablet_index, _, tablet_size in tablets:
        if tablet_size > desired_tablet_size:
            unbalanced_tablet_indexes.append(tablet_index)

    logging.info("There are %s / %s unbalanced tablets in %s",
                 len(unbalanced_tablet_indexes), len(tablets), table)

    # Iterate in reverse order to produce correct sequence of operations.
    operations = []
    for unbalanced_tablet_index in reversed(unbalanced_tablet_indexes):
        tablet_id = tablets[unbalanced_tablet_index][1]
        tablet_info = get_tablet_info(tablet_id)
        pivot_keys = suggest_pivot_keys(tablet_info, number_of_key_columns, desired_tablet_size)
        if pivot_keys is None:
            logging.error(
                "Tablet %s (%s) could not be split yet",
                unbalanced_tablet_index, tablet_id)
        elif len(pivot_keys) <= 1:
            logging.error(
                "Cannot split tablet %s (%s) due to data skew; consider increasing `--key-columns`",
                unbalanced_tablet_index, tablet_id)
        else:
            logging.info(
                "Computed %s new pivot keys for tablet %s: %s",
                len(pivot_keys), unbalanced_tablet_index, pivot_keys)
            operations.append((unbalanced_tablet_index, pivot_keys))

    if len(operations) == 0:
        logging.info("No operations to perform; exiting")
        return

    if not yes:
        fmt = lambda x: "'" + yson.dumps(x)[1:-1] + "'"
        for index, keys in operations:
            print "yt reshard_table --first '%s' --last '%s' '%s' %s" % (
                index, index, table, " ".join(map(fmt, keys)))
        logging.info("`--yes` was not specified; exiting")
        return
    else:
        logging.info("You have 5 seconds to press Ctrl+C to abort...")
        time.sleep(5)

    logging.info("Unmounting %s", table)
    yt.unmount_table(table)
    while not all(x["state"] == "unmounted" for x in yt.get(table + "/@tablets")):
        logging.info("Waiting for tablets to become unmounted...")
        time.sleep(1)

    logging.info("Resharding %s", table)
    for index, keys in operations:
        logging.info("Resharding tablet %s in table %s", index, table)
        yt.reshard_table(table, keys, first_tablet_index=index, last_tablet_index=index)

    logging.info("Mounting %s", table)
    yt.mount_table(table)
    while not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
        logging.info("Waiting for tablets to become mounted...")
        time.sleep(1)

    logging.info("Done")


def test():
    import unittest

    class MyTestCase(unittest.TestCase):
        def test_combine_partitions_0(self):
            self.assertItemsEqual(combine_partitions([], 1), [])

        def test_combine_partitions_1(self):
            actual = combine_partitions([
                (0, [1, 1], 100),
                (1, [2, 1], 100),
            ], 1)
            expected = [
                (0, [1], 100),
                (1, [2], 100),
            ]
            self.assertItemsEqual(expected, actual)

        def test_combine_partitions_2(self):
            actual = combine_partitions([
                (0, [1, 1], 100),
                (1, [1, 2], 100),
            ], 1)
            expected = [
                (0, [1], 200),
            ]
            self.assertItemsEqual(expected, actual)

        def test_combine_partitions_3(self):
            actual = combine_partitions([
                (0, [1, 1], 100),
                (1, [1, 2], 100),
                (2, [2, 1], 100),
            ], 1)
            expected = [
                (0, [1], 200),
                (2, [2], 100),
            ]
            self.assertItemsEqual(expected, actual)

        def test_combine_partitions_4(self):
            actual = combine_partitions([
                (0, [1, 1], 100),
            ], 1)
            expected = [
                (0, [1], 100),
            ]
            self.assertItemsEqual(expected, actual)

        def test_combine_partitions_5(self):
            actual = combine_partitions([
                (0, [1, 1], 100),
                (1, [1, 2], 100),
                (2, [2, 1], 100),
                (2, [2, 2], 100),
            ], 1)
            expected = [
                (0, [1], 200),
                (2, [2], 200),
            ]
            self.assertItemsEqual(expected, actual)

        def test_split_partitions_0(self):
            actual = split_partitions([
                (0, [1], 100),
            ], 10)
            expected = []
            self.assertItemsEqual(expected, actual)

        def test_split_partitions_1(self):
            actual = split_partitions([
                (0, [1], 100),
            ], 1000)
            expected = []
            self.assertItemsEqual(expected, actual)

        def test_split_partitions_2(self):
            actual = split_partitions([
                (0, [1], 100),
                (1, [2], 100),
                (2, [3], 100),
            ], 50)
            expected = [[2], [3]]
            self.assertItemsEqual(expected, actual)

        def test_split_partitions_3(self):
            actual = split_partitions([
                (0, [1], 100),
                (1, [2], 100),
                (2, [3], 100),
            ], 150)
            expected = [[2]]
            self.assertItemsEqual(expected, actual)

    suite = unittest.TestLoader().loadTestsFromTestCase(MyTestCase)
    unittest.TextTestRunner(verbosity=2).run(suite)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", type=str, required=True, help="Table to rebalance")
    parser.add_argument("--key-columns", type=int, required=False, default=1,
                        help="How many key columns to take into account")
    parser.add_argument("--threshold-gbs", type=long, required=False, default=80,
                        help="Maximum tablet size (in GBs)")
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.add_argument("--yes", action="store_true", help="Actually do something (do nothing by default)")
    parser.add_argument("--test", action="store_true", help="Run unit tests")
    args = parser.parse_args()
    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    if args.test:
        test()
    else:
        rebalance(args.table, args.key_columns, args.threshold_gbs * GB, args.yes)
