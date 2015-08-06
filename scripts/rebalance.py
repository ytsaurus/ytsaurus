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


def get_tablet_attributes(tablet_id):
    tablet_cell_id = yt.get("//sys/tablets/%s/@cell_id" % tablet_id)
    peers = yt.get("//sys/tablet_cells/%s/@peers" % tablet_cell_id)
    leading_peer = None
    for peer in peers:
        if peer["state"] == "leading":
            leading_peer = peer
            break
    assert leading_peer["address"]
    # Due to 0.16-0.17 incompatibilities in YSON we load information in JSON and deserialize it manually.
    attributes = yt.get("//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s" % (
        leading_peer["address"],
        tablet_cell_id,
        tablet_id), format=yt.JsonFormat())
    attributes = json.loads(attributes)
    return attributes


def get_tablet_spans_to_rebalance(tablets, desired_tablet_size):
    def _impl():
        first_index, accumulated_size = None, 0
        for tablet_index, _, tablet_size in tablets:
            if tablet_size > desired_tablet_size / 2 and tablet_size < desired_tablet_size:
                if first_index is not None:
                    if accumulated_size > desired_tablet_size:
                        yield (first_index, tablet_index - 1, accumulated_size)
                    else:
                        yield (first_index, tablet_index, accumulated_size + tablet_size)
                first_index, accumulated_size = None, 0
            else:
                if first_index is None:
                    first_index, accumulated_size = tablet_index, 0
                accumulated_size += tablet_size
        if first_index is not None:
            yield (first_index, len(tablets) - 1, accumulated_size)
    return list(_impl())


def combine_partitions(partitions, number_of_key_columns):
    def _impl():
        leftmost_pivot_key, accumulated_size = [], 0
        for pivot_key, size in partitions:
            if leftmost_pivot_key[0:number_of_key_columns] != pivot_key[0:number_of_key_columns]:
                if leftmost_pivot_key != []:
                    yield (leftmost_pivot_key, accumulated_size)
                leftmost_pivot_key, accumulated_size = pivot_key, 0
            accumulated_size += size
        if leftmost_pivot_key != []:
            yield (leftmost_pivot_key, accumulated_size)
    return list(_impl())


def rebalance_partitions(partitions, split_factor):
    # Here we would like to regroup partitions so they would have
    # roughly equal sum of sizes. This is known to be a hard issue,
    # and we are trying to bruteforce it. This should work fine for small
    # split factors.
    #
    # Ultimate goal is to find a partitioning that minimizes total squared
    # size mismatch; when splitting into two groups this reduces to minimizing
    # squared size difference.
    def _penalty(sums):
        p = 0
        n = len(sums)
        for i in range(n):
            for j in range(i + 1, n):
                r = float(sums[i] - sums[j]) / GB
                p += r * r
        return p

    def _impl(begin, end, total_sum, factor):
        if factor == 1:
            yield ([begin], [total_sum])
        else:
            assert begin < end
            current, left_sum, right_sum = begin + 1, partitions[begin][1], total_sum - partitions[begin][1]
            while current < end:
                delta = partitions[current][1]
                for breaks, sums in _impl(current, end, right_sum, factor - 1):
                    yield ([begin] + breaks, [left_sum] + sums)
                current += 1
                left_sum += delta
                right_sum -= delta

    def _solve():
        min_breaks = None
        min_penalty = None
        for breaks, sums in _impl(0, len(partitions), sum(size for _, size in partitions), split_factor):
            penalty = _penalty(sums)
            if min_penalty is None or penalty < min_penalty:
                min_breaks = breaks
                min_penalty = penalty
        for begin, end in zip(min_breaks[0:], min_breaks[1:]):
            yield (partitions[begin][0], sum(size for _, size in partitions[begin:end]))
        yield (partitions[min_breaks[-1]][0], sum(size for _, size in partitions[min_breaks[-1]:]))

    return list(_solve())


def suggest_pivot_keys(tablets_attributes, number_of_key_columns, desired_tablet_size):
    # Load partition information.
    distribution = [
        (partition["pivot_key"], partition["uncompressed_data_size"])
        for tablet_attributes in tablets_attributes
        for partition in tablet_attributes["partitions"]]
    # Adjust desired table size for smoother distribution.
    total_tablet_size = sum(size for _, size in distribution)
    split_factor = int(1 + total_tablet_size / desired_tablet_size)
    # Perform sanity checks.
    for tablet_attributes in tablets_attributes:
        if tablet_attributes["eden"]["uncompressed_data_size"] > 1 * GB:
            logging.warning("Eden is extermely large; likely data is not yet partitioned")
            return None
    if sum(map(lambda p: p[1], distribution)) < desired_tablet_size:
        logging.warning("Scarce partitions prevent from choosing good pivot keys")
        return None
    # Combine partitions with seemingly same pivot keys.
    distribution = combine_partitions(distribution, number_of_key_columns)
    distribution = rebalance_partitions(distribution, split_factor)
    # Compute pivot keys.
    pivot_keys = map(lambda p: p[0][0:number_of_key_columns], distribution)
    return [tablets_attributes[0]["pivot_key"]] + pivot_keys[1:]


def rebalance(table, number_of_key_columns, desired_tablet_size, yes=False):
    if not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
        logging.error("Cannot extract partition information from unmounted table; please, mount table aforehead")
        logging.error("Aborting")
        return

    logging.info("Analyzing %s", table)

    tablets = map(
        lambda t: (t[0], t[1]["tablet_id"], t[1]["statistics"]["uncompressed_data_size"]),
        enumerate(yt.get(table + "/@tablets")))

    number_of_large_tablets = sum(1 for t in tablets if t[2] > desired_tablet_size)
    number_of_small_tablets = sum(1 for t in tablets if t[2] < desired_tablet_size / 2)

    logging.info("There are %s small and %s large tablets out of %s total",
                 number_of_small_tablets, number_of_large_tablets, len(tablets))

    tablet_spans_to_rebalance = get_tablet_spans_to_rebalance(tablets, desired_tablet_size)

    logging.info("Got %s spans to rebalance", len(tablet_spans_to_rebalance))

    # Iterate in reverse order to produce correct sequence of operations.
    operations = []
    for first_index, last_index, cumulative_size in reversed(tablet_spans_to_rebalance):
        logging_subject = "Tablets %s-%s (cumulative size is %.2f GBs)" % (
            first_index, last_index, float(cumulative_size) / GB)
        if cumulative_size < desired_tablet_size:
            attributes = get_tablet_attributes(tablets[first_index][1])
            logging.info("%s will be merged", logging_subject)
            operations.append((first_index, last_index, [attributes["pivot_key"]]))
        else:
            attributes = map(get_tablet_attributes, map(lambda t: t[1], tablets[first_index:last_index+1]))
            keys = suggest_pivot_keys(attributes, number_of_key_columns, desired_tablet_size)
            if keys is None:
                logging.info("%s could not be resharded", logging_subject)
            elif len(keys) <= 1:
                logging.info("%s could not be resharded because of data skew", logging_subject)
                logging.info("Consider increasing `--key-columns`")
            else:
                logging.info("%s will be resharded with pivot keys %s", logging_subject, keys)
                operations.append((first_index, last_index, keys))

    if len(operations) == 0:
        logging.info("No operations to perform; exiting")
        return
    else:
        logging.info("About to perform %s operations", len(operations))

    if not yes:
        fmt = lambda x: "'" + yson.dumps(x)[1:-1] + "'"
        for first_index, last_index, keys in operations:
            print "yt reshard_table --first '%s' --last '%s' '%s' %s" % (
                first_index, last_index, table, " ".join(map(fmt, keys)))
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
    for first_index, last_index, keys in operations:
        logging.info("Resharding tablets %s-%s in table %s", first_index, last_index, table)
        yt.reshard_table(table, keys, first_tablet_index=first_index, last_tablet_index=last_index)

    logging.info("Mounting %s", table)
    yt.mount_table(table)
    while not all(x["state"] == "mounted" for x in yt.get(table + "/@tablets")):
        logging.info("Waiting for tablets to become mounted...")
        time.sleep(1)

    logging.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", type=str, required=True, help="Table to rebalance")
    parser.add_argument("--key-columns", type=int, required=False, default=1,
                        help="How many key columns to take into account")
    parser.add_argument("--threshold-gbs", type=long, required=False, default=80,
                        help="Maximum tablet size (in GBs)")
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.add_argument("--yes", action="store_true", help="Actually do something (do nothing by default)")
    args = parser.parse_args()
    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)
    rebalance(args.table, args.key_columns, args.threshold_gbs * GB, args.yes)
