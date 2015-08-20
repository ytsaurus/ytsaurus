#!/usr/bin/python

"""
This script rebalances given table, splitting large tablets if necessary.
"""

import argparse
import time
import logging
import json
import sys

from collections import namedtuple

import yt.wrapper as yt
import yt.yson as yson

yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT = "yson"

GB = 1024 * 1024 * 1024


# ADTs for data management.
class Partition(namedtuple("Partition", ["pivot_key", "size"])):
    pass


class Tablet(namedtuple("Tablet", ["tablet_id", "index", "size"])):
    pass


class Span(namedtuple("Span", ["first_index", "last_index", "size", "pivot_keys"])):
    @property
    def nice_str(self):
        return "%s-%s (%.2f GBs)" % (self.first_index, self.last_index, float(self.size) / GB)


# ADTs for encoding diagnostics.
class _EdenIsExtremelyLarge(namedtuple("_EdenIsExtremelyLarge", ["tablet_id", "size"])):
    def tackle(self):
        logging.warning(
            "Eden in tablet %s is extremely large (%.2f GBs); likely data is not yet partitioned",
            self.tablet_id, float(self.size) / GB)


class _ScarceInformation(namedtuple("_ScarceInformation", ["tablet_ids"])):
    def tackle(self):
        logging.warning(
            "Scarce information in tablets %s prevents from further partitioning",
            ", ".join(self.tablet_ids))


class _OversizedPartition(namedtuple("_OversizedPartition", ["pivot_key", "size"])):
    def tackle(self):
        logging.info(
            "Oversized partition with pivot key %s (estimated size is %.2f GBs)",
            self.pivot_key, float(self.size) / GB)


class _UndersizedPartition(namedtuple("_UndersizedPartition",
                                      ["pivot_key", "size", "is_left_mergeable", "is_right_mergeable"])):
    def tackle(self):
        hints = ["estimated size is %.2f GBs" % (float(self.size) / GB,)]
        if self.is_left_mergeable:
            hints.append("left-mergeable")
        if self.is_right_mergeable:
            hints.append("right-mergeable")
        if not self.is_left_mergeable and not self.is_right_mergeable:
            hints.append("non-mergeable")
        logging.info(
            "Undersized partition with pivot key %s (%s)",
            self.pivot_key, ", ".join(hints))


def check_tablet_state(table, state):
    return all(t["state"] == state for t in yt.get(table + "/@tablets"))


def wait_tablet_state(table, state):
    while not check_tablet_state(table, state):
        logging.info("Waiting for tablets to become %s...", state)
        time.sleep(1)


def get_tablet_attributes(tablet):
    tablet_id = tablet.tablet_id
    tablet_cell_id = yt.get("//sys/tablets/%s/@cell_id" % tablet_id)
    peers = yt.get("//sys/tablet_cells/%s/@peers" % tablet_cell_id)
    leading_peers = filter(lambda p: p["state"] == "leading", peers)
    assert len(leading_peers) == 1
    assert leading_peers[0].get("address", None) is not None
    # Due to 0.16-0.17 incompatibilities in YSON we load information in JSON and deserialize it manually.
    attributes = yt.get("//sys/nodes/%s/orchid/tablet_cells/%s/tablets/%s" % (
        leading_peers[0]["address"],
        tablet_cell_id,
        tablet_id), format=yt.JsonFormat())
    attributes = json.loads(attributes)
    # Hack these inside for simplicity.
    attributes["tablet_id"] = tablet_id
    attributes["tablet_cell_id"] = tablet_cell_id
    return attributes


def extract_unbalanced_spans(tablets, desired_size):
    def _impl():
        first_index, accumulated_size = None, 0
        for tablet in tablets:
            if tablet.size > desired_size / 2 and tablet.size < desired_size:
                if first_index is not None:
                    if accumulated_size > desired_size:
                        yield Span(
                            first_index=first_index,
                            last_index=tablet.index - 1,
                            size=accumulated_size,
                            pivot_keys=None)
                    else:
                        yield Span(
                            first_index=first_index,
                            last_index=tablet.index,
                            size=accumulated_size + tablet.size,
                            pivot_keys=None)
                first_index, accumulated_size = None, 0
            else:
                if first_index is None:
                    first_index, accumulated_size = tablet.index, 0
                accumulated_size += tablet.size
        if first_index is not None:
            yield Span(
                first_index=first_index,
                last_index=len(tablets) - 1,
                size=accumulated_size)

    return list(_impl())


def combine_partitions(partitions, number_of_key_columns):
    # Combine all partitions that share same pivot key prefix of length |number_of_key_columns|.
    def _impl():
        leftmost_pivot_key, accumulated_size = [], 0
        for partition in partitions:
            if leftmost_pivot_key[0:number_of_key_columns] != partition.pivot_key[0:number_of_key_columns]:
                if leftmost_pivot_key != []:
                    yield Partition(
                        pivot_key=leftmost_pivot_key,
                        size=accumulated_size)
                leftmost_pivot_key, accumulated_size = partition.pivot_key, 0
            accumulated_size += partition.size
        if leftmost_pivot_key != []:
            yield Partition(
                pivot_key=leftmost_pivot_key,
                size=accumulated_size)

    return list(_impl())


def rebalance_partitions(partitions, desired_size):
    # First, find oversized partitions and reuse them as is; as for others,
    # do whatever appropriate.
    def _impl():
        first_index = 0
        for index, partition in enumerate(partitions):
            if partition.size > desired_size:
                if index > first_index:
                    for resulting_partition in rebalance_partitions_given_desired_size(
                            partitions[first_index:index], desired_size):
                        yield resulting_partition
                yield partition
                first_index = index + 1
        if first_index < len(partitions):
            for resulting_partition in rebalance_partitions_given_desired_size(
                    partitions[first_index:len(partitions)], desired_size):
                yield resulting_partition

    return list(_impl())


def rebalance_partitions_given_desired_size(partitions, desired_size):
    assert len(partitions) >= 1

    total_size = sum(partition.size for partition in partitions)
    if total_size < desired_size:
        yield Partition(pivot_key=partitions[0].pivot_key, size=total_size)
        return

    split_factor = int(1 + total_size / desired_size)
    for partition in rebalance_partitions_given_split_factor(partitions, split_factor):
        yield partition


def rebalance_partitions_given_split_factor(partitions, split_factor):
    assert len(partitions) >= 1

    # If split factor is too high then use approximate solution: first, try to
    # split partitions into two (roughly) equal groups, and then split every half further.
    if split_factor >= 4:
        logging.warning(
            "rebalance_partitions_given_split_factor() was called with split_factor=%s",
            split_factor)
        binary_partitions = rebalance_partitions_given_split_factor(partitions, 2)
        binary_partitions = list(binary_partitions)
        assert len(binary_partitions) == 2
        middle_pivot_key = binary_partitions[1].pivot_key
        middle_index = filter(
            lambda i: partitions[i].pivot_key == middle_pivot_key,
            range(len(partitions)))
        assert len(middle_index) == 1
        middle_index = middle_index[0]
        assert middle_index >= 0 and middle_index < len(partitions)
        left_factor = split_factor / 2
        left_partitions = rebalance_partitions_given_split_factor(partitions[:middle_index], left_factor)
        right_factor = split_factor - left_factor
        right_partitions = rebalance_partitions_given_split_factor(partitions[middle_index:], right_factor)
        for resulting_partition in left_partitions:
            yield resulting_partition
        for resulting_partition in right_partitions:
            yield resulting_partition
        return

    # Here we would like to regroup partitions so they would have roughly equal sum of sizes.
    # This is known to be a hard problem, and we are trying to bruteforce it.
    # This should work fine for small split factors.
    #
    # Ultimate goal is to find a partitioning that minimizes total squared size difference.
    def _penalty(sums):
        p = 0
        n = len(sums)
        for i in range(n):
            for j in range(i + 1, n):
                r = float(sums[i] - sums[j]) / GB
                p += r * r
        return p

    def _impl(begin, end, total_sum, factor):
        if factor == 1 or begin + 1 == end:
            yield ([begin], [total_sum])
        else:
            assert begin < end
            current, left_sum, right_sum = begin + 1, partitions[begin].size, total_sum - partitions[begin].size
            while current < end:
                delta = partitions[current].size
                for breaks, sums in _impl(current, end, right_sum, factor - 1):
                    yield ([begin] + breaks, [left_sum] + sums)
                current += 1
                left_sum += delta
                right_sum -= delta

    def _solve():
        min_breaks = None
        min_penalty = None
        for breaks, sums in _impl(0, len(partitions), sum(_.size for _ in partitions), split_factor):
            penalty = _penalty(sums)
            if min_penalty is None or penalty < min_penalty:
                min_breaks = breaks
                min_penalty = penalty
        assert min_breaks is not None
        assert min_penalty is not None
        for begin, end in zip(min_breaks[0:], min_breaks[1:]):
            yield Partition(
                pivot_key=partitions[begin].pivot_key,
                size=sum(_.size for _ in partitions[begin:end]))
        yield Partition(
            pivot_key=partitions[min_breaks[-1]].pivot_key,
            size=sum(_.size for _ in partitions[min_breaks[-1]:]))

    for resulting_partition in _solve():
        yield resulting_partition


def suggest_pivot_keys(tablets_attributes, number_of_key_columns, desired_size):
    # Load partition information.
    partitions = [
        Partition(
            pivot_key=partition_attributes["pivot_key"],
            size=partition_attributes["uncompressed_data_size"])
        for tablet_attributes in tablets_attributes
        for partition_attributes in tablet_attributes["partitions"]]
    # Perform sanity checks.
    for tablet_attributes in tablets_attributes:
        eden_size = tablet_attributes["eden"]["uncompressed_data_size"]
        if eden_size > GB:
            return [_EdenIsExtremelyLarge(tablet_id=tablet_attributes["tablet_id"], size=eden_size)], None
    # Now, figure out what to do with this span,
    partitions = combine_partitions(partitions, number_of_key_columns)
    partitions = rebalance_partitions(partitions, desired_size)
    diagnostics = []
    for index, partition in enumerate(partitions):
        if partition.size > desired_size:
            diagnostics.append(_OversizedPartition(
                pivot_key=partition.pivot_key[0:number_of_key_columns],
                size=partition.size))
        if partition.size < desired_size / 2:
            diagnostics.append(_UndersizedPartition(
                pivot_key=partition.pivot_key[0:number_of_key_columns],
                size=partition.size,
                is_left_mergeable=(index == 0),
                is_right_mergeable=(index + 1 == len(partitions))))
    # Compute pivot keys.
    pivot_keys = [partition.pivot_key[0:number_of_key_columns] for partition in partitions]
    pivot_keys = [tablets_attributes[0]["pivot_key"]] + pivot_keys[1:]
    sizes = [partition.size for partition in partitions]
    return diagnostics, pivot_keys, sizes


def rebalance(table, number_of_key_columns, desired_tablet_size, requested_spans=None, yes=False):
    if not check_tablet_state(table, "mounted"):
        logging.error("Cannot extract partition information from unmounted table; please, mount table aforehead")
        logging.error("Aborting")
        return

    logging.info("Analyzing %s", table)

    tablets = [Tablet(
        tablet_id=t["tablet_id"],
        index=t["index"],
        size=t["statistics"]["uncompressed_data_size"])
        for t in yt.get(table + "/@tablets")]

    spans = []

    def _recompute_span_size_and_pivot_key(span):
        return span._replace(
            size=sum(tablet.size for tablet in tablets[span.first_index:span.last_index+1]),
            pivot_keys=None)

    if requested_spans is None or len(requested_spans) == 0:
        number_of_large_tablets = sum(1 for tablet in tablets if tablet.size > desired_tablet_size)
        number_of_small_tablets = sum(1 for tablet in tablets if tablet.size < desired_tablet_size / 2)

        logging.info("There are %s small and %s large tablets out of %s total",
                     number_of_small_tablets, number_of_large_tablets, len(tablets))

        spans = extract_unbalanced_spans(tablets, desired_tablet_size)
    else:
        spans = []
        for requested_span in requested_spans:
            parts = map(int, requested_span.split("-"))
            span = None
            if len(parts) == 1:
                span = Span(first_index=parts[0], last_index=parts[0], size=None, pivot_keys=None)
            elif len(parts) == 2:
                span = Span(first_index=parts[0], last_index=parts[1], size=None, pivot_keys=None)
            else:
                logging.warning("Bad span `%s`; skipping", requested_span)
                continue
            span = _recompute_span_size_and_pivot_key(span)
            spans.append(span)

    logging.info("Got %s spans to rebalance", len(spans))

    # Iterate in reverse order to produce correct sequence of operations.
    consider_increasing_key_columns = False

    for iteration in range(5):
        reiterate = False

        logging.info("Iteration %s", iteration + 1)

        # Compute new spans.
        updated_spans = []
        for span in spans:
            if span.pivot_keys is None:
                logging.info("Analyzing span %s", span.nice_str)
                if span.size < desired_tablet_size:
                    logging.info("Span %s will be merged into a single tablet", span.nice_str)
                    attributes = get_tablet_attributes(tablets[span.first_index])
                    span = span._replace(pivot_keys=[attributes["pivot_key"]])
                else:
                    attributes = map(get_tablet_attributes, tablets[span.first_index:span.last_index+1])
                    diagnostics, pivot_keys, sizes = suggest_pivot_keys(attributes, number_of_key_columns,
                                                                        desired_tablet_size)
                    logging.info("Suggested splits are %s",
                                 " + ".join("%.2f GBs" % (float(size) / GB) for size in sizes))
                    for diagnostic in diagnostics:
                        diagnostic.tackle()
                        if isinstance(diagnostic, _OversizedPartition):
                            consider_increasing_key_columns = True
                        if isinstance(diagnostic, _UndersizedPartition):
                            extended_span = None
                            if diagnostic.is_left_mergeable and span.first_index - 1 >= 0:
                                extended_span = span._replace(first_index=span.first_index-1)
                            if diagnostic.is_right_mergeable and span.last_index + 1 < len(tablets):
                                extended_span = span._replace(last_index=span.last_index+1)
                            if not diagnostic.is_left_mergeable and not diagnostic.is_right_mergeable:
                                consider_increasing_key_columns = True
                                pivot_keys = None  # NB: Do not update this span.
                            if extended_span is not None:
                                extended_span = _recompute_span_size_and_pivot_key(extended_span)
                                logging.info(
                                    "Extending span %s to span %s; will reiterate",
                                    span.nice_str, extended_span.nice_str)
                                span = extended_span
                                pivot_keys = None  # NB: Do not update this span.
                                reiterate = True
                    if pivot_keys is not None and len(pivot_keys) > 1:
                        logging.info("Span %s will be resharded with pivot keys %s", span.nice_str, pivot_keys)
                        span = span._replace(pivot_keys=pivot_keys)
                    else:
                        logging.info("Span %s will be kept intact", span.nice_str)
            updated_spans.append(span)

        # Merge adjacent spans that were extended.
        spans, i, j = [], 0, 0
        while i < len(updated_spans):
            j = i + 1
            while j < len(updated_spans) and (updated_spans[j].first_index - updated_spans[i].last_index) <= 1:  # noqa
                j += 1
            if (j - i) == 1:
                spans.append(updated_spans[i])
            else:
                first_index, last_index = updated_spans[i].first_index, updated_spans[j - 1].last_index
                merged_span = Span(first_index=first_index, last_index=last_index, size=None, pivot_keys=None)
                merged_span = _recompute_span_size_and_pivot_key(merged_span)
                logging.info("Merging spans %s into %s",
                             ", ".join(span.nice_str for span in updated_spans[i:j]),
                             merged_span.nice_str)
                spans.append(merged_span)
            i = j

        # If done, just break.
        if not reiterate:
            break

    for previous_span, next_span in zip(spans[0:], spans[1:]):
        assert previous_span.first_index <= previous_span.last_index
        assert previous_span.last_index + 1 < next_span.first_index
        assert next_span.first_index <= next_span.last_index

    if consider_increasing_key_columns:
        logging.warning("Consider increasing `--key-columns` for better balance")

    spans = filter(lambda span: span.pivot_keys is not None, spans)
    if len(spans) == 0:
        logging.info("No reshards to perform; exiting")
        return
    else:
        logging.info("About to perform %s reshards", len(spans))

    if not yes:
        fmt = lambda x: "'" + yson.dumps(x)[1:-1] + "'"
        for span in reversed(spans):
            print "yt reshard_table --first '%s' --last '%s' '%s' %s" % (
                span.first_index, span.last_index, table, " ".join(map(fmt, span.pivot_keys)))
        logging.info("`--yes` was not specified; exiting")
        return
    else:
        logging.info("You have 5 seconds to press Ctrl+C to abort...")
        time.sleep(5)

    logging.info("Unmounting %s", table)
    yt.unmount_table(table)
    wait_tablet_state(table, "unmounted")

    logging.info("Resharding %s", table)
    for span in reversed(spans):
        logging.info("Resharding tablets %s-%s in table %s", span.first_index, span.last_index, table)
        yt.reshard_table(table, span.pivot_keys,
                         first_tablet_index=span.first_index, last_tablet_index=span.last_index)

    logging.info("Mounting %s", table)
    yt.mount_table(table)
    wait_tablet_state(table, "mounted")

    logging.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", type=str, required=True, help="Table to rebalance")
    parser.add_argument("--key-columns", type=int, required=False, default=1,
                        help="How many key columns to take into account")
    parser.add_argument("--threshold-gbs", type=long, required=False, default=80,
                        help="Maximum tablet size (in GBs)")
    parser.add_argument("--span", action="append", type=str, required=False,
                        help="Rebalance specific tablet spans (e.g.: 143, 156-158, ...)")
    parser.add_argument("--silent", action="store_true", help="Do not log anything")
    parser.add_argument("--yes", action="store_true", help="Actually do something (do nothing by default)")
    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    rebalance(args.table, args.key_columns, args.threshold_gbs * GB, args.span, args.yes)
