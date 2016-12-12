#!/usr/bin/python

"""
This script rebalances given table, splitting large tablets if necessary.

Alyx/Seneca:
  ./rebalance.py --oversized --no-undersized --desired-size-gbs 80 \
    --table //yabs/OrderCounter

Pythia/Vanga:
  ./rebalance.py --no-oversized --no-undersized --desired-size-gbs 10 \
    --include '^//home/statface/webface.*' \
    --exclude '.*/_rm/.*' \

TODO:
  - Option that rebalances table entirely.
"""

import argparse
import time
import logging
import json
import math
import re

from collections import namedtuple

import yt.wrapper as yt
import yt.yson as yson

GB = 1024 * 1024 * 1024
MAX_ITERATIONS = 5  # Maximum number of iterations until balance convergance.


# ADTs for data management.
class Partition(namedtuple("Partition", ["pivot_key", "size"])):
    @property
    def nice_str(self):
        return "{%s, %.2f GBs}" % (self.pivot_key, float(self.size) / GB)


class Tablet(namedtuple("Tablet", ["tablet_id", "cell_id", "index", "pivot_key", "size", "attributes"])):
    def _load_attributes(self):
        all_peers = yt.get("//sys/tablet_cells/{}/@peers".format(self.cell_id))
        leading_peers = filter(lambda p: p["state"] == "leading", all_peers)
        assert len(leading_peers) == 1
        assert "address" in leading_peers[0]
        # TODO(sandello): Rewrite this in YSON, because we are on 18-19 now!
        # Due to 0.16-0.17 incompatibilities in YSON we load information in JSON and deserialize it manually.
        attributes = yt.get(
            "//sys/nodes/{}/orchid/tablet_cells/{}/tablets/{}".format(
                leading_peers[0]["address"], self.cell_id, self.tablet_id),
            format=yt.JsonFormat())
        self.attributes.update(json.loads(attributes))

    def _ensure_attributes(self):
        if len(self.attributes) == 0:
            self._load_attributes()

    @property
    def eden_size(self):
        self._ensure_attributes()
        return self.attributes["eden"]["uncompressed_data_size"]

    @property
    def partitions(self):
        self._ensure_attributes()
        return [
            Partition(pivot_key=p["pivot_key"], size=p["uncompressed_data_size"])
            for p in self.attributes["partitions"]]


class Span(namedtuple("Span", ["first_index", "last_index", "partitions", "altered"])):
    @property
    def count(self):
        return 1 + self.last_index - self.first_index

    @property
    def sizes(self):
        return [partition.size for partition in self.partitions]

    @property
    def pivot_keys(self):
        return [partition.pivot_key for partition in self.partitions]

    @property
    def total_size(self):
        return sum(self.sizes)

    @property
    def nice_str(self):
        return "%s-%s (%.2f GBs)" % (self.first_index, self.last_index, float(self.total_size) / GB)

    @property
    def nice_partitions_str(self):
        return "; ".join(partition.nice_str for partition in self.partitions)


# ADTs for encoding diagnostics.
class _EdenIsExtremelyLarge(namedtuple("_EdenIsExtremelyLarge", ["tablet"])):
    def display(self):
        logging.warning(
            "Eden in tablet %s is extremely large (%.2f GBs); likely data is not yet partitioned",
            self.tablet.tablet_id, float(self.tablet.eden_size) / GB)


class _ScarceInformation(namedtuple("_ScarceInformation", ["tablets"])):
    def display(self):
        logging.warning(
            "Scarce information in tablets %s prevents from further partitioning",
            ", ".join(tablet.tablet_id for tablet in self.tablets))


def check_tablet_state(table, state):
    return all(t["state"] == state for t in yt.get(table + "/@tablets"))


def wait_tablet_state(table, state):
    while not check_tablet_state(table, state):
        logging.info("Waiting for tablets to become %s...", state)
        time.sleep(1)


def fill_span_partitions_from_tablets(tablets, span):
    """
    Fills in initial partition structure given by tablet statistics and pivot keys.
    """
    return span._replace(
        partitions=[
            Partition(pivot_key=tablet.pivot_key, size=tablet.size)
            for tablet in tablets[span.first_index:span.last_index+1]],
        altered=False)


def extract_unbalanced_spans(tablets, desired_size):
    """
    Scans provided list for missized tablets (> |desired_size| or < |desired_size| / 2 and
    groups them in contiguous spans that are at least 3 * |desired_size| / 2 in size
    for better hysteresis (so merged tablet could be split into two 3/4-sized tablets).

    When there is not enough data in missized tablets, group may span over a properly sized tablet.

    Ultimate goal is to return a disjoint list of tablet spans that worth rebalancing.
    """
    def _impl():
        first_index, last_index, accumulated_size = -1, -1, 0
        for tablet in tablets:
            if tablet.size > desired_size / 2 and tablet.size < desired_size:
                if first_index >= 0:
                    # Accumulate slightly more data to achieve hysteresis
                    # for further runs.
                    if accumulated_size > 3 * desired_size / 2:
                        last_index = tablet.index - 1
                        yield Span(
                            first_index=first_index, last_index=last_index,
                            partitions=None, altered=False)
                        first_index, accumulated_size = -1, 0
                    else:
                        accumulated_size += tablet.size
            else:
                if first_index < 0:
                    first_index, accumulated_size = tablet.index, 0
                accumulated_size += tablet.size
        if first_index >= 0:
            while accumulated_size < 3 * desired_size / 2:
                if first_index - last_index > 2:
                    first_index = first_index - 1
                    accumulated_size += tablets[first_index].size
                else:
                    break
            yield Span(
                first_index=first_index, last_index=len(tablets) - 1,
                partitions=None, altered=False)

    return list(_impl())


def extract_requested_spans(tablets, requested_spans):
    """
    Parses user-provided list of tablet spans.
    """
    def _impl():
        for requested_span in requested_spans:
            parts = map(int, requested_span.split("-"))
            span = None
            if len(parts) == 1:
                span = Span(first_index=parts[0], last_index=parts[0], partitions=None, altered=False)
            elif len(parts) == 2:
                span = Span(first_index=parts[0], last_index=parts[1], partitions=None, altered=False)
            else:
                logging.warning("Bad span `%s`; skipping", requested_span)
                continue
            if span is not None:
                yield span

    return list(_impl())


def check_that_spans_are_disjoint(spans):
    for previous_span, next_span in zip(spans[0:], spans[1:]):
        assert previous_span.first_index <= previous_span.last_index, \
            "Span %s has invalid indexes" % previous_span.nice_str
        assert previous_span.last_index + 1 < next_span.first_index, \
            "Span %s overlaps or in contact with %s" % (previous_span.nice_str, next_span.nice_str)
        assert next_span.first_index <= next_span.last_index, \
            "Span %s has invalid indexes" % next_span.nice_str


def combine_partitions(partitions, number_of_key_columns):
    """
    Combines partitions that share same pivot key prefix of length |number_of_key_columns|.
    """
    def _impl():
        leftmost_pivot_key, accumulated_size = [], 0
        for partition in partitions:
            if leftmost_pivot_key[0:number_of_key_columns] != partition.pivot_key[0:number_of_key_columns]:
                if leftmost_pivot_key != []:
                    yield Partition(pivot_key=leftmost_pivot_key, size=accumulated_size)
                leftmost_pivot_key, accumulated_size = partition.pivot_key, 0
            accumulated_size += partition.size
        if leftmost_pivot_key != []:
            yield Partition(pivot_key=leftmost_pivot_key, size=accumulated_size)

    return list(_impl())


def rebalance_partitions(partitions, desired_size):
    """
    Best-effort partition rebalancing.

    Current implementation skips oversized partitions (as there is not enough information to do
    anything meaningful) and reuses them as is; for others is tries to regroup them.
    """
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
        logging.info(
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


def suggest_partitions(tablets, number_of_key_columns, desired_size):
    # Load detailed partition information.
    partitions = [partition for tablet in tablets for partition in tablet.partitions]

    # Perform sanity checks.
    diagnostics = []
    for tablet in tablets:
        tablet._ensure_attributes()
        if tablet.eden_size > GB:
            diagnostics.append(_EdenIsExtremelyLarge(tablet=tablet))
    if sum(partition.size for partition in partitions) < desired_size / 2:
        diagnostics.append(_ScarceInformation(tablets=tablets))
    if len(diagnostics) > 0:
        return diagnostics, None

    # Now, figure out what to do with this span.
    partitions = combine_partitions(partitions, number_of_key_columns)
    partitions = rebalance_partitions(partitions, desired_size)

    assert len(partitions) > 0

    # Now, patch pivot keys.
    for index, partition in enumerate(partitions):
        partitions[index] = partition._replace(pivot_key=partition.pivot_key[0:number_of_key_columns])
    partitions[0] = partitions[0]._replace(pivot_key=tablets[0].pivot_key)

    # Done!
    return diagnostics, partitions


def rebalance_spans(tablets, spans, number_of_key_columns, desired_size,
                    allow_oversized, allow_undersized):
    consider_increasing_key_columns = False

    for iteration in range(MAX_ITERATIONS):
        logging.info("Analyzing %s spans, iteration %s", len(spans), iteration + 1)

        # Sanity check.
        check_that_spans_are_disjoint(spans)

        reiterate = False

        # Compute new spans.
        updated_spans = []
        for span in spans:
            if span.altered:
                updated_spans.append(span)
                continue

            logging.info("Analyzing span %s", span.nice_str)

            if span.total_size < desired_size and span.total_size > desired_size / 2:
                logging.info("Span %s will be altered into a single partition", span.nice_str)
                span = span._replace(
                    partitions=[Partition(pivot_key=span.pivot_keys[0], size=span.total_size)],
                    altered=True)
            else:
                diagnostics, partitions = suggest_partitions(
                    tablets[span.first_index:span.last_index+1],
                    number_of_key_columns, desired_size)
                for diagnostic in diagnostics:
                    diagnostic.display()
                if partitions is not None:
                    logging.info(
                        "Span %s is suggested to be altered with partitioning {%s}",
                        span.nice_str,
                        "; ".join(partition.nice_str for partition in partitions))
                if partitions is not None:
                    for partition in partitions:
                        if partition.size > desired_size and not allow_oversized:
                            logging.info("Span %s was not altered because of oversized partition %s",
                                         span.nice_str, partition.nice_str)
                            partitions = None  # NB: Do not alter this span.
                            consider_increasing_key_columns = True
                            break
                if partitions is not None:
                    extended_span = None
                    if partitions[0].size < desired_size / 2 and span.first_index - 1 >= 0:
                        extended_span = span._replace(first_index=span.first_index-1)
                    if partitions[-1].size < desired_size / 2 and span.last_index + 1 < len(tablets):
                        extended_span = span._replace(last_index=span.last_index+1)
                    if extended_span is not None:
                        extended_span = fill_span_partitions_from_tablets(tablets, extended_span)
                        logging.info(
                            "Extending span %s to span %s; will reiterate",
                            span.nice_str, extended_span.nice_str)
                        span = extended_span
                        partitions = None  # NB: Do not alter this span.
                        reiterate = True
                if partitions is not None:
                    updated_partitions = []
                    index = 0
                    while index < len(partitions):
                        current_partition = partitions[index]
                        if current_partition.size < desired_size / 2 and not allow_undersized:
                            if allow_oversized:
                                can_merge_to_the_left = len(updated_partitions) > 0
                                can_merge_to_the_right = index + 1 < len(partitions)
                                if can_merge_to_the_left:
                                    left_partition = updated_partitions[-1]
                                if can_merge_to_the_right:
                                    right_partition = partitions[index + 1]
                                if can_merge_to_the_left and can_merge_to_the_right:
                                    if left_partition.size < right_partition.size:
                                        can_merge_to_the_right = False
                                    else:
                                        can_merge_to_the_left = False
                                if can_merge_to_the_left:
                                    updated_partitions[-1] = Partition(
                                        pivot_key=left_partition.pivot_key,
                                        size=left_partition.size + current_partition.size)
                                if can_merge_to_the_right:
                                    updated_partitions.append(Partition(
                                        pivot_key=current_partition.pivot_key,
                                        size=current_partition.size + right_partition.size))
                                    index = index + 1
                            else:
                                logging.info("Span %s was not altered because of undersized partition %s",
                                             span.nice_str, current_partition.nice_str)
                                partitions = None  # NB: Do not alter this span.
                                consider_increasing_key_columns = True
                                break
                        else:
                            updated_partitions.append(current_partition)
                        index = index + 1
                    if partitions is not None:
                        partitions = updated_partitions
                if partitions is not None:
                    old_pivot_keys = [partition.pivot_key for partition in span.partitions]
                    new_pivot_keys = [partition.pivot_key for partition in partitions]
                    if old_pivot_keys != new_pivot_keys:
                        span = span._replace(partitions=partitions, altered=True)
                        logging.info("Span %s was altered with the suggested partitioning", span.nice_str)
                    else:
                        logging.info("Span %s was not altered because pivot keys are okay", span.nice_str)

            updated_spans.append(span)

        # Merge adjacent spans that were extended.
        spans, i, j = [], 0, 0
        while i < len(updated_spans):
            j = i + 1
            while j < len(updated_spans) and (updated_spans[j].first_index - updated_spans[j - 1].last_index) <= 1:
                j += 1
            if (j - i) == 1:
                spans.append(updated_spans[i])
            else:
                first_index, last_index = updated_spans[i].first_index, updated_spans[j - 1].last_index
                merged_span = Span(first_index=first_index, last_index=last_index, partitions=None, altered=False)
                merged_span = fill_span_partitions_from_tablets(tablets, merged_span)
                logging.info("Merging spans %s into %s",
                             ", ".join(span.nice_str for span in updated_spans[i:j]),
                             merged_span.nice_str)
                spans.append(merged_span)
            i = j

        # If done, just break.
        if not reiterate:
            break

    # Sanity check.
    check_that_spans_are_disjoint(spans)

    if consider_increasing_key_columns:
        logging.warning("Consider increasing `--key-columns` for better balance")

    logging.info("Analysis completed, got %s spans", len(spans))

    return spans


def rebalance_table(table, number_of_key_columns, desired_size, requested_spans,
                    allow_oversized, allow_undersized):
    if not check_tablet_state(table, "mounted"):
        logging.warning("Table is not mounted; please, mount table aforehead; aborting")
        return None, None

    logging.info("Analyzing %s", table)

    tablets = [Tablet(
        tablet_id=t["tablet_id"],
        cell_id=t["cell_id"],
        index=t["index"],
        pivot_key=t["pivot_key"],
        size=t["statistics"]["uncompressed_data_size"],
        attributes={})
        for t in yt.get(table + "/@tablets")]

    original_spans = []

    if requested_spans is None or len(requested_spans) == 0:
        number_of_large_tablets = sum(1 for tablet in tablets if tablet.size > desired_size)
        number_of_small_tablets = sum(1 for tablet in tablets if tablet.size < desired_size / 2)

        logging.info("Table has %s tablets, %s small and %s large",
                     len(tablets), number_of_small_tablets, number_of_large_tablets)

        original_spans = extract_unbalanced_spans(tablets, desired_size)
    else:
        original_spans = extract_requested_spans(tablets, requested_spans)

    original_spans = [fill_span_partitions_from_tablets(tablets, span) for span in original_spans]
    balanced_spans = rebalance_spans(tablets, original_spans, number_of_key_columns, desired_size,
                                     allow_oversized, allow_undersized)
    balanced_spans = filter(lambda span: span.altered, balanced_spans)
    original_spans = [fill_span_partitions_from_tablets(tablets, span) for span in balanced_spans]

    # Check if we are going to change anything at all.
    if len(balanced_spans) == 0:
        logging.warning("No altered spans; aborting")
        return None, None

    return original_spans, balanced_spans


def compute_logit_coefficients(spans, desired_size):
    # Compute with prior samples [DS; DS/2] = [1; 1/2]
    x0 = float(desired_size) / GB
    s1 = 1.5
    s2 = 1.25
    sO = 0.0
    sU = 0.0
    n = 2
    for span in spans:
        for partition in span.partitions:
            xi = float(partition.size) / GB
            xi = xi / x0
            s1 += xi
            s2 += xi * xi
            if xi > 1.0:
                sO += xi - 1.0
            if xi < 0.5:
                sU += 0.5 - xi
            n += 1
    # variance, complexity, total overdraft, total underdraft.
    m1 = s1 / n
    m2 = s2 / n
    var = math.sqrt((float(n) / float(n - 1.5)) * (m2 - m1 * m1))
    cpx = math.log(n)
    return [var, cpx, sO, sU]


def make_table_list(args):
    tables = []
    for table in args.table:
        tables.append(table)

    if len(args.include) + len(args.exclude) > 0:
        all_tables = []
        for table in yt.search("/", node_type="table", attributes=["tablets", "locks"]):
            tablets = table.attributes.get("tablets", [])
            locks = table.attributes.get("locks", [])
            if len(locks) > 0:
                continue
            sizes = [tablet["statistics"]["uncompressed_data_size"] for tablet in tablets]
            if sum(sizes) < args.desired_size_gbs * GB / 2:
                continue
            all_tables.append(str(table))
        for regexp in args.include:
            all_tables = filter(lambda table: re.match(regexp, table), all_tables)
        for regexp in args.exclude:
            all_tables = filter(lambda table: not re.match(regexp, table), all_tables)
        tables = list(sorted(set(tables + all_tables)))

    if len(tables) == 0:
        logging.error("You must specify at least one table with either `--table` or `--include`/`--exclude`")
        return None

    if len(tables) > 1 and args.span:
        logging.error("Cannot use --span with multiple input tables")
        return None

    return tables


def main(args):
    tables = make_table_list(args)
    if tables is None:
        return

    for i, table in enumerate(tables):
        logging.info("Table %s/%s -- %s", i, len(tables), table)

        old_spans, new_spans = rebalance_table(
            table, args.key_columns, args.desired_size_gbs * GB, args.span,
            args.allow_oversized, args.allow_undersized)
        if old_spans is None or new_spans is None:
            continue

        for old_span, new_span in zip(old_spans, new_spans):
            print "Span %s |> -- {%s}" % (old_span.nice_str, old_span.nice_partitions_str)
            print "Span %s |> ++ {%s}" % (new_span.nice_str, new_span.nice_partitions_str)

        old_coefs = compute_logit_coefficients(old_spans, args.desired_size_gbs * GB)
        new_coefs = compute_logit_coefficients(new_spans, args.desired_size_gbs * GB)

        delta_coefs = [(new - old) for new, old in zip(new_coefs, old_coefs)]

        if args.action == "build_logit_train_set":
            main_build_logit_train_set(delta_coefs, args.output)
        elif args.action == "improve":
            main_improve(table, old_spans, new_spans, delta_coefs, args.yes)
        else:
            logging.fatal("Unknown action `%s`", args.action)


def main_build_logit_train_set(delta_coefs, output):
    while True:
        decision = raw_input("Yes/No/Skip? ")
        if decision.lower() in ["y", "ye", "yes"]:
            klass = 1
            break
        elif decision.lower() in ["n", "no"]:
            klass = 0
            break
        elif decision.lower() in ["s", "sk", "ski", "skip"]:
            klass = None
            break

    if klass is not None:
        with open(output, "a") as handle:
            handle.write("\t".join(map(str, [klass] + [d for d in delta_coefs])))
            handle.write("\n")
            handle.write("\t".join(map(str, [1 - klass] + [-d for d in delta_coefs])))
            handle.write("\n")


def main_improve(table, old_spans, new_spans, delta_coefs, yes):
    logit = \
        delta_coefs[0] * args.logit_variance_coef + \
        delta_coefs[1] * args.logit_complexity_coef + \
        delta_coefs[2] * args.logit_overdraft_coef + \
        delta_coefs[3] * args.logit_underdraft_coef

    if logit < -1e-5:
        logging.warning("Logit %.4f < 0.0; skipping this change", logit)
        return
    else:
        logging.info("Logit %.4f > 0.0; applying this change", logit)

    if yes:
        if yes == 1:
            logging.info("You have 5 seconds to press Ctrl+C to abort...")
            time.sleep(5)
    else:
        logging.warning("`--yes` was not specified; performing dry run")

    try:
        ctors = []
        for schemata in yt.get(table + "/@schema"):
            if schemata.get("sort_order", None) != "ascending":
                break
            ty = schemata.get("type", None)
            if ty == "string":
                ctors.append(yson.YsonString)
            elif ty == "int64":
                ctors.append(yson.YsonInt64)
            elif ty == "uint64":
                ctors.append(yson.YsonUint64)
            else:
                raise RuntimeError("Unknown key column type %s" % ty)

        if yes:
            logging.info("Unmounting %s", table)
            yt.unmount_table(table)
            wait_tablet_state(table, "unmounted")

        fmt = lambda x: "'" + yson.dumps(x)[1:-1] + "'"
        for span in reversed(new_spans):
            if yes:
                logging.info("Resharding tablets %s-%s in table %s", span.first_index, span.last_index, table)
                pivot_keys = []
                for parts in span.pivot_keys:
                    pivot_key = []
                    for ctor, value in zip(ctors, parts):
                        pivot_key.append(ctor(value))
                    pivot_keys.append(pivot_key)
                yt.reshard_table(table, pivot_keys,
                                 first_tablet_index=span.first_index, last_tablet_index=span.last_index)
            else:
                print "yt reshard_table --first '%s' --last '%s' '%s' %s" % (
                    span.first_index, span.last_index, table, " ".join(map(fmt, span.pivot_keys)))
    finally:
        if yes:
            logging.info("Mounting %s", table)
            yt.mount_table(table)
            wait_tablet_state(table, "mounted")

    logging.info("Done!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--key-columns", metavar="N", type=int, required=False, default=1,
                        help="Number of key columns to use in pivot keys")
    parser.add_argument("--desired-size-gbs", metavar="N", type=long, required=False, default=80,
                        help="Desired tablet size (in GBs)")

    input_spec_group = parser.add_argument_group("input specification")

    input_spec_group.add_argument("--table", action="append", type=str,
                                  help="Add a single table to task list")
    input_spec_group.add_argument("--span", action="append", type=str,
                                  help="Rebalance specific tablet spans (e.g.: 143, 156-158)")

    input_spec_group.add_argument("--include", metavar="REGEXP", action="append", type=str,
                                  help="Add tables matching regular expression to task list")
    input_spec_group.add_argument("--exclude", metavar="REGEXP", action="append", type=str,
                                  help="Remove tables matching regular expression from task list")
    input_spec_group.set_defaults(table=[], include=[], exclude=[])

    oversized_group = parser.add_mutually_exclusive_group()
    oversized_group.add_argument("--oversized", dest="allow_oversized", action="store_true",
                                 help="Allow rebalancing algorithm to produce oversized tablets")
    oversized_group.add_argument("--no-oversized", dest="allow_oversized", action="store_false",
                                 help="Disallow rebalancing algorithm to produce oversized tablets")
    oversized_group.set_defaults(oversized=True)

    undersized_group = parser.add_mutually_exclusive_group()
    undersized_group.add_argument("--undersized", dest="allow_undersized", action="store_true",
                                  help="Allow rebalancing algorithm to produce undersized tablets")
    undersized_group.add_argument("--no-undersized", dest="allow_undersized", action="store_false",
                                  help="Disallow rebalancing algorithm to produce undersized tablets")
    undersized_group.set_defaults(undersized=False)

    parser.add_argument("--silent", action="store_true", help="Do not log anything")

    subparsers = parser.add_subparsers()

    build_logit_train_set_parser = subparsers.add_parser(
        "build_logit_train_set",
        help="Write out a train set of logit coefficients for further training")
    build_logit_train_set_parser.add_argument("--output", type=str, required=True,
                                              help="Output file")
    build_logit_train_set_parser.set_defaults(action="build_logit_train_set")

    improve_parser = subparsers.add_parser(
        "improve",
        help="Rebalance tables satisfying logit decision criteria")
    coefs = [
        -0.60321193,
        +5.4304166,
        -4.19723123,
        -7.20775292,
    ]
    improve_parser.add_argument("--logit-variance-coef", type=float, default=coefs[0],
                                help="Variance weight in decision criteria")
    improve_parser.add_argument("--logit-complexity-coef", type=float, default=coefs[1],
                                help="Complexity weight in decision criteria")
    improve_parser.add_argument("--logit-overdraft-coef", type=float, default=coefs[2],
                                help="Size overdraft weight in decision criteria")
    improve_parser.add_argument("--logit-underdraft-coef", type=float, default=coefs[3],
                                help="Size underdraft weight in decision criteria")
    improve_parser.add_argument("--yes", action="count",
                                help="Actually do something (do nothing by default)")
    improve_parser.set_defaults(action="improve")

    args = parser.parse_args()

    if args.silent:
        logging.basicConfig(level=logging.ERROR)
    else:
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    main(args)
