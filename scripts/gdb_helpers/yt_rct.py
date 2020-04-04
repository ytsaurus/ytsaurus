from __future__ import print_function

import argparse
import gdb
import re
import shlex

def extract_from_vector(vector):
    start = vector["_M_impl"]["_M_start"]
    finish = vector["_M_impl"]["_M_finish"]
    result = []
    while start < finish:
        result.append(start.dereference())
        start += 1
    return result


def extract_from_hash_set_node(node):
    uintptr_t = gdb.lookup_type("uintptr_t")
    to_long = lambda v: long(v.cast(uintptr_t))

    while to_long(node):
        if to_long(node) & 1:
            break
        yield node["val"]
        node = node["next"]


def extract_from_hash_set(hash_set):
    buckets = hash_set["rep"]["buckets"]
    return [item
            for node in extract_from_vector(buckets)
            for item in extract_from_hash_set_node(node)]


class RCTSlot(object):
    def __init__(self, oa=0, ba=0, of=0, bf=0):
        self.objects_allocated = oa
        self.bytes_allocated = ba
        self.objects_freed = of
        self.bytes_freed = bf

    def __iadd__(self, other):
        self.objects_allocated += other.objects_allocated
        self.bytes_allocated += other.bytes_allocated
        self.objects_freed += other.objects_freed
        self.bytes_freed += other.bytes_freed
        return self

    @property
    def objects_alive(self):
        return self.objects_allocated - self.objects_freed

    @property
    def bytes_alive(self):
        return self.bytes_allocated - self.bytes_freed

    def __repr__(self):
        return "<RCTSlot Objects={{Allocated={}, Freed={}}} Bytes={{Allocated={}, Freed={}}}>".format(
            self.objects_allocated, self.objects_freed,
            self.bytes_allocated, self.bytes_freed)


class YtRefCountedTracker(gdb.Command):
    NAME = "yt_rct"

    @staticmethod
    def exit_handler(status=0, message=None):
        raise gdb.GdbError(message or "Aborting")

    @staticmethod
    def error_handler(message):
        raise gdb.GdbError("ERROR: {}".format(message))

    def __init__(self):
        gdb.Command.__init__(self, self.NAME, gdb.COMMAND_STACK, gdb.COMPLETE_SYMBOL)
        self.parser = argparse.ArgumentParser(self.NAME)
        self.parser.error = self.error_handler
        self.parser.exit = self.exit_handler
        self.parser.add_argument("-f", dest="filter", help="Optional regexp to filter types")
        self.parser.add_argument("-s", dest="sort_by", choices=("o", "b"), default="b",
                                 help="Sort by object (o) or by bytes (b)")
        self.parser.add_argument("-a", dest="allocated", action="store_true", default=False,
                                 help="Show allocated counters instead of alive counters")
        self.parser.add_argument("-r", dest="reverse", action="store_false", default=True,
                                 help="Show counters in ascending order")
        self.parser.add_argument("-z", dest="zero", action="store_true", default=False,
                                 help="Show rows with zero counters")

    def invoke(self, arg, _from_tty):
        opts = self.parser.parse_args(shlex.split(arg))

        val = gdb.parse_and_eval("NYT::RefCountedTrackerInstance")
        if val == 0:
            self.error_handler("RCT is nullptr")

        keys = extract_from_vector(val["CookieToKey_"])
        holders = extract_from_hash_set(val["PerThreadHolders_"])
        result = [RCTSlot() for _ in keys]

        for holder in holders:
            statistics = extract_from_vector(holder["Statistics_"])
            for i, statistic in enumerate(statistics):
                if i < len(result):
                    result[i] += RCTSlot(
                        statistic["ObjectsAllocated_"],
                        statistic["BytesAllocated_"],
                        statistic["ObjectsFreed_"],
                        statistic["BytesFreed_"])

        for i, key in enumerate(keys):
            nice_key = str(key["TypeKey"])
            nice_key = nice_key[nice_key.index(" ") + 1:]  # strip address
            nice_key = nice_key[14:-1]
            result[i] = (nice_key, result[i])

        if opts.allocated:
            key_hdr = "Allocated"
            objects_fn = lambda row: row[1].objects_allocated
            bytes_fn = lambda row: row[1].bytes_allocated
        else:
            key_hdr = "Alive"
            objects_fn = lambda row: row[1].objects_alive
            bytes_fn = lambda row: row[1].bytes_alive

        if opts.sort_by == "o":
            key_fn = objects_fn
        if opts.sort_by == "b":
            key_fn = bytes_fn

        if not opts.zero:
            result = filter(lambda row: key_fn(row) > 0, result)

        if opts.filter:
            result = filter(lambda row: re.match(opts.filter, row[0]), result)

        result = sorted(result, key=key_fn, reverse=opts.reverse)

        # Compute totals.
        total_by_objects = sum(objects_fn(row) for row in result)
        total_by_bytes = sum(bytes_fn(row) for row in result)

        # Now, format result.
        screen_width = gdb.parameter("width")
        right_width = 42
        if opts.sort_by == "o":
            right_fmt = " {objects_val:>20} {bytes_val:>20}"
        if opts.sort_by == "b":
            right_fmt = " {bytes_val:>20} {objects_val:>20}"
        left_width = max(0, screen_width - right_width - 2)
        left_fmt = "{ty:<" + str(left_width) + "}" + " |"

        fmt = left_fmt + right_fmt
        dmt = "-" * (left_width + 1) + "+" + "-" * right_width

        print(fmt.format(ty="", objects_val="Objects {}".format(key_hdr), bytes_val="Bytes {}".format(key_hdr)))
        print(dmt)
        print(fmt.format(ty="Total", objects_val=total_by_objects, bytes_val=total_by_bytes))
        print(dmt)
        for row in result:
            ty = row[0]
            occupied_lines = 1 + len(ty) / left_width
            for i in range(occupied_lines - 1):
                print(fmt.format(ty=ty[i * left_width:(i + 1) * left_width], objects_val="", bytes_val=""))
            print(fmt.format(ty=ty[(occupied_lines - 1) * left_width:],
                             objects_val=objects_fn(row), bytes_val=bytes_fn(row)))

YtRefCountedTracker()
