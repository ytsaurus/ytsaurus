#!/usr/bin/env python

import argparse
import copy
import gc
import os
import psutil
import sys
import time

import yt.yson as yson
assert yson.TYPE == "BINARY"


class Timer:
    def __init__(self, print_stats=True):
        self.print_stats = print_stats
        self._running = False
        self._extra_info = None
        self._process = psutil.Process()
        self._process.set_cpu_affinity([0])

    def start(self):
        assert not self._running
        self._running = True

        gc.disable()
        gc.collect()

        self._start_memory_info = self._process.get_memory_info()

        self._start_gc_counts = gc.get_count()
        self._start_cpu_times = self._process.get_cpu_times()
        self._start_wall_clock = time.clock()

    def stop(self):
        assert self._running
        self._running = False

        self._finish_wall_clock = time.clock()
        self._finish_cpu_times = self._process.get_cpu_times()
        self._finish_gc_counts = gc.get_count()

        self._before_gc_memory_info = self._process.get_memory_info()
        gc.collect()
        self._after_gc_memory_info = self._process.get_memory_info()

        gc.enable()

    def set_extra_info(self, extra_info):
        self._extra_info = extra_info

    def dump_stats(self):
        MB = 1.0 * 1024.0 * 1024.0

        def dump_stats_line(key, value):
            print "  {:>40s} | {}".format(key, value)

        print "=== {}".format(self._extra_info or repr(self))
        dump_stats_line(
            "wall clock time",
            "{:.3f}s".format(self._finish_wall_clock - self._start_wall_clock))
        dump_stats_line(
            "user cpu time",
            "{:.3f}s".format(self._finish_cpu_times.user - self._start_cpu_times.user))
        dump_stats_line(
            "system cpu time",
            "{:.3f}s".format(self._finish_cpu_times.system - self._start_cpu_times.system))
        dump_stats_line(
            "rss memory (before GC)",
            "{:+.3f} MB".format((self._before_gc_memory_info.rss - self._start_memory_info.rss) / MB))
        dump_stats_line(
            "rss memory (after GC)",
            "{:+.3f} MB".format((self._after_gc_memory_info.rss - self._start_memory_info.rss) / MB))
        dump_stats_line(
            "garbage memory",
            "{:.3f} MB".format((self._after_gc_memory_info.rss - self._before_gc_memory_info.rss) / MB))
        dump_stats_line(
            "gc generation counts",
            "{:+d} G0 / {:+d} G1 / {:+d} G2".format(
                self._finish_gc_counts[0] - self._start_gc_counts[0],
                self._finish_gc_counts[1] - self._start_gc_counts[1],
                self._finish_gc_counts[2] - self._start_gc_counts[2]))
        print "==="

    def __enter__(self):
        if not self._running:
            self.start()

        return self

    def __exit__(self, type, value, traceback):
        if self._running:
            self.stop()

        if self.print_stats:
            self.dump_stats()


def benchmark(filename, perf=None, stamp=None):
    def perf_step():
        if perf:
            try:
                raw_input("Press any key to continue...")
            except KeyboardInterrupt:
                os._exit(1)

    if perf:
        print "Here are a couple of useful profiling commands:"
        print ""
        print "  perf record -p {} --call-graph dwarf -o 'perf.data.{}.{}'".format(
            os.getpid(), os.path.basename(filename), stamp)
        print "  perf top -p {} -z -d 1 --call-graph dwarf".format(
            os.getpid())
        print ""

    with Timer() as t:
        with open(filename, "rb") as handle:
            raw_data = handle.read()
        t.set_extra_info("%s: read %d bytes" % (filename, len(raw_data)))

    perf_step()

    with Timer() as t:
        loaded = yson.loads(raw_data, yson_type="list_fragment", always_create_attributes=True)
        loaded = list(loaded)
        t.set_extra_info("%s: loaded %d rows" % (filename, len(loaded)))

    perf_step()

    with Timer() as t:
        dumped = yson.dumps(loaded, yson_type="list_fragment", yson_format="binary", ignore_inner_attributes=True)
        t.set_extra_info("%s: dumped %d bytes" % (filename, len(dumped)))

    perf_step()


def print_sizeof_information():
    def printer(ctor):
        print "{:>4s}: {}".format(str(ctor), sys.getsizeof(ctor()))

    print "=== native object size"
    map(printer, [int, long, float, str, list, dict])

    print "=== yson object size"
    map(printer, [yson.YsonInt64, yson.YsonUint64, yson.YsonDouble,
                  yson.YsonString, yson.YsonList, yson.YsonMap])


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", type=str, nargs="*", help="input data")
    parser.add_argument("--perf", action="store_true", default=False,
                        help="perf mode, adds pauses between stages to aid profiling")
    parser.add_argument("--stamp", type=str, default=time.strftime("%Y%m%d_%H%M%S"),
                        help="stamp to uniquely identify particular invocation")

    args = parser.parse_args()

    if len(args.filename) == 0:
        print_sizeof_information()
    else:
        for filename in args.filename:
            benchmark(filename, perf=args.perf, stamp=args.stamp)


if __name__ == "__main__":
    main()
