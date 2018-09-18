from yt_commands import is_debug

from yt.wrapper.client import YtClient

import yt.yson as yson

from flaky import flaky

import gc
import os
import sys
import pytest
import psutil
import time

class Timer(object):
    def __init__(self):
        self._running = False
        self._extra_info = None
        self._process = psutil.Process()
        self._process.cpu_affinity([0])

    def start(self):
        assert not self._running
        self._running = True

        gc.disable()
        gc.collect()

        self.start_memory_info = self._process.memory_info()
        self.start_cpu_times = self._process.cpu_times()
        self.start_wall_clock = time.clock()

    def stop(self):
        assert self._running
        self._running = False

        self.finish_wall_clock = time.clock()
        self.finish_cpu_times = self._process.cpu_times()

        gc.collect()
        gc.enable()

    def set_extra_info(self, extra_info):
        self._extra_info = extra_info

    def dump_stats(self):
        def dump_stats_line(key, value):
            print >>sys.stderr, "  {0:>30s} | {1}".format(key, value)

        print >>sys.stderr, "\n=== {0}".format(self._extra_info or repr(self))
        dump_stats_line(
            "wall clock time",
            "{0:.3f}s".format(self.finish_wall_clock - self.start_wall_clock))
        dump_stats_line(
            "user cpu time",
            "{0:.3f}s".format(self.finish_cpu_times.user - self.start_cpu_times.user))
        dump_stats_line(
            "system cpu time",
            "{0:.3f}s".format(self.finish_cpu_times.system - self.start_cpu_times.system))

    def __enter__(self):
        if not self._running:
            self.start()
        return self

    def __exit__(self, type, value, traceback):
        if self._running:
            self.stop()
        self.dump_stats()

class TestYsonPerformance(object):
    DATASETS_PATH = "//home/files/test_data/yson"
    DATASETS_CLUSTER = "locke"

    ACCEPTABLE_TIME_GROW_RATIO = 0.20
    ITERATIONS_COUNT = 5

    @classmethod
    def setup_class(cls):
        cls.client = YtClient(proxy=cls.DATASETS_CLUSTER,
                              token=os.environ.get("TEAMCITY_YT_TOKEN"))

    @flaky(max_runs=5)
    @pytest.mark.parametrize("dataset,expected_loads_time,expected_dumps_time", [
        ("access_log", 5.0, 4.0),
        ("numbers", 2.5, 2.5)
    ])
    def test_yson_performance(self, dataset, expected_loads_time, expected_dumps_time):
        if is_debug() or yson.TYPE != "BINARY":
            pytest.skip()

        dataset_path = os.path.join(self.DATASETS_PATH, dataset)
        raw_data = self.client.read_table(dataset_path, format="yson", raw=True).read()

        loads_results = []
        dumps_results = []

        for iteration in xrange(self.ITERATIONS_COUNT):
            with Timer() as t:
                loaded = list(yson.loads(raw_data, yson_type="list_fragment", always_create_attributes=False))
                t.set_extra_info("{0}: loaded {1} rows".format(dataset, len(loaded)))
            loads_results.append(t.finish_wall_clock - t.start_wall_clock)

            with Timer() as t:
                dumped = yson.dumps(loaded, yson_type="list_fragment", yson_format="binary", ignore_inner_attributes=True)
                t.set_extra_info("{0}: dumped {1} bytes".format(dataset, len(dumped)))
            dumps_results.append(t.finish_wall_clock - t.start_wall_clock)

        min_loads_time = min(loads_results)
        min_dumps_time = min(dumps_results)

        assert min_loads_time <= (1.0 + self.ACCEPTABLE_TIME_GROW_RATIO) * expected_loads_time
        assert min_dumps_time <= (1.0 + self.ACCEPTABLE_TIME_GROW_RATIO) * expected_dumps_time
