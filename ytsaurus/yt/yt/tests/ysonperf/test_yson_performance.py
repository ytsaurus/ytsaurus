from yt_env_setup import is_asan_build, is_msan_build

from yt_commands import print_debug

import yt.yson as yson

import os
import pytest


def is_debug():
    try:
        from yson_lib import is_debug_build
    except ImportError:
        from yt_yson_bindings.yson_lib import is_debug_build

    return is_debug_build()


def _benchmark_loads(benchmark, dataset_path):
    raw_data = open(dataset_path, "rb").read()

    def loads(raw_data):
        return list(yson.loads(raw_data, yson_type="list_fragment", always_create_attributes=False, encoding=None))

    return benchmark(loads, raw_data)


def _benchmark_dumps(benchmark, dataset_path):
    raw_data = open(dataset_path, "rb").read()
    data = list(yson.loads(raw_data, yson_type="list_fragment", always_create_attributes=False, encoding=None))
    return benchmark(yson.dumps, data, yson_type="list_fragment", yson_format="binary", ignore_inner_attributes=True)


@pytest.mark.benchmark(
    disable_gc=True,
    min_rounds=5,
    warmup=3,
)
class TestYsonPerformance(object):
    ACCEPTABLE_TIME_GROW_RATIO = 0.20
    DATASETS_PATH = "testdata"

    @pytest.mark.parametrize(
        "benchmark_function,dataset,expected_time",
        [
            (_benchmark_loads, "access_log.yson", 3.5),
            (_benchmark_dumps, "access_log.yson", 4.0),
            (_benchmark_loads, "numbers.yson", 2.0),
            (_benchmark_dumps, "numbers.yson", 2.5),
        ],
    )
    def test_yson_performance(self, benchmark, benchmark_function, dataset, expected_time):
        if is_debug() or is_asan_build() or is_msan_build() or yson.TYPE != "BINARY":
            pytest.skip()

        dataset_path = os.path.join(self.DATASETS_PATH, dataset)
        benchmark_function(benchmark, dataset_path)
        print_debug(benchmark_function, dataset, benchmark.stats["mean"])
        assert benchmark.stats["mean"] <= (1.0 + self.ACCEPTABLE_TIME_GROW_RATIO) * expected_time
