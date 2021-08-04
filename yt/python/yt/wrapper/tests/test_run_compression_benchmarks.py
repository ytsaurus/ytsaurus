from .conftest import authors
from .helpers import TEST_DIR

import yt.wrapper as yt

import yt.wrapper.run_compression_benchmarks as yt_run_compression_benchmarks

import pytest

@pytest.mark.usefixtures("yt_env")
class TestRunCompressionBenchmarks(object):
    ROWS_COUNT = 10**6
    TIME_LIMIT_SEC = 3
    MAX_OPERATIONS = 3
    SAMPLE_SIZE = 10**6
    CODECS = {
        "lz4",
        "lz4_high_compression",
        "none",
        "snappy",
        "brotli_1",
        "brotli_11",
        "bzip2_1",
        "lzma_0",
        "zlib_1",
        "zstd_1"
    }
    
    @authors("egor-gutrov")
    def test_run_compression_benchmarks(self):
        table = TEST_DIR + "/compression_table"
        yt.write_table(table, [{"x": i, "y": str(i)} for i in range(self.ROWS_COUNT)])
        results = yt_run_compression_benchmarks.run(
            table,
            time_limit_sec=self.TIME_LIMIT_SEC,
            max_operations=self.MAX_OPERATIONS,
            sample_size=self.SAMPLE_SIZE,
            all_codecs=False,
        )

        for codec in self.CODECS:
            assert codec in results

        assert results["brotli_11"]["cpu_write"] == "Timed out"
        assert results["brotli_11"]["cpu_read"] == "Not launched"

    @authors("egor-gutrov")
    def test_empty_table(self):
        table = TEST_DIR + "/empty_table"
        yt.create("table", table)
        with pytest.raises(yt.YtError):
            yt_run_compression_benchmarks.run(
                table,
                time_limit_sec=self.TIME_LIMIT_SEC,
                max_operations=self.MAX_OPERATIONS,
                sample_size=self.SAMPLE_SIZE,
                all_codecs=False,
            )
