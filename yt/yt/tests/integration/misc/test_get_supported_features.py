from yt_env_setup import YTEnvSetup

from yt_commands import authors, get_driver, get_supported_features

import builtins

import pytest

##################################################################


class TestGetFeatures(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1
    SKIP_STATISTICS_DESCRIPTIONS = False

    @authors("levysotsky")
    def test_get_features(self):
        driver = get_driver(api_version=4)
        features = get_supported_features(driver=driver)

        is_compat = "23_2" in getattr(self, "ARTIFACT_COMPONENTS", {})
        if is_compat:
            return

        assert "primitive_types" in features
        expected_types = {
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
            "string",
            "yson",
            "utf8",
            "json",
            "float",
            "double",
            "void",
            "null",
            "date",
            "datetime",
            "timestamp",
            "interval",
            "date32",
            "datetime64",
            "timestamp64",
            "interval64",
        }
        assert expected_types == expected_types.intersection(builtins.set(features["primitive_types"]))

        assert "compression_codecs" in features
        expected_compression_codecs = {
            "none",
            "snappy",
            "lz4",
            "lz4_high_compression",
            "brotli_1",
            "brotli_11",
            "zlib_1",
            "zlib_9",
            "zstd_1",
            "zstd_21",
            "lzma_0",
            "lzma_9",
            "bzip2_1",
            "bzip2_9",
        }
        assert expected_compression_codecs == expected_compression_codecs.intersection(
            builtins.set(features["compression_codecs"])
        )
        deprecated_compression_codecs = {"zlib6", "gzip_best_compression", "brotli8"}
        assert not deprecated_compression_codecs.intersection(builtins.set(features["compression_codecs"]))
        assert len(features["compression_codecs"]) == len(builtins.set(features["compression_codecs"]))

        assert "erasure_codecs" in features
        expected_erasure_codecs = {
            "none",
            "reed_solomon_6_3",
            "reed_solomon_3_3",
            "isa_reed_solomon_6_3",
            "lrc_12_2_2",
            "isa_lrc_12_2_2",
        }
        assert expected_erasure_codecs == expected_erasure_codecs.intersection(
            builtins.set(features["erasure_codecs"])
        )

    @authors("egor-gutrov")
    def test_operation_statistics_descriptions(self):
        if self.SKIP_STATISTICS_DESCRIPTIONS:
            pytest.skip("Statistics descriptions are retrieved from scheduler, but its' version is too old")
        driver = get_driver(api_version=4)
        features = get_supported_features(driver=driver)

        assert "operation_statistics_descriptions" in features
        expected_statistics_descriptions = {
            "time/total",
            "data/input/row_count",
            "data/output/*/data_weight",
            "exec_agent/traffic/inbound/from_*",
            "exec_agent/artifacts/cache_bypassed_artifacts_size",
            "job_proxy/cpu/system",
            "job_proxy/traffic/*_to_*",
            "user_job/cumulative_memory_reserve",
            "user_job/cpu/wait",
            "user_job/block_io/io_write",
            "user_job/pipes/output/*/bytes",
            "codec/cpu/decode/*",
            "codec/cpu/encode/*/*",
        }
        assert expected_statistics_descriptions == expected_statistics_descriptions.intersection(
            builtins.set(features["operation_statistics_descriptions"])
        )
        for desc_name in expected_statistics_descriptions:
            description = features["operation_statistics_descriptions"][desc_name]
            assert "description" in description
            assert "unit" in description
