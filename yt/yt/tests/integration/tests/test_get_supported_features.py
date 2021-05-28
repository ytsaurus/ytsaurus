import __builtin__

from yt_env_setup import YTEnvSetup
from yt_commands import *

##################################################################


class TestGetFeatures(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1

    @authors("levysotsky")
    def test_get_features(self):
        driver = get_driver(api_version=4)
        features = get_supported_features(driver=driver)

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
        }
        assert expected_types == expected_types.intersection(__builtin__.set(features["primitive_types"]))

        assert "compression_codecs" in features
        expected_compression_codecs = {
            "none",
            "snappy",
            "lz4",
            "lz4_high_compression",
            "quick_lz",
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
            __builtin__.set(features["compression_codecs"])
        )
        deprecated_compression_codecs = {"zlib6", "gzip_best_compression", "brotli8"}
        assert not deprecated_compression_codecs.intersection(__builtin__.set(features["compression_codecs"]))
        assert len(features["compression_codecs"]) == len(__builtin__.set(features["compression_codecs"]))

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
            __builtin__.set(features["erasure_codecs"])
        )
