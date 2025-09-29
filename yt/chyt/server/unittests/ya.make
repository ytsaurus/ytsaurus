GTEST(unittester-clickhouse-server)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)
INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

ALLOCATOR(TCMALLOC)

IF (AUTOCHECK)
    ENV(SKIP_CHYT_CONVERSION_BENCHMARK_TESTS=1)
ENDIF()

IF (DISTBUILD)
    ENV(SKIP_CHYT_CONVERSION_BENCHMARK_TESTS=1)
ENDIF()

SRCS(
    computed_columns_ut.cpp
    ch_to_yt_converter_ut.cpp
    helpers.cpp
    read_range_inference_ut.cpp
    framework.cpp
    yt_to_ch_converter_ut.cpp
)

PEERDIR(
    library/cpp/testing/hook
    yt/yt/build
    yt/yt/core/test_framework
    yt/chyt/server
)

SIZE(SMALL)

END()
