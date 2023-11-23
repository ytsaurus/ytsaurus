GTEST(unittester-library-clickhouse-discovery)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(YT)

SRCS(
    discovery_ut.cpp
    helpers_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/build
    yt/yt/client/unittests/mock
    yt/yt/core
    yt/yt/core/test_framework
    yt/yt/library/clickhouse_discovery
)

SIZE(MEDIUM)

END()
