GTEST(unittester-discovery-server)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    discovery_service_ut.cpp
    distributed_throttler_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/discovery_server
    yt/yt/server/lib/discovery_server/unittests/mock
    yt/yt/ytlib/distributed_throttler
    yt/yt/core/test_framework
)

SIZE(MEDIUM)

END()
