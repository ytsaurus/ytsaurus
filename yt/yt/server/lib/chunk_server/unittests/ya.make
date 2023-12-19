GTEST(unittester-server-lib-chunk-server)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    immutable_chunk_meta_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/server/lib/chunk_server
)

SIZE(SMALL)

END()
