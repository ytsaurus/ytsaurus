GTEST(unittester-ytlib-table-chunk-format)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    column_meta_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/client/table_client/unittests/helpers
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/ytlib
)

FORK_SUBTESTS(MODULO)

SPLIT_FACTOR(5)

SIZE(MEDIUM)

END()
