GTEST(unittester-data-node)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    p2p_ut.cpp
    journal_manager_ut.cpp
    data_node_service_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/node
    yt/yt/server/tools

    yt/yt/ytlib/test_framework
    yt/yt/core/test_framework
)

DEPENDS(
    yt/yt/server/tools/bin
)

EXPLICIT_DATA()

TAG(
    ya:yt
    ya:fat
    ya:large_tests_on_single_slots
)

ENV(ASAN_OPTIONS="detect_leaks=0")

YT_SPEC(yt/yt/tests/integration/spec.yson)

SIZE(LARGE)

DATA(arcadia/yt/yt/server/node/data_node/unittests/testdata)

END()
