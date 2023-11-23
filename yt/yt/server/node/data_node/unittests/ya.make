GTEST(unittester-data-node)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    p2p_ut.cpp
    journal_manager_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/server/node
    yt/yt/server/tools
)

DEPENDS(
    yt/yt/server/tools/bin
)

EXPLICIT_DATA()

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

TAG(
    ya:yt
)

ENV(ASAN_OPTIONS="detect_leaks=0")

YT_SPEC(yt/yt/tests/integration/spec.yson)

SIZE(MEDIUM)

DATA(arcadia/yt/yt/server/node/data_node/unittests/testdata)

END()
