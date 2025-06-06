UNITTEST_FOR(contrib/ydb/core/config/tools/protobuf_plugin)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/core/config/tools/protobuf_plugin/ut/protos
)

SRCS(
    ut.cpp
)

END()
