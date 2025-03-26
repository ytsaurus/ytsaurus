UNITTEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
    REQUIREMENTS(
        ram:32
    )
ELSE()
    SIZE(MEDIUM)
ENDIF()

IF (BUILD_TYPE == "RELEASE" OR BUILD_TYPE == "RELWITHDEBINFO")
    SRCS(
        huge_cluster.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/ut/lib
    contrib/ydb/library/actors/interconnect/ut/protos
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
)

END()
