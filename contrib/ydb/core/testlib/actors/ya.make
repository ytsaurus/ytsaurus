LIBRARY()

SRCS(
    block_events.cpp
    block_events.h
    test_runtime.cpp
    test_runtime.h
    wait_events.cpp
    wait_events.h
)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/testlib
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/mon
    contrib/ydb/core/mon_alloc
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/testlib/audit_helpers
)

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
