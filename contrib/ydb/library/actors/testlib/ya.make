LIBRARY()

SRCS(
    scoped_allocation_cache.h
    test_runtime.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect/mock
    contrib/ydb/library/actors/protos
    contrib/ydb/library/actors/testlib/common
    library/cpp/random_provider
    library/cpp/time_provider
)

IF (GCC)
    CFLAGS(-fno-devirtualize-speculatively)
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
