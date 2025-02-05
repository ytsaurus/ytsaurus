LIBRARY()

NO_WSHADOW()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

IF (ALLOCATOR == "B" OR ALLOCATOR == "BS" OR ALLOCATOR == "C")
    CXXFLAGS(-DBALLOC)
    PEERDIR(
        library/cpp/balloc/optional
    )
ENDIF()

SRCS(
    cpu_consumption.cpp
    pool.cpp
    shared_info.cpp
    waiting_stats.cpp
    harmonizer.cpp
)

PEERDIR(
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/protos
    contrib/ydb/library/services
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/monlib/dynamic_counters
    library/cpp/time_provider
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        ../tsan.supp
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
