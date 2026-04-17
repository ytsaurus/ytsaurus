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
    stats.cpp
)

PEERDIR(
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/protos
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(
        ../tsan.supp
    )
ENDIF()

END()

