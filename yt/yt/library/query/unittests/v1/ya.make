GTEST(unittester-query-v1)

ALLOCATOR(TCMALLOC)

SRCS(
    init.cpp
)

PEERDIR(
    yt/yt/library/query/unittests
)

FORK_SUBTESTS(MODULO)

IF (SANITIZER_TYPE)
    SPLIT_FACTOR(10)
ELSE()
    SPLIT_FACTOR(3)
ENDIF()

SIZE(MEDIUM)

END()
