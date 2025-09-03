GTEST(unittester-query-v2)

ALLOCATOR(TCMALLOC)

SRCS(
    init.cpp
    ql_cast_ut.cpp
    ql_nested_subqueries_ut.cpp
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
