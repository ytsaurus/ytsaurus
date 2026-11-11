GTEST(unittester-query-v2)

ALLOCATOR(TCMALLOC)

SRCS(
    init.cpp
    ql_cast_ut.cpp
    ql_hierarchical_join_ut.cpp
    ql_nested_subqueries_ut.cpp
    ql_union_all_ut.cpp
)

PEERDIR(
    yt/yt/library/query/unittests
    yt/yt/library/query/proto
)

FORK_SUBTESTS(MODULO)

IF (SANITIZER_TYPE)
    SPLIT_FACTOR(10)
ELSE()
    SPLIT_FACTOR(5)
ENDIF()

SIZE(MEDIUM)

END()
