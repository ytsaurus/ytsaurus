UNITTEST_FOR(contrib/ydb/library/yql/dq/comp_nodes)

PEERDIR(
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/comp_nodes/ut/utils
    contrib/ydb/library/yql/dq/comp_nodes/ut/join_perf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)

IF (SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

YQL_LAST_ABI_VERSION()

SRCS(
    dq_hash_combine_ut.cpp
    dq_hash_join_ut.cpp
)

END()
