UNITTEST_FOR(contrib/ydb/library/yql/dq/comp_nodes)

PEERDIR(
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/comp_nodes/ut/utils
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    contrib/ydb/core/kqp/tools/join_perf
    contrib/ydb/core/kqp/runtime

    library/cpp/testing/unittest
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry

)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

YQL_LAST_ABI_VERSION()

SRCS(

    dq_hash_combine_ut.cpp
    dq_hash_join_ut.cpp
)

END()
