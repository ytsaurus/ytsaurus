UNITTEST_FOR(contrib/ydb/library/yql/dq/opt)

SRCS(
    dq_cbo_ut.cpp
    dq_opt_hypergraph_ut.cpp
    dq_opt_interesting_orderings_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/opt
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
