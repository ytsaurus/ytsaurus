UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/worker_manager)

SRCS(
    local_worker_manager_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/providers/dq/actors
    library/cpp/testing/unittest
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
