UNITTEST_FOR(yt/yql/providers/dq/actors/execution)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/providers/dq/actors
    library/cpp/testing/unittest
    library/cpp/time_provider
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    actors_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
