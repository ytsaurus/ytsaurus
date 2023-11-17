UNITTEST_FOR(contrib/ydb/library/yql/sql/v0)

SRCS(
    sql_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
