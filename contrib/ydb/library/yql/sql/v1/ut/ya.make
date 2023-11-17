UNITTEST_FOR(contrib/ydb/library/yql/sql/v1)

SRCS(
    sql_ut.cpp
    sql_match_recognize_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/core/sql_types
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/sql/v1/format
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
