UNITTEST_FOR(contrib/ydb/library/yql/sql/pg)

SRCS(
    pg_sql_ut.cpp
    pg_sql_autoparam_ut.cpp
    optimizer_ut.cpp
    optimizer_impl_ut.cpp
)

ADDINCL(
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/libs/fmt

    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/parser/pg_wrapper
)

SIZE(MEDIUM)

NO_COMPILER_WARNINGS()

END()
