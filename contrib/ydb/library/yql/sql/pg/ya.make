LIBRARY()

PROVIDES(
    yql_pg_sql_translator
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/public/api/protos
)

ADDINCL(
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

SRCS(
    pg_sql.cpp
    optimizer.cpp
    utils.cpp
)

CFLAGS(
    -Dpalloc0=yql_palloc0
    -Dpfree=yql_pfree
)

IF (OS_WINDOWS)
CFLAGS(
   "-D__thread=__declspec(thread)"
   -Dfstat=microsoft_native_fstat
   -Dstat=microsoft_native_stat
)
ENDIF()

NO_COMPILER_WARNINGS()
YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
