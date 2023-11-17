UNITTEST_FOR(contrib/ydb/library/yql/parser/pg_catalog)

SRCS(
    catalog_ut.cpp
    catalog_consts_ut.cpp
)

ADDINCL(
    contrib/ydb/library/yql/parser/pg_wrapper/postgresql/src/include
)

PEERDIR(
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(
        -DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION
    )
ENDIF()

END()
