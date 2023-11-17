LIBRARY()

SRCS(
    arrow_util.cpp
    mkql_functions.cpp
    mkql_memory_pool.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf/arrow
)

IF (MKQL_RUNTIME_VERSION)
    CFLAGS(-DMKQL_RUNTIME_VERSION=$MKQL_RUNTIME_VERSION)
ENDIF()

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
