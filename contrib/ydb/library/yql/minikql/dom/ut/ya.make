IF (NOT WINDOWS)
    UNITTEST_FOR(contrib/ydb/library/yql/minikql/dom)

    SRCS(
        yson_ut.cpp
        json_ut.cpp
    )

    SIZE(MEDIUM)

    PEERDIR(
        contrib/ydb/library/yql/minikql/computation/llvm
        contrib/ydb/library/yql/public/udf/service/exception_policy
        contrib/ydb/library/yql/sql/pg_dummy
    )

    YQL_LAST_ABI_VERSION()

    END()
ENDIF()
