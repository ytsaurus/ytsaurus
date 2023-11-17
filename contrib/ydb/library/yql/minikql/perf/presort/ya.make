PROGRAM()

PEERDIR(
    library/cpp/presort
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    presort.cpp
)

END()
