Y_BENCHMARK(jsonpath-benchmark)

PEERDIR(
    library/cpp/json
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
    contrib/ydb/library/yql/minikql/jsonpath
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
