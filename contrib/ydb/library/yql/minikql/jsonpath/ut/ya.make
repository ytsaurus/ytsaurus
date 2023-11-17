UNITTEST_FOR(contrib/ydb/library/yql/minikql/jsonpath)



SRCS(
    common_ut.cpp
    examples_ut.cpp
    lax_ut.cpp
    strict_ut.cpp
    test_base.cpp
    lib_id_ut.cpp
)

PEERDIR(
    library/cpp/json
    contrib/ydb/library/binary_json
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
