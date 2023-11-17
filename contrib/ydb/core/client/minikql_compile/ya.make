LIBRARY()

SRCS(
    compile_context.cpp
    compile_context.h
    compile_result.cpp
    compile_result.h
    db_key_resolver.cpp
    db_key_resolver.h
    mkql_compile_service.cpp
    yql_expr_minikql.cpp
    yql_expr_minikql.h
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/threading/future
    contrib/ydb/core/base
    contrib/ydb/core/engine
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/scheme
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
