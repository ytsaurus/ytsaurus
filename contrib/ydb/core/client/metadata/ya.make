LIBRARY()

SRCS(
    types_metadata.cpp
    functions_metadata.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/scheme_types
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
