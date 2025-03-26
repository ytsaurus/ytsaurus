LIBRARY()

SRCS(
    types_metadata.cpp
    functions_metadata.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/scheme_types
    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
