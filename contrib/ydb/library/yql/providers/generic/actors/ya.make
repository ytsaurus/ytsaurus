LIBRARY()

SRCS(
    yql_generic_read_actor.cpp
    yql_generic_lookup_actor.cpp
    yql_generic_provider_factories.cpp
    yql_generic_token_provider.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation
    yql/essentials/providers/common/structured_token
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/generic/proto
    yql/essentials/public/types
    contrib/ydb/library/yql/providers/generic/connector/libcpp
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
