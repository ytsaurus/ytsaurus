LIBRARY()

SRCS(
    metadata_helpers.cpp
    scheme_helpers.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/kqp/gateway/actors
    contrib/ydb/core/kqp/provider
    contrib/ydb/core/protos
    contrib/ydb/core/protos/schemeshard
)

YQL_LAST_ABI_VERSION()

END()
