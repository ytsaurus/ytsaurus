LIBRARY()

SRCS(
    yql_pq_gateway.cpp
    yql_pq_session.cpp
)

PEERDIR(
    yql/essentials/providers/common/metrics
    yql/essentials/providers/common/proto
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/pq/cm_client
    contrib/ydb/library/yql/providers/pq/provider
    yql/essentials/utils
    contrib/ydb/public/sdk/cpp/src/client/datastreams
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/federated_topic
    contrib/ydb/public/sdk/cpp/src/client/topic
)

END()
