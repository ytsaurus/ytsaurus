LIBRARY()

SRCS(
    yql_pq_gateway_factory.cpp
    yql_pq_gateway_services.cpp
    yql_pq_gateway.cpp
    yql_pq_session.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/pq/cm_client
    contrib/ydb/library/yql/providers/pq/gateway/abstract
    contrib/ydb/library/yql/providers/pq/gateway/clients/external
    contrib/ydb/library/yql/providers/pq/gateway/clients/local
    contrib/ydb/library/yverify_stream
    contrib/ydb/public/sdk/cpp/src/client/datastreams
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic
    yql/essentials/providers/common/metrics
    yql/essentials/providers/common/proto
    yql/essentials/utils
    yql/essentials/utils/log
)

END()
