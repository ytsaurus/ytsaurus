LIBRARY()

SRCS(
    dq_warmup.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/svnversion
    library/cpp/threading/future
    yql/essentials/core/file_storage
    yql/essentials/utils/log
    yt/yql/providers/dq/common
    yt/yql/providers/dq/config
)

IF (NOT OPENSOURCE AND NOT OS_WINDOWS)
    SRCS(
        dq_clique_warmup_session.cpp
        yql_dq_clique_routing_gateway.cpp
    )

    PEERDIR(
        yt/yql/providers/dq/actors/yt
        yt/yql/providers/dq/clique_discovery
        yt/yql/providers/dq/gateway
    )
ELSE()
    SRCS(
        dq_clique_warmup_session_stub.cpp
        yql_dq_clique_routing_gateway_stub.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
