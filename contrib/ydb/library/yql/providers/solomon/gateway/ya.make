LIBRARY()

SRCS(
    yql_solomon_gateway.cpp
)

PEERDIR(
    yql/essentials/providers/common/gateway
    contrib/ydb/library/yql/providers/solomon/provider
)

END()
