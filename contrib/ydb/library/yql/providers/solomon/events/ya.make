LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/library/yql/providers/solomon/proto
    contrib/ydb/library/yql/providers/solomon/solomon_accessor/client
)

END()
