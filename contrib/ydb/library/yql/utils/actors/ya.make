LIBRARY()

SRCS(
    rich_actor.cpp
    http_sender_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/public/types
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/providers/solomon/proto
)

END()

RECURSE_FOR_TESTS(
    ut
)
