LIBRARY()

SRCS(
    cache_policy.cpp
    manager.cpp
)

PEERDIR(
    contrib/ydb/core/tx/general_cache
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/resource_subscriber
)

END()
