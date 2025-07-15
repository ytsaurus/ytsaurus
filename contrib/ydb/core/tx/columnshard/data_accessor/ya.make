LIBRARY()

SRCS(
    request.cpp
    manager.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/data_accessor/abstract
    contrib/ydb/core/tx/columnshard/data_accessor/local_db
    contrib/ydb/core/tx/columnshard/data_accessor/cache_policy
    contrib/ydb/core/tx/columnshard/resource_subscriber
)

END()
