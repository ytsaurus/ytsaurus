LIBRARY()

SRCS(
    helpers.cpp
    query_id.cpp
    settings.cpp
    services.cpp
    kqp_event_ids.cpp
    temp_tables.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/public/api/protos
)

END()
