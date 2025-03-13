LIBRARY()

SRCS(
    helpers.cpp
    kqp_event_ids.cpp
    query_id.cpp
    reattach.cpp
    services.cpp
    settings.cpp
    temp_tables.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/core/base
    contrib/ydb/core/protos
    yql/essentials/ast
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/public/api/protos
)

END()
