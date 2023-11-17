LIBRARY()

SRCS(
    control_plane_storage_requester_actor.cpp
    query_utils.cpp
    ydb_schema_query_actor.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/iterator
    contrib/ydb/core/fq/libs/common
    contrib/ydb/core/fq/libs/control_plane_proxy/events
    contrib/ydb/core/fq/libs/control_plane_storage/events
    contrib/ydb/core/fq/libs/result_formatter
    contrib/ydb/core/kqp/provider
    contrib/ydb/library/db_pool/protos
)

GENERATE_ENUM_SERIALIZATION(ydb_schema_query_actor.h)

YQL_LAST_ABI_VERSION()

END()
