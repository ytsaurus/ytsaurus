LIBRARY()

SRCS(
    cms_grpc_client_actor.cpp
    compute_database_control_plane_service.cpp
    compute_databases_cache.cpp
    ydbcp_grpc_client_actor.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    contrib/ydb/core/fq/libs/compute/ydb/synchronization_service
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/quota_manager/proto
    contrib/ydb/core/protos
    contrib/ydb/library/db_pool/protos
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/lib/operation_id/protos
)

YQL_LAST_ABI_VERSION()

END()
