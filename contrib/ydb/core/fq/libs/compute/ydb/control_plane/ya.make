LIBRARY()

SRCS(
    cms_grpc_client_actor.cpp
    compute_database_control_plane_service.cpp
    compute_databases_cache.cpp
    database_monitoring.cpp
    monitoring_grpc_client_actor.cpp
    monitoring_rest_client_actor.cpp
    ydbcp_grpc_client_actor.cpp
)

PEERDIR(
    library/cpp/json
    contrib/ydb/core/fq/libs/compute/ydb/synchronization_service
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/quota_manager/proto
    contrib/ydb/core/kqp/workload_service/common
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    contrib/ydb/library/db_pool/protos
    contrib/ydb/library/grpc/actor_client
    contrib/ydb/library/yql/utils/actors
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/src/client/resources
    contrib/ydb/public/sdk/cpp/src/library/operation_id/protos
    yql/essentials/public/issue
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()
