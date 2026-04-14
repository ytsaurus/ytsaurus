LIBRARY()

SRCS(
    synchronization_service.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/compute/common
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/core/fq/libs/quota_manager/proto
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    contrib/ydb/library/db_pool/protos
    contrib/ydb/library/grpc/actor_client
    contrib/ydb/library/services
    contrib/ydb/public/api/grpc
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/public/sdk/cpp/adapters/issue
    contrib/ydb/public/sdk/cpp/src/library/operation_id
)

YQL_LAST_ABI_VERSION()

END()
