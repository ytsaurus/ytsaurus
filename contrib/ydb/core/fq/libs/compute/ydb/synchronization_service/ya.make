LIBRARY()

SRCS(
    synchronization_service.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/protos
    contrib/ydb/core/fq/libs/control_plane_storage/proto
    contrib/ydb/public/api/grpc
    contrib/ydb/library/db_pool/protos
    contrib/ydb/public/lib/operation_id/protos
    contrib/ydb/core/fq/libs/quota_manager/proto
    contrib/ydb/public/api/grpc/draft
    contrib/ydb/library/services
)

YQL_LAST_ABI_VERSION()

END()
