LIBRARY()

SRCS(
    yq_cloud_audit_service.cpp
)

PEERDIR(
    library/cpp/unified_agent_client
    contrib/ydb/core/fq/libs/actors
    contrib/ydb/core/fq/libs/audit/events
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/library/actors/log_backend
    contrib/ydb/library/folder_service
    contrib/ydb/library/security
    contrib/ydb/library/ycloud/api
    contrib/ydb/public/api/client/yc_public/events
)

END()
