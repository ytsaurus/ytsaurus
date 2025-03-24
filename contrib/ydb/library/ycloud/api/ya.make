LIBRARY()

SRCS(
    access_service.h
    folder_service.h
    folder_service_transitional.h
    iam_token_service.h
    user_account_service.h
)

PEERDIR(
    contrib/ydb/public/api/client/yc_private/iam
    contrib/ydb/public/api/client/yc_private/servicecontrol
    contrib/ydb/public/api/client/yc_private/accessservice
    contrib/ydb/public/api/client/yc_private/resourcemanager
    contrib/ydb/library/actors/core
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/core/base
    contrib/ydb/core/grpc_caching
)

END()
