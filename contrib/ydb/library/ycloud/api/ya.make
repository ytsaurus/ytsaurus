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
    contrib/ydb/public/api/client/yc_private/resourcemanager
    library/cpp/actors/core
    library/cpp/grpc/client
    contrib/ydb/core/base
    contrib/ydb/core/grpc_caching
)

END()
