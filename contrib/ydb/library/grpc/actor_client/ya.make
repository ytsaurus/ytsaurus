LIBRARY()

SRCS(
    grpc_service_cache.h
    grpc_service_client.h
    grpc_service_settings.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/util
    library/cpp/digest/crc32c
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/library/services




    #ydb/library/ycloud/api
    #ydb/library/actors/core
    #library/cpp/digest/crc32c
    #ydb/public/sdk/cpp/src/library/grpc/client
    #library/cpp/json
    #ydb/core/base
    #ydb/library/services
    #ydb/public/lib/deprecated/client
    #ydb/public/lib/deprecated/kicli
)

END()
