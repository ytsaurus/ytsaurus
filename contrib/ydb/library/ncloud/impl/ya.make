RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    access_service.cpp
    access_service.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/grpc/actor_client
    contrib/ydb/library/ncloud/api
    contrib/ydb/core/base
)

END()
