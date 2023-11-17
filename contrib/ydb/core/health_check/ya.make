LIBRARY()

SRCS(
    health_check.cpp
    health_check.h
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/library/aclib
    contrib/ydb/public/api/protos
    contrib/ydb/public/api/grpc
    contrib/ydb/library/yql/public/issue/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
