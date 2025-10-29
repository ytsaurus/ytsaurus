LIBRARY()

SRCS(
    counters_info.cpp
    counters_info.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/mon
    contrib/ydb/library/aclib
    contrib/ydb/public/api/protos
    contrib/ydb/public/api/grpc
    yql/essentials/public/issue/protos
)

END()

