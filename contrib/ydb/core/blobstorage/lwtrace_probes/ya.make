LIBRARY()

SRCS(
    blobstorage_probes.cpp
)

PEERDIR(
    contrib/libs/grpc
    library/cpp/lwtrace/protos
    contrib/ydb/core/base
    contrib/ydb/core/protos
)

END()
