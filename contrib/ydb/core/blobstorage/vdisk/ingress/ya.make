LIBRARY()

PEERDIR(
    library/cpp/lwtrace
    contrib/ydb/core/base
    contrib/ydb/core/protos
)

SRCS(
    blobstorage_ingress.cpp
    blobstorage_ingress.h
    blobstorage_ingress_matrix.cpp
    blobstorage_ingress_matrix.h
    defs.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
