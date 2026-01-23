LIBRARY()

SRCS(
    blobstorage_pdisk_metadata.h
    blobstorage_pdisk_metadata.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/nodewarden
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/library/actors/core
    contrib/ydb/library/keys
)

END()
