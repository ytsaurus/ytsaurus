LIBRARY()

SRCS(
    pdisk_mock.cpp
    pdisk_mock.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/blobstorage/pdisk
)

END()
