LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/vdisk/common
)

SRCS(
    defs.h
    dsproxy_mock.cpp
    dsproxy_mock.h
)

END()
