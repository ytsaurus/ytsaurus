LIBRARY()

PEERDIR(
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb
    contrib/ydb/core/blobstorage/vdisk/ingress
    contrib/ydb/core/blobstorage/vdisk/repl
)

SRCS(
    balancing_actor.cpp
    deleter.cpp
    handoff_map.cpp
    sender.cpp
    utils.cpp
)

END()

