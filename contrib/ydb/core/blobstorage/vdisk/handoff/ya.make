LIBRARY()

PEERDIR(
    library/cpp/actors/core
    library/cpp/monlib/service/pages
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/hulldb/generic
    contrib/ydb/core/protos
)

SRCS(
    defs.h
    handoff_basic.cpp
    handoff_basic.h
    handoff_delegate.cpp
    handoff_delegate.h
    handoff_map.cpp
    handoff_map.h
    handoff_mon.cpp
    handoff_mon.h
    handoff_proxy.cpp
    handoff_proxy.h
    handoff_synclogproxy.cpp
    handoff_synclogproxy.h
)

END()
