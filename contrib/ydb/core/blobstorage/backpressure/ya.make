LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/actors/core
    library/cpp/containers/intrusive_rb_tree
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/lwtrace_probes
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/protos
)

SRCS(
    common.h
    defs.h
    event.cpp
    event.h
    queue.cpp
    queue.h
    queue_backpressure_client.cpp
    queue_backpressure_client.h
    queue_backpressure_common.h
    queue_backpressure_server.h
    unisched.cpp
    unisched.h
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_client
)
