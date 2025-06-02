LIBRARY()

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/library/actors/core
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
    load_based_timeout.cpp
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
