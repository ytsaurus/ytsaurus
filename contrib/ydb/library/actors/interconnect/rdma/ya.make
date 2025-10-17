LIBRARY()

SRCS(
    cq_actor.cpp
    ctx.cpp
    ctx_open.cpp
    link_manager.cpp
    mem_pool.cpp
    rdma.cpp
)

PEERDIR(
    contrib/libs/ibdrv
    contrib/libs/protobuf
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    contrib/ydb/library/actors/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
