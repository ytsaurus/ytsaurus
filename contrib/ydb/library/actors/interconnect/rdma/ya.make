LIBRARY()

SRCS(
    ctx.cpp
    link_manager.cpp
    mem_pool.cpp
)

PEERDIR(
    contrib/libs/ibdrv
    contrib/libs/protobuf
    contrib/ydb/library/actors/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
