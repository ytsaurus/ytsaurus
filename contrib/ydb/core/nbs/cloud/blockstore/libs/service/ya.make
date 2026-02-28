LIBRARY()

GENERATE_ENUM_SERIALIZATION(request.h)

SRCS(
    aligned_device_handler.cpp
    blocks_info.cpp
    context.cpp
    device_handler.cpp
    request.cpp
    unaligned_device_handler.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/common
    contrib/ydb/core/nbs/cloud/blockstore/public/api/protos
    contrib/ydb/core/nbs/cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(ut)
