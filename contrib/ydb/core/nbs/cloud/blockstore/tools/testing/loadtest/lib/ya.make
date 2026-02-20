LIBRARY()

SRCS(
    app_context.cpp
    buffer_pool.cpp
    helpers.cpp
    range_allocator.cpp
    range_map.cpp
    request_generator.cpp
    test_runner.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/libs/common
    contrib/ydb/core/nbs/cloud/blockstore/libs/diagnostics
    contrib/ydb/core/nbs/cloud/blockstore/libs/service
    contrib/ydb/core/nbs/cloud/blockstore/tools/testing/loadtest/lib/protos
    contrib/ydb/core/nbs/cloud/storage/core/libs/common
    contrib/ydb/core/nbs/cloud/storage/core/libs/diagnostics

    util
)

END()

RECURSE(
)
