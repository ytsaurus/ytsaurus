LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

GENERATE_YT_RECORD(
    records/snapshot_block.yaml
)

SRCS(
    block_flusher.cpp
    block_map.cpp
    block_store.cpp
    config.cpp
    block_store_helpers.cpp
    dirty_block_pool.cpp
    journal_block_device.cpp
    snapshot_reader.cpp
    snapshot_writer.cpp
)

PEERDIR(
    yt/yt/server/lib/nbd
    yt/yt/client
    yt/yt/ytlib
    yt/yt/library/query/engine
)

END()

RECURSE_FOR_TESTS(
    unittests
)
