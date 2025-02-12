GTEST(unittester-ytlib)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    misc/test_connection.cpp

    arrow_writer_ut.cpp
    chunk_client_fetcher_ut.cpp
    chunk_fragment_read_controller_ut.cpp
    chunk_meta_cache_ut.cpp
    chunk_slice_ut.cpp
    downed_cell_tracker_ut.cpp
    encoding_writer_ut.cpp
    erasure_parts_reader_ut.cpp
    job_resources_ut.cpp
    memory_usage_tracker_ut.cpp
    multi_reader_manager/multi_reader_manager_ut.cpp
    object_service_cache_ut.cpp
    parallel_reader_memory_manager_ut.cpp
    partitioner_ut.cpp
    permission_cache_ut.cpp
    replication_reader_ut.cpp
    row_merger_ut.cpp
    secondary_index_ut.cpp
    serialize_ut.cpp
    sorted_merging_reader_ut.cpp
    striped_erasure_ut.cpp
    tablet_request_batcher_ut.cpp
    transaction_helpers_ut.cpp
    protobuf_helpers_ut.cpp
    ypath_ut.cpp
)

ADDINCL(
    contrib/libs/sparsehash/src
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/library/query/engine
    yt/yt/library/query/unittests/helpers
    yt/yt/library/query/row_comparer

    yt/yt/build

    yt/yt/core
    yt/yt/core/test_framework

    yt/yt/ytlib

    contrib/libs/sparsehash

    library/cpp/http/server
)

FORK_TESTS()

SPLIT_FACTOR(5)

SIZE(MEDIUM)

REQUIREMENTS(ram:12)

END()
