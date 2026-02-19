LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    chunk_view_size_compaction_hint.cpp
    compaction_hints.cpp
    lsm_backend.cpp
    min_hash_digest_compaction_hint.cpp
    partition.cpp
    partition_balancer.cpp
    row_digest_compaction_hint.cpp
    started_tasks_summary.cpp
    store.cpp
    store_compactor.cpp
    store_rotator.cpp
    tablet.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/server/lib/tablet_node
)

END()

RECURSE_FOR_TESTS(
    unittests
)
