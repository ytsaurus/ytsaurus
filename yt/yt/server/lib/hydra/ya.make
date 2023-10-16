LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    changelog.cpp
    changelog_acquisition.cpp
    changelog_discovery.cpp
    changelog_download.cpp
    changelog_store_factory_thunk.cpp
    changelog_store_helpers.cpp
    checkpointable_stream.cpp
    composite_automaton.cpp
    config.cpp
    decorated_automaton.cpp
    distributed_hydra_manager.cpp
    dry_run_hydra_manager.cpp
    file_changelog_dispatcher.cpp
    file_changelog_index.cpp
    file_helpers.cpp
    helpers.cpp
    hydra_context.cpp
    hydra_janitor_helpers.cpp
    hydra_manager.cpp
    hydra_service.cpp
    lazy_changelog.cpp
    lease_tracker.cpp
    local_changelog_store.cpp
    local_hydra_janitor.cpp
    local_snapshot_store.cpp
    mutation.cpp
    mutation_committer.cpp
    mutation_context.cpp
    persistent_response_keeper.cpp
    private.cpp
    recovery.cpp
    remote_changelog_store.cpp
    remote_snapshot_store.cpp
    serialize.cpp
    snapshot_discovery.cpp
    snapshot_download.cpp
    snapshot_store_thunk.cpp
    state_hash_checker.cpp
    unbuffered_file_changelog.cpp

    proto/hydra_service.proto
)

PEERDIR(
    yt/yt/library/process
    yt/yt/ytlib
    yt/yt/server/lib/misc
    yt/yt/server/lib/election
    yt/yt/server/lib/io
    library/cpp/yt/logging/backends/stream
    library/cpp/streams/lz/lz4
    library/cpp/streams/lz/snappy
)

END()

RECURSE(
    dry_run
    mock
)

RECURSE_FOR_TESTS(
    unittests
)

