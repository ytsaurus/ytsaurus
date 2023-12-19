LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    file_changelog_index.cpp
    checkpointable_stream.cpp
    changelog_store_factory_thunk.cpp
    changelog_store_helpers.cpp
    changelog.cpp
    config.cpp
    composite_automaton.cpp
    file_changelog_dispatcher.cpp
    file_helpers.cpp
    hydra_context.cpp
    hydra_janitor_helpers.cpp
    hydra_manager.cpp
    hydra_service.cpp
    lazy_changelog.cpp
    local_changelog_store.cpp
    local_hydra_janitor.cpp
    local_snapshot_store.cpp
    mutation.cpp
    private.cpp
    mutation_context.cpp
    persistent_response_keeper.cpp
    public.cpp
    remote_changelog_store.cpp
    remote_snapshot_store.cpp
    serialize.cpp
    snapshot_store_thunk.cpp
    state_hash_checker.cpp
    unbuffered_file_changelog.cpp
)

PEERDIR(
    yt/yt/library/process
    yt/yt/ytlib
    yt/yt/server/lib/election
    yt/yt/server/lib/io
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
