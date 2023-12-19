LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    changelog_discovery.cpp
    changelog_download.cpp
    changelog_acquisition.cpp
    decorated_automaton.cpp
    distributed_hydra_manager.cpp
    dry_run_hydra_manager.cpp
    helpers.cpp
    lease_tracker.cpp
    mutation_committer.cpp
    private.cpp
    recovery.cpp
    snapshot_discovery.cpp
    snapshot_download.cpp

    proto/hydra_service.proto
)

PEERDIR(
    yt/yt/library/process
    yt/yt/ytlib
    yt/yt/server/lib/misc
    yt/yt/server/lib/election
    yt/yt/server/lib/hydra_common
    yt/yt/server/lib/io
    library/cpp/yt/logging/backends/stream
)

END()
