LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    changelog_discovery.cpp
    changelog_download.cpp
    checkpointer.cpp
    decorated_automaton.cpp
    distributed_hydra_manager.cpp
    lease_tracker.cpp
    local_snapshot_service.cpp
    mutation_committer.cpp
    recovery.cpp
    snapshot_discovery.cpp
    snapshot_download.cpp

    proto/hydra_service.proto
    proto/snapshot_service.proto
)

PEERDIR(
    yt/yt/library/process
    yt/yt/ytlib
    yt/yt/server/lib/election
    yt/yt/server/lib/hydra_common
    yt/yt/server/lib/io
    yt/yt/server/lib/misc
)

END()
