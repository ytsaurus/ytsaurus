LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    abort.cpp
    commit.cpp
    config.cpp
    serialize.cpp
    strong_ordering_manager.cpp
    transaction_lease_tracker.cpp
    transaction_manager.cpp
    transaction_manager_detail.cpp
    transaction_participant_provider.cpp
    transaction_supervisor.cpp

    proto/transaction_supervisor.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/server/lib/election
    yt/yt/server/lib/hydra
    yt/yt/server/lib/transaction_server
)

END()

RECURSE_FOR_TESTS(
    unittests
)
