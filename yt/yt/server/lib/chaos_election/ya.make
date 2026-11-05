LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    chaos_lease.cpp
    config.cpp
    election_manager.cpp
)

PEERDIR(
    yt/yt/library/lock_election
    yt/yt/core
    yt/yt/client
)

END()

RECURSE_FOR_TESTS(
    unittests
)
