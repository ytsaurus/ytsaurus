LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    distributed_election_manager.cpp
    election_manager.cpp
    election_manager_thunk.cpp
    alien_cell_peer_channel_factory.cpp
    public.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()

RECURSE_FOR_TESTS(
    unittests
)
