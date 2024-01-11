PROGRAM(hydra_stress)

PROTO_NAMESPACE(yt)

SRCS(
    automaton.cpp
    channel.cpp
    checkers.cpp
    client.cpp
    config.cpp
    disruptors.cpp
    main.cpp
    peer.cpp
    peer_service.proto
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/ytlib
    yt/yt/server/lib
    yt/yt/server/lib/hydra
    library/cpp/getopt/small
)

END()
