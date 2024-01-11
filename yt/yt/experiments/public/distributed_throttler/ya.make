PROGRAM(distributed_throttler)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/ytlib/distributed_throttler
    yt/yt/server/lib
    yt/yt/server/lib/discovery_server
    yt/yt/server/lib/discovery_server/unittests/mock
)

END()
