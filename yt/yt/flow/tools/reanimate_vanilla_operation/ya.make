PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/vanilla
    yt/yt/client
    yt/yt/client/cache
    yt/yt/library/program
    yt/yt/core
    library/cpp/getopt
)

END()
