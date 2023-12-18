PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/client/formats
    yt/yt/ytlib
    library/cpp/getopt/small
)

SRCS(
    main.cpp
)

END()
