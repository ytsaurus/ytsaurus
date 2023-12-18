PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/client
    yt/yt/server/lib/io
    yt/yt/ytlib
    library/cpp/getopt/small
)

END()
