PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/server/node
    yt/yt/server/lib/io
    library/cpp/yt/phdr_cache
    yt/yt/ytlib
    yt/yt/client
    yt/yt/core
    library/cpp/getopt/small
)

END()
