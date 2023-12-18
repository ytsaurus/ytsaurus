PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/yt/phdr_cache
    yt/yt/client
    yt/yt/ytlib
    library/cpp/getopt/small
)

END()
