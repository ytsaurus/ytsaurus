LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    program.cpp
)

PEERDIR(
    yt/yt/orm/server

    library/cpp/yt/phdr_cache

    yt/yt/core

    library/cpp/getopt
)

END()
