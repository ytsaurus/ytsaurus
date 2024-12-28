LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    program.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/yt/phdr_cache
    yt/yt/core
    yt/yt/ytlib
    yt/yt/library/fusion
    yt/yt/library/program
    yt/yt/library/server_program
    yt/yt/server/master
)

END()
