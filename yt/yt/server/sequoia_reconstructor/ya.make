LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    helpers.cpp
    mapper.cpp
    program.cpp
    reducer.cpp
    record_consumer.cpp
    sequoia_reconstructor.cpp
)

PEERDIR(
    library/cpp/getopt/small
    yt/yt/core
    yt/yt/library/program
    yt/yt/library/fusion
    yt/yt/ytlib
    yt/yt/server/lib/io
    yt/yt/server/master
)

END()
