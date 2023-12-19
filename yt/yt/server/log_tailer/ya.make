LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    bootstrap.cpp
    log_reader.cpp
    log_rotator.cpp
    log_tailer.cpp
    log_writer_liveness_checker.cpp
)

PEERDIR(
    yt/yt/library/query/engine

    yt/yt/core
    yt/yt/ytlib
    yt/yt/server/lib
    library/cpp/getopt/small
)

END()

RECURSE(
    bin
)
