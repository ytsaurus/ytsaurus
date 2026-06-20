LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    time_parser.cpp
    log_slice_engine.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/streams/zstd
)

END()
