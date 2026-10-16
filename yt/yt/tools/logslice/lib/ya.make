LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    time_parser.cpp
    log_slice_engine.cpp
)

PEERDIR(
    library/cpp/streams/zstd
    library/cpp/yt/assert
    library/cpp/yt/error
)

END()
