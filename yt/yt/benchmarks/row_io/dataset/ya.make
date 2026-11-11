LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    dataset.cpp
    input_stream.cpp
    schema_ordering_writer.cpp
    writer.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/library/formats
    yt/yt/library/query/engine
)

END()
