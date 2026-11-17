LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    dataset.cpp
    input_stream.cpp
    multi_chunk_writer.cpp
    schema_ordering_writer.cpp
    writer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow_next
    yt/yt/ytlib
    yt/yt/library/formats
    yt/yt/library/query/engine
)

END()

RECURSE_FOR_TESTS(
    unittests
)
