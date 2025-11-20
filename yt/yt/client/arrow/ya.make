LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    arrow_row_stream_encoder.cpp
    arrow_row_stream_decoder.cpp
    columnar_statistics.cpp
    public.cpp
    schema.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/client/arrow/fbs
    yt/yt/library/formats

    contrib/libs/apache/arrow_next
)

END()
