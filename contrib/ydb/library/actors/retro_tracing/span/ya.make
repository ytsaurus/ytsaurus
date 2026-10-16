LIBRARY()

SRCS(
    retro_span.cpp
    span_buffer.cpp
)

PEERDIR(
    contrib/proto/opentelemetry
    contrib/ydb/library/actors/wilson
)

END()
