LIBRARY()

SRCS(
    retro_span.cpp
    span_buffer.cpp
)

PEERDIR(
    contrib/libs/opentelemetry-proto
    contrib/ydb/library/actors/wilson
)

END()
