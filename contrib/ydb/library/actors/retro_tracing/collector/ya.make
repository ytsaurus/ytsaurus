LIBRARY()

SRCS(
    events.cpp
    retro_collector.cpp
    retro_span_deserialization.cpp
)

PEERDIR(
    contrib/proto/opentelemetry
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/retro_tracing/span
    contrib/ydb/library/actors/wilson
)

END()
