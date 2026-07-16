LIBRARY()

SRCS(
    events.cpp
    retro_collector.cpp
    retro_span_deserialization.cpp
)

PEERDIR(
    contrib/libs/opentelemetry-proto
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/retro_tracing/span
    contrib/ydb/library/actors/wilson
)

END()
