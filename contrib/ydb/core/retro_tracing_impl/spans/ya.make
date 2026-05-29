LIBRARY()

SRCS(
    named_span.cpp
    retro_tracing.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/library/actors/retro_tracing
)

END()
