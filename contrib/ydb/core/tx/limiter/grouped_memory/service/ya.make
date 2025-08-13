LIBRARY()

SRCS(
    actor.cpp
    manager.cpp
    counters.cpp
    group.cpp
    process.cpp
    allocation.cpp
    ids.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/signals
    contrib/ydb/core/tx/limiter/grouped_memory/tracing
)

GENERATE_ENUM_SERIALIZATION(allocation.h)

END()
