LIBRARY()

SRCS(
    actor.cpp
    fetcher.cpp
    fetching_executor.cpp
    fetching_steps.cpp
    contexts.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/engines/writer
    contrib/ydb/core/tx/columnshard/export/events
    contrib/ydb/core/kqp/compute_actor
)

GENERATE_ENUM_SERIALIZATION(contexts.h)

END()
