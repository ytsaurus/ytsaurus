LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/interconnect
    contrib/ydb/core/fq/libs/checkpointing_common
    contrib/ydb/core/fq/libs/events
    contrib/ydb/core/fq/libs/checkpoint_storage/proto
    contrib/ydb/library/yql/public/issue
)

END()
