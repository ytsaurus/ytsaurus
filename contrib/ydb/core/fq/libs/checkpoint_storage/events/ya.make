LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/core/fq/libs/checkpointing_common
    contrib/ydb/core/fq/libs/checkpoint_storage/proto
    yql/essentials/public/issue
)

END()
