LIBRARY()

SRCS(
    checkpoint_coordinator.cpp
    checkpoint_coordinator.h
    checkpoint_id_generator.cpp
    checkpoint_id_generator.h
    pending_checkpoint.cpp
    pending_checkpoint.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/checkpointing_common
    contrib/ydb/core/fq/libs/checkpoint_storage/events
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/state
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
