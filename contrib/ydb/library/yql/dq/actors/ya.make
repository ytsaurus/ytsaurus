LIBRARY()

GRPC()

SRCS(
    dq.cpp
    dq_events_ids.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/actors/protos
    contrib/ydb/library/yql/public/ydb_issue
    yql/essentials/public/issue
)

END()

RECURSE(
    common
    compute
    input_transforms
    spilling
    task_runner
)
