LIBRARY()

SRCS(
    common.cpp
    interface.cpp
    scheduler.cpp
    activity.cpp
    task.cpp
    state.cpp
)

PEERDIR(
    contrib/ydb/library/accessor
    library/cpp/actors/core
    contrib/ydb/public/api/protos
    contrib/ydb/services/bg_tasks/protos
    contrib/ydb/core/base
)

END()
