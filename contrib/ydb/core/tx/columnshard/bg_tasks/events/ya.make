LIBRARY()

SRCS(
    events.cpp
    local.cpp
    global.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/tx/columnshard/bg_tasks/protos
)

END()
