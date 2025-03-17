LIBRARY()

SRCS(
    controller.cpp
    state.cpp
    events.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
)

YQL_LAST_ABI_VERSION()

END()
