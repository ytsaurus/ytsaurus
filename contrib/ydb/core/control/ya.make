LIBRARY()

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/base
    contrib/ydb/core/control/lib
    contrib/ydb/core/mon
    contrib/ydb/core/node_whiteboard
    contrib/ydb/library/actors/core
)

SRCS(
    defs.h
    immediate_control_board_actor.cpp
    immediate_control_board_actor.h
    immediate_control_board_impl.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
