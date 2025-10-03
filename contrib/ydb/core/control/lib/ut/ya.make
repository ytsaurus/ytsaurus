UNITTEST_FOR(contrib/ydb/core/control/lib)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/actors/actor_type
    library/cpp/testing/unittest
    util
)

SRCS(
    immediate_control_board_ut.cpp
)

END()
