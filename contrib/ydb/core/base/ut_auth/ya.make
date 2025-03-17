UNITTEST_FOR(contrib/ydb/core/base)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/aclib
)

YQL_LAST_ABI_VERSION()

SRCS(
    auth_ut.cpp
)

END()
