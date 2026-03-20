LIBRARY()

SRCS(
    test_utils.cpp
    test_with_actor_system.cpp
)

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/testlib/actors
    contrib/ydb/core/testlib/basics
)

YQL_LAST_ABI_VERSION()

END()
