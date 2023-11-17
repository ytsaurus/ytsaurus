LIBRARY()

SRCS(
    failure_injector.cpp
)

PEERDIR(
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
)

END()

RECURSE_FOR_TESTS(
    ut
)
