LIBRARY()

SRCS(
    manager.h
    manager.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/library/services
    contrib/ydb/library/yql/providers/common/metrics
    contrib/ydb/library/yql/utils
)

END()

RECURSE_FOR_TESTS(
    style
)
