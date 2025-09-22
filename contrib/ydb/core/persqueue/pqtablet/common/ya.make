LIBRARY()

SRCS(
    event_helpers.cpp
    tracing_support.cpp
)



PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/persqueue/common
    contrib/ydb/library/logger
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
)

END()

RECURSE_FOR_TESTS(
)
