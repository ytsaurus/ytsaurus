LIBRARY()

SRCS(
    fetch_request_actor.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public
)

END()

RECURSE_FOR_TESTS(
    ut
)
