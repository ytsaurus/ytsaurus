LIBRARY()

SRCS(
    rich_actor.cpp
    http_sender_actor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    library/cpp/retry
)

END()

RECURSE_FOR_TESTS(
    ut
)
