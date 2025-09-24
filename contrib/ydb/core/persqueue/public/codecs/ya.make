LIBRARY()

SRCS(
    pqv1.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/client/persqueue_public
    contrib/ydb/public/sdk/cpp/src/client/topic
)

END()

RECURSE_FOR_TESTS(
    ut
)
