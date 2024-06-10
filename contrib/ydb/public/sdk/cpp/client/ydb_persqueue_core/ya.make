LIBRARY()

SRCS(
    persqueue.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core/impl
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public
)

END()

RECURSE_FOR_TESTS(
    ut
)
