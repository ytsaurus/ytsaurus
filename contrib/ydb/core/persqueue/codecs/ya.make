LIBRARY()

SRCS(
    pqv1.cpp
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_core
)

END()

RECURSE_FOR_TESTS(
    ut
)
