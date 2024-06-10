LIBRARY()

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    contrib/ydb/public/api/grpc
)

END()

RECURSE_FOR_TESTS(
    ut
)
