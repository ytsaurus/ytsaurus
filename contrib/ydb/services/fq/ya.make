LIBRARY()

SRCS(
    grpc_service.cpp
    private_grpc.cpp
    ydb_over_fq.cpp
)

PEERDIR(
    contrib/ydb/library/grpc/server
    library/cpp/retry
    contrib/ydb/core/fq/libs/grpc
    contrib/ydb/core/grpc_services
    contrib/ydb/core/grpc_services/base
    contrib/ydb/library/protobuf_printer
)

END()

RECURSE_FOR_TESTS(
    ut_integration
)
