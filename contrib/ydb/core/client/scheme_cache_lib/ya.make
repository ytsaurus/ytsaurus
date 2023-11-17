LIBRARY()

SRCS(
    yql_db_scheme_resolver.h
    yql_db_scheme_resolver.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/actors/core
    library/cpp/grpc/client
    library/cpp/threading/future
    contrib/ydb/core/base
    contrib/ydb/core/client/minikql_compile
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tx
)

YQL_LAST_ABI_VERSION()

END()
