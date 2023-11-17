LIBRARY()

SRCS(
    yql_provider_gateway.h
    yql_provider_gateway.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/yql/ast
)

END()
