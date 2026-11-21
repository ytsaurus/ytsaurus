YQL_LIBRARY()

PEERDIR(
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/public/api/protos
    yql/essentials/minikql/computation
    yql/essentials/parser/pg_catalog
    yql/essentials/parser/pg_wrapper/interface
)

SRCS(
    mkql_proto.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
