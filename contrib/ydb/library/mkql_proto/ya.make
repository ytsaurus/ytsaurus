LIBRARY()

PEERDIR(
    contrib/ydb/library/mkql_proto/protos
    yql/essentials/minikql/computation
    yql/essentials/parser/pg_catalog
    yql/essentials/providers/common/codec
    contrib/ydb/public/api/protos
)

SRCS(
    mkql_proto.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
