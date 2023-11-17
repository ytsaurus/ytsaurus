LIBRARY()

PEERDIR(
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/parser/pg_catalog
    contrib/ydb/library/yql/providers/common/codec
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
