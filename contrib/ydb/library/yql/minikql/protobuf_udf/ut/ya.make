UNITTEST_FOR(contrib/ydb/library/yql/minikql/protobuf_udf)

SRCS(
    type_builder_ut.cpp
    protobuf_ut.proto
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/schema
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/providers/common/schema/mkql
    contrib/libs/protobuf

    #alice/wonderlogs/protos
)

YQL_LAST_ABI_VERSION()

END()
