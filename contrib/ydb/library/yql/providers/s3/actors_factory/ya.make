LIBRARY()

YQL_LAST_ABI_VERSION()

SRCS(
    yql_s3_actors_factory.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/common
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/s3/proto
)

END()
