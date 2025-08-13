RECURSE(
    inference
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/curl

    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/s3/common
    contrib/ydb/library/yql/providers/s3/credentials
    contrib/ydb/public/sdk/cpp/adapters/issue
)

SRC(
    s3_fetcher.cpp
)

END()
