LIBRARY()

PEERDIR(
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/public/udf
    contrib/ydb/core/formats
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

SRCS(
    sharding.cpp
    hash.cpp
    xx_hash.cpp
    unboxed_reader.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)