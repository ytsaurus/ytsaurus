LIBRARY()

SRCS(
    object.cpp
    s3_uri.cpp
    identifier.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/library/conclusion
    contrib/ydb/services/metadata/secret/accessor
    contrib/restricted/aws/aws-crt-cpp
)

YQL_LAST_ABI_VERSION()

END()
