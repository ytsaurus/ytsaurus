LIBRARY()

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/iam
    library/cpp/digest/crc32c
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

SRCS(
    util.cpp
    ydb_credentials_provider_factory.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
