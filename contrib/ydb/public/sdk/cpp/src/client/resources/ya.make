LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    ydb_resources.cpp
    ydb_ca.cpp
)

RESOURCE(
    contrib/ydb/public/sdk/cpp/src/client/resources/ydb_sdk_version.txt ydb_sdk_version_v3.txt
    contrib/ydb/public/sdk/cpp/src/client/resources/ydb_root_ca.pem ydb_root_ca_v3.pem
)

END()
