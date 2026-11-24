LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/libs/ydb-cpp-sdk/sdk_common.inc)

SRCS(
    ydb_resources.cpp
    ydb_ca.cpp
)

RESOURCE(
    contrib/libs/ydb-cpp-sdk/src/client/resources/ydb_root_ca.pem ydb_root_ca_v3.pem
)

END()
