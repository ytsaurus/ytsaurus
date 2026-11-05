LIBRARY()
INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common_arcadia.inc)

SRCS(
    iam.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/json
    contrib/ydb/public/api/client/yc_public/iam
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common
    contrib/ydb/public/sdk/cpp/src/client/types/core_facility
)

END()
