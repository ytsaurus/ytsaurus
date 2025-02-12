LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    jwt.cpp
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/json
)

END()
