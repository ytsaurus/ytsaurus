LIBRARY()

SRCS(
    jwt.cpp
    jwt.h
)

PEERDIR(
    contrib/libs/jwt-cpp
    library/cpp/json
    contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/common
)

END()
