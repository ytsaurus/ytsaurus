LIBRARY()

SRCS(
    client.cpp
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/public/issue
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

GENERATE_ENUM_SERIALIZATION(client.h)

END()
