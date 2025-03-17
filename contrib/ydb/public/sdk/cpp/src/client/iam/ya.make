LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/json
    contrib/ydb/public/api/client/yc_public/iam
    contrib/ydb/public/sdk/cpp/src/client/iam/common
)

END()
