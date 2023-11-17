LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    contrib/ydb/public/api/client/yc_public/iam
    contrib/ydb/public/sdk/cpp/client/iam/impl
    contrib/ydb/public/sdk/cpp/client/iam/common
)

END()
