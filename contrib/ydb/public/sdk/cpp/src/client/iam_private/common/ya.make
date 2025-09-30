LIBRARY()

SRCS(
    iam.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/common
    contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common
)

END()
