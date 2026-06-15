LIBRARY()

SRCS(
    iam.cpp
)

PEERDIR(
    contrib/ydb/public/api/client/yc_private/iam
    contrib/ydb/public/sdk/cpp/src/client/iam_private/common
    contrib/ydb/public/sdk/cpp/src/client/types/core_facility
)

END()
