LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/storage/core/libs/actors
    contrib/ydb/core/nbs/cloud/storage/core/libs/common

    contrib/ydb/library/actors/core

    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
