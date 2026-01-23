LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    contrib/ydb/core/fq/libs/actors/logging
    contrib/ydb/core/fq/libs/compute/common
    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/core/fq/libs/control_plane_storage
    contrib/ydb/public/api/protos
)

YQL_LAST_ABI_VERSION()

END()
