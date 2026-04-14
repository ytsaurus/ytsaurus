LIBRARY()

SRCS(
    request_info.cpp
    volume_label.cpp
    volume_model.cpp
)

PEERDIR(
    contrib/ydb/core/nbs/cloud/blockstore/config/protos
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/model
    contrib/ydb/core/nbs/cloud/storage/core/libs/common
    contrib/ydb/core/nbs/cloud/storage/core/protos
    contrib/ydb/core/control
    contrib/ydb/core/protos

    library/cpp/deprecated/atomic
    library/cpp/lwtrace
)

END()
