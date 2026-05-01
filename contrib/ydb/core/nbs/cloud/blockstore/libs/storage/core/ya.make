LIBRARY()

SRCS(
    request_info.cpp
    tablet.cpp
    tablet_schema.cpp
    volume_label.cpp
    volume_model.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/nbs/cloud/blockstore/libs/storage/model
    contrib/ydb/core/nbs/cloud/storage/core/libs/common
    contrib/ydb/core/nbs/cloud/storage/core/protos
    contrib/ydb/core/control
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/protos
    contrib/ydb/core/tablet_flat

    library/cpp/deprecated/atomic
    library/cpp/lwtrace
)

END()

RECURSE(ut)
