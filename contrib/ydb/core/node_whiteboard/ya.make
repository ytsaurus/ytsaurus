LIBRARY()

SRCS(
    node_whiteboard.h
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/protos
    library/cpp/deprecated/enum_codegen
    library/cpp/logger
    library/cpp/lwtrace/mon
    library/cpp/random_provider
    library/cpp/time_provider
    contrib/ydb/core/base
    contrib/ydb/core/base/services
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/debug
    contrib/ydb/core/erasure
    contrib/ydb/core/protos
)

END()
