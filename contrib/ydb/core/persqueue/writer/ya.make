LIBRARY()

SRCS(
    metadata_initializers.cpp
    partition_chooser_impl.cpp
    source_id_encoding.cpp
    writer.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/digest/md5
    library/cpp/string_utils/base64
    contrib/ydb/core/base
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/grpc_services/cancelation/protos
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/core/protos
    contrib/ydb/library/wilson_ids
    contrib/ydb/public/lib/base
    contrib/ydb/public/lib/deprecated/kicli
    contrib/ydb/public/sdk/cpp/src/client/params
)

END()
