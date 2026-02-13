LIBRARY(base_utils)

SRCS(
    base_utils.h
    format_info.h
    format_info.cpp
    format_util.h
    format_util.cpp
    node_by_host.h
    node_by_host.cpp
)

PEERDIR(
    library/cpp/deprecated/enum_codegen
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    contrib/ydb/core/blobstorage/pdisk
    contrib/ydb/core/client/server
    contrib/ydb/core/driver_lib/cli_config_base
    contrib/ydb/core/protos
    contrib/ydb/public/lib/deprecated/client
)

YQL_LAST_ABI_VERSION()

END()
