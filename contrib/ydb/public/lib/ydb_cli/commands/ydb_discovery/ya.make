LIBRARY(ydb_cli_command_ydb_discovery)

SRCS(
    ../ydb_service_discovery.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/commands/command_base
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/src/client/discovery
)

END()
