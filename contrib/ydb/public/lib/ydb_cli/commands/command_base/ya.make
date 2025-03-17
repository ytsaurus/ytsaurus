LIBRARY(ydb_cli_command_base)

SRCS(
    ../ydb_command.cpp
)

PEERDIR(
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/src/client/draft
    contrib/ydb/public/sdk/cpp/src/client/driver
)

END()
