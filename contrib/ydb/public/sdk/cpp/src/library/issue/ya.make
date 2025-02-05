LIBRARY()

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    utf8.cpp
    yql_issue.cpp
    yql_issue_message.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/colorizer
    contrib/ydb/public/api/protos
    contrib/ydb/public/sdk/cpp/src/library/string_utils/helpers
)

END()
