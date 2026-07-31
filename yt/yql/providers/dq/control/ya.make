LIBRARY()

SRCS(
    yql_dq_control.cpp
    yql_dq_control.h
)

PEERDIR(
    contrib/ydb/library/yql/providers/dq/api/grpc
    contrib/ydb/library/yql/providers/dq/api/protos
    contrib/ydb/library/yql/providers/dq/common
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/svnversion
    yql/essentials/core/file_storage
    yql/essentials/minikql
    yql/essentials/providers/common/proto
    yql/essentials/utils/log
    yt/yql/providers/dq/config
)

YQL_LAST_ABI_VERSION()

END()
