LIBRARY()

SRCS(
    yson_helpers.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/providers/yt/common
)

END()
