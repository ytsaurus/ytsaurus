LIBRARY()

SRCS(
    schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    yt/cpp/mapreduce/interface
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/yt/common
)

END()
