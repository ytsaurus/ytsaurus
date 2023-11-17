LIBRARY()

SRCS(
    yql_skiff_schema.cpp
)

PEERDIR(
    library/cpp/yson
    contrib/ydb/library/yql/providers/yt/common
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/schema/skiff
    contrib/ydb/library/yql/utils
)

END()
