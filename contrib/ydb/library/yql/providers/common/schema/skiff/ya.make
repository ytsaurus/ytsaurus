LIBRARY()

SRCS(
    yql_skiff_schema.cpp
)

PEERDIR(
    library/cpp/yson/node
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/schema/parser
)

END()
