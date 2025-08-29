LIBRARY()

SRCS(
    table.cpp
)

PEERDIR(
    contrib/ydb/library/formats/arrow/csv/converter
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/lib/scheme_types
)

END()
