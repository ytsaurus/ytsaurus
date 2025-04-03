LIBRARY()

SRCS(
    table.cpp
)

PEERDIR(
    contrib/ydb/library/formats/arrow/csv/converter
    contrib/ydb/public/sdk/cpp/src/client/table
)

END()
