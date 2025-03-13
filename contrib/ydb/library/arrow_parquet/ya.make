LIBRARY()

SRCS(
    result_set_parquet_printer.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/value
    contrib/libs/apache/arrow
)

END()
