LIBRARY()

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/util
    yt/cpp/mapreduce/library/blob_table
    yt/cpp/mapreduce/library/table_schema

    yt/yt/library/arrow_parquet_adapter

    yt/yt/library/huggingface_client

    yt/yt/library/s3

    library/cpp/yson/node
    library/cpp/getopt

    contrib/libs/apache/arrow
)

SRCS(
    import_table.cpp
)

END()
