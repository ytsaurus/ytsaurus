PROGRAM()

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/util
    yt/cpp/mapreduce/library/blob_table

    yt/yt/library/arrow_parquet_adapter

    yt/yt/library/huggingface_client

    library/cpp/yson/node
    library/cpp/getopt

    contrib/libs/apache/arrow
)

SRCS(
    main.cpp
)

END()
