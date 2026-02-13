LIBRARY()

SRCS(
    colors.cpp
    workload_factory.cpp
)

PEERDIR(
    library/cpp/getopt
    contrib/ydb/library/accessor
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/query
    contrib/ydb/public/sdk/cpp/src/client/table
    contrib/ydb/public/sdk/cpp/src/client/value
)

END()
