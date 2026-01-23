LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/library/yql/providers/common/arrow/interface
)

SRCS(
    arrow_reader_impl.cpp
)

CFLAGS(
    -DARCADIA_BUILD -DUSE_PARQUET
)

END()

RECURSE(
    interface
)
