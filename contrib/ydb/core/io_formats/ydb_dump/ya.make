LIBRARY()

SRCS(
    csv_ydb_dump.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/scheme_types
    contrib/ydb/core/io_formats/cell_maker
)

YQL_LAST_ABI_VERSION()

END()
