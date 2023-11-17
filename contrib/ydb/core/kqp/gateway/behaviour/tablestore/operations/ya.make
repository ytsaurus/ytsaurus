LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL add_column.cpp
    GLOBAL alter_column.cpp
    GLOBAL drop_column.cpp
)

PEERDIR(
    contrib/ydb/services/metadata/manager
    contrib/ydb/core/formats/arrow/compression
    contrib/ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
