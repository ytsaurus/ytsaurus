LIBRARY()


SRCS(
    abstract.cpp
    GLOBAL sub_column.cpp
    GLOBAL default.cpp
)

PEERDIR(
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow/accessor/sub_columns
)

YQL_LAST_ABI_VERSION()

END()
