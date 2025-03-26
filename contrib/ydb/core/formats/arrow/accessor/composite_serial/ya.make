LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/core/formats/arrow/common
    contrib/ydb/core/formats/arrow/save_load
)

SRCS(
    accessor.cpp
)

END()
