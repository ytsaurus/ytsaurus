LIBRARY()

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/core/formats/arrow/save_load
    contrib/ydb/core/formats/arrow/serializer
    contrib/ydb/core/formats/arrow/splitter
    contrib/ydb/core/formats/arrow/accessor/common
)

SRCS(
    GLOBAL constructor.cpp
    GLOBAL request.cpp
    accessor.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
