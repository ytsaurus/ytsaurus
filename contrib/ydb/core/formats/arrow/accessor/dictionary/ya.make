LIBRARY()

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/core/tx/columnshard/engines/protos
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/formats/arrow/protos
)

SRCS(
    accessor.cpp
    additional_data.cpp
    GLOBAL constructor.cpp
    GLOBAL request.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
