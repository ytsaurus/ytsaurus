LIBRARY()

PEERDIR(
    contrib/ydb/core/formats/arrow/accessor/abstract
    contrib/ydb/library/formats/arrow
    contrib/ydb/library/formats/arrow/protos
)

SRCS(
    accessor.cpp
    GLOBAL constructor.cpp
    GLOBAL request.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
