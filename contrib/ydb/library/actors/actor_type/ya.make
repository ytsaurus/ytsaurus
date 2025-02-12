LIBRARY()

SRCS(
    common.cpp
    indexes.cpp
    index_constructor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/prof
)

GENERATE_ENUM_SERIALIZATION(common.h)

END()
