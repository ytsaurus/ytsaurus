LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/scheme_types
    contrib/ydb/library/actors/core
    contrib/ydb/library/formats/arrow/switch
)

SRCS(
    switch_type.cpp
)

END()
