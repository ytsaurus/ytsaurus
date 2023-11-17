LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/scheme_types
    library/cpp/actors/core
)

SRCS(
    switch_type.cpp
)

END()
