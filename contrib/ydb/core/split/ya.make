LIBRARY()

SRCS(
    split.h
    key_access.cpp
    data_size.cpp
)

PEERDIR(
    contrib/ydb/core/scheme
    contrib/ydb/core/scheme_types
    contrib/ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
