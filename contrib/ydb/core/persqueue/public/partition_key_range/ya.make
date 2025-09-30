LIBRARY()

SRCS(
    partition_key_range.cpp
    partition_key_range_sequence.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
)

END()

RECURSE_FOR_TESTS(ut)
