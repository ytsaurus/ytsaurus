LIBRARY()

SRCS(
    describer.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public
)

GENERATE_ENUM_SERIALIZATION(describer.h)

END()

RECURSE_FOR_TESTS(
    ut
)
