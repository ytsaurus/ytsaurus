LIBRARY()

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/public/sdk/cpp/src/library/persqueue/topic_parser_public
    contrib/ydb/public/api/protos
)

SRCS(
    topic_parser.h
    topic_parser.cpp
    counters.h
    counters.cpp
    type_definitions.h
)

END()

RECURSE_FOR_TESTS(
    ut
)
