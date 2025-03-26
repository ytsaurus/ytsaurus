LIBRARY()

SRCS(
    counters.cpp
    net_classifier.cpp
)

GENERATE_ENUM_SERIALIZATION(net_classifier.h)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/base
    contrib/ydb/core/cms/console
    contrib/ydb/core/mon
    contrib/ydb/core/protos
    contrib/ydb/core/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
