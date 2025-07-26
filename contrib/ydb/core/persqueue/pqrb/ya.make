LIBRARY()

SRCS(
    partition_scale_request.cpp
    partition_scale_manager.cpp
    read_balancer__balancing_app.cpp
    read_balancer__balancing.cpp
    read_balancer_app.cpp
    read_balancer.cpp
)

GENERATE_ENUM_SERIALIZATION(read_balancer__balancing.h)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/core/base
    contrib/ydb/core/engine/minikql
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/partition_key_range
    contrib/ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
#    ut
#    dread_cache_service/ut
#    ut/slow
#    ut/ut_with_sdk
)
