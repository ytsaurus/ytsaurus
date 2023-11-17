LIBRARY()

SRCS(
    service.cpp
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/services/metadata/abstract
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/services/bg_tasks/protos
    contrib/ydb/library/services
)

END()

RECURSE_FOR_TESTS(
    ut
)
