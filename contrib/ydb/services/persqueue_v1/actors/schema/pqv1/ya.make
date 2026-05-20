LIBRARY()

PEERDIR(
    contrib/ydb/core/persqueue/public/schema
    contrib/ydb/services/persqueue_v1/actors/schema/common
    contrib/ydb/services/lib/actors
)

SRCS(
    actors.cpp
    add_consumer.cpp
    alter_topic.cpp
    common.cpp
    create_topic.cpp
    drop_topic.cpp
    remove_consumer.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
