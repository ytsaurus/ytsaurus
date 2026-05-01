LIBRARY()

SRCS(
    alter_topic.cpp
    alter_topic_internal.cpp
    common.cpp
    drop_topic.cpp
    schema.cpp
    schema_int.cpp
    topic_alterer.cpp
    validation.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/public
    contrib/ydb/core/persqueue/public/describer
)

END()

RECURSE_FOR_TESTS(
#    ut
)
