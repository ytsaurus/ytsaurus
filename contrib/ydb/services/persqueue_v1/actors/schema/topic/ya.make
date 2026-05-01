LIBRARY()

PEERDIR(
    contrib/ydb/core/persqueue/public/schema
    contrib/ydb/services/persqueue_v1/actors/schema/common
)

SRCS(
    actors.cpp
    alter_topic.cpp
    drop_topic.cpp
)

END()
