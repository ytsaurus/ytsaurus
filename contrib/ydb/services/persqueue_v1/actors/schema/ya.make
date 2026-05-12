LIBRARY()

PEERDIR(
    contrib/ydb/services/persqueue_v1/actors/schema/common
    contrib/ydb/services/persqueue_v1/actors/schema/pqv1
    contrib/ydb/services/persqueue_v1/actors/schema/topic
)

SRCS(
)

END()

RECURSE(
    common
    pqv1
    topic
)
