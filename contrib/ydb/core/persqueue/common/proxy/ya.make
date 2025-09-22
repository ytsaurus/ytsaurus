LIBRARY()

SRCS(
    actor_persqueue_client_iface.cpp
)

PEERDIR(
    contrib/ydb/core/persqueue/public
    contrib/ydb/public/sdk/cpp/src/client/topic
)

END()

