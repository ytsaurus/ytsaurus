LIBRARY()

SRCS(
    agent.cpp
    client.cpp
    owner.cpp
    private.cpp
    object_counter.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/protos
    contrib/ydb/core/base
)

END()
