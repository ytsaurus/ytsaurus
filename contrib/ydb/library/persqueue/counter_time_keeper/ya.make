LIBRARY()

PEERDIR(
    library/cpp/lwtrace/protos
    contrib/ydb/library/actors/protos
    contrib/ydb/core/protos
)

SRCS(
    counter_time_keeper.h
    counter_time_keeper.cpp
)

END()
