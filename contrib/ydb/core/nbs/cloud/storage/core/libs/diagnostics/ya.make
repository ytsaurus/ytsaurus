LIBRARY()

SRCS(
    histogram.cpp
    logging.cpp
)

PEERDIR(
    library/cpp/lwtrace
    util
    contrib/ydb/core/protos/nbs
    contrib/ydb/library/services
)

END()

