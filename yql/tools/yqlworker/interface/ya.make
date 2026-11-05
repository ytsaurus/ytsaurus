LIBRARY()

SRCS(
    worker_api.cpp
)

PEERDIR(
    yql/tools/yqlworker/interface/proto
    library/cpp/threading/future
)

END()
