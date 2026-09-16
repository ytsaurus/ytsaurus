LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    per_workload_category_request_queue_provider.cpp
)

PEERDIR(
    yt/yt/server/lib
)

END()
