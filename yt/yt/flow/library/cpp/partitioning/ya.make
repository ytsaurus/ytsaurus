LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    input_partitioning.cpp
    partitioning_coordinator.cpp
    partitioning_helpers.cpp
)

PEERDIR(
    library/cpp/iterator
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/throughput_autoscaler
)

END()

RECURSE_FOR_TESTS(unittests)
