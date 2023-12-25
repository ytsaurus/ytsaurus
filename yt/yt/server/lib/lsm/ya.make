LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    helpers.cpp
    lsm_backend.cpp
    partition_balancer.cpp
    partition.cpp
    store_compactor.cpp
    store_rotator.cpp
    tablet.cpp
    store.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/server/lib/tablet_node
)

END()

RECURSE_FOR_TESTS(
    unittests
)
