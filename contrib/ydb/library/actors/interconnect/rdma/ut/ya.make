GTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")
    REQUIREMENTS(cpu:4)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    allocator_ut.cpp
    ibv_ut.cpp
    utils.cpp
    rdma_low_ut.cpp
)

PEERDIR(
    contrib/libs/ibdrv
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/address
    contrib/ydb/library/actors/interconnect/rdma
    contrib/ydb/library/actors/interconnect/rdma/cq_actor
    contrib/ydb/library/actors/interconnect/rdma/ut/utils
    contrib/ydb/library/actors/testlib
)

ENDIF()

END()
