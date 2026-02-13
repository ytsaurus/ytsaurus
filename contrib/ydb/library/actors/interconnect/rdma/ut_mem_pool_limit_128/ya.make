GTEST()
#TIMEOUT(3600)
IF (OS_LINUX AND SANITIZER_TYPE != "memory")

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TIMEOUT(3600)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    allocator_ut.cpp
)

PEERDIR(
    library/cpp/threading/future
    contrib/ydb/library/actors/interconnect/rdma
    contrib/ydb/library/actors/interconnect/rdma/ut/utils
)

ENDIF()

END()
