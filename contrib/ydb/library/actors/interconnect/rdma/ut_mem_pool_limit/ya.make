GTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    allocator_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/interconnect/rdma
    contrib/ydb/library/actors/interconnect/rdma/ut/utils
)

ENDIF()

END()
