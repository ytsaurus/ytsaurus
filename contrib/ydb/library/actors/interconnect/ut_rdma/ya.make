GTEST()

IF (OS_LINUX AND SANITIZER_TYPE != "memory")

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    port_manager.cpp
    rdma_xdc_ut.cpp    
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/ut/protos
    contrib/ydb/library/actors/interconnect/ut/lib
    contrib/ydb/library/actors/interconnect/rdma/ut/utils
    contrib/ydb/library/actors/testlib
)

ENDIF()

END()
