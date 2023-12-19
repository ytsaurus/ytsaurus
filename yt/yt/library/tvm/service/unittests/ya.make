GTEST(unittester-library-auth_tvm)

ALLOCATOR(YT)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/build

    yt/yt/core/test_framework

    yt/yt/library/tvm/service
)

EXPLICIT_DATA()

IF(NOT OPENSOURCE)
    DATA(arcadia/library/cpp/tvmauth/client/ut/files)

    SRCS(
        tvm_ut.cpp
    )
ENDIF()

END()
