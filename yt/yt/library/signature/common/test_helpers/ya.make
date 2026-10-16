LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    helpers.cpp
    stub_keystore.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/library/signature/common
)

END()
