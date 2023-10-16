LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/ytlib
    yt/yt/core/test_framework
)

SRCS(
    api_test_base.cpp
)

END()
