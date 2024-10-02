LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    client.cpp
)

PEERDIR(
    yt/yt/orm/client/native
    library/cpp/testing/gtest
)

END()
