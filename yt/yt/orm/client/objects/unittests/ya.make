GTEST(unittester-yt-orm-client-objects)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    object_key_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/client/objects

    yt/yt/core/test_framework
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_test_settings.inc)

END()
