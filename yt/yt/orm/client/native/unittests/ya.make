GTEST(unittester-yt-orm-client-native)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    common.cpp
    config_ut.cpp
    connection_ut.cpp
    parse_attributes_ut.cpp
    peer_discovery_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/client/native/unittests/mock

    yt/yt/orm/server

    yt/yt/core

    yt/yt/core/test_framework
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_test_settings.inc)

END()
