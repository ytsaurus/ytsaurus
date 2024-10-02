GTEST(unittester-yt-orm)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    attribute_policy_ut.cpp
    master_config_ut.cpp
    request_tracker_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/server

    yt/yt/orm/client/objects

    yt/yt/core/test_framework
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_test_settings.inc)

END()
