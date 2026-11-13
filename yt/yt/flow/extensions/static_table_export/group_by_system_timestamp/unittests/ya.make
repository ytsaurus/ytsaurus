GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    static_table_export_ut.cpp
)

PEERDIR(
    yt/yt/flow/extensions/static_table_export/group_by_system_timestamp
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
