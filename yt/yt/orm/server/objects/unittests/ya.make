GTEST(unittester-yt-orm-library)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SRCS(
    batch_size_backoff_ut.cpp
    build_tags_ut.cpp
    config_ut.cpp
    history_collector_ut.cpp
    try_get_table_name_from_query.cpp
)

PEERDIR(
    yt/yt/orm/server

    yt/yt/core/test_framework
)

END()
