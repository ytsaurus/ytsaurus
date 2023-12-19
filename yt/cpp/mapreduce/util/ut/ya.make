UNITTEST_FOR(yt/cpp/mapreduce/util)

SRCS(
    temp_table_ut.cpp
    ypath_join_ut.cpp
)

PEERDIR(
    yt/cpp/mapreduce/library/mock_client
)

END()
