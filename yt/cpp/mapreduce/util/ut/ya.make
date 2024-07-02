GTEST()

SRCS(
    temp_table_ut.cpp
    ypath_join_ut.cpp
)

PEERDIR(
    yt/cpp/mapreduce/library/mock_client
    yt/cpp/mapreduce/util
)

END()
