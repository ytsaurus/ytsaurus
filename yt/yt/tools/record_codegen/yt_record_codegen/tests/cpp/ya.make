GTEST()

SRCS(
    test_simple_mapping.cpp
)

PEERDIR(
    yt/yt/client
)

GENERATE_YT_RECORD(
    ../records/simple_mapping.yaml
    OUTPUT_INCLUDES
        library/cpp/yt/yson_string/string.h
)

END()
