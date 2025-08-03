UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    remote_clusters.cpp
)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/tests/native/proto_lib
)

SET(YT_CLUSTER_NAMES first,second,third)
INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

REQUIREMENTS(
    cpu:4
    ram:16
    ram_disk:16
)

SIZE(MEDIUM)

END()
