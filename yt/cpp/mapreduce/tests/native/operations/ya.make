UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

TAG(
    ya:huge_logs
    ya:fat
)

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

SRCS(
    helpers.cpp
    job_binary.cpp
    jobs.cpp
    operation_commands.cpp
    operation_tracker.cpp
    operation_watch.cpp
    operations.cpp
    prepare_operation.cpp
    raw_operations.cpp
    remote_clusters.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/lazy_sort
    yt/cpp/mapreduce/library/operation_tracker
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/tests/native/proto_lib
    yt/cpp/mapreduce/util
)

SET(YT_CLUSTER_NAMES first,second,third)
SET(YT_CONFIG_PATCH {init_operations_archive=%true;})
INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

REQUIREMENTS(
    cpu:4
    ram_disk:32
)

SIZE(LARGE)
TIMEOUT(1800)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(8)

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_multi_slots.inc)

END()
