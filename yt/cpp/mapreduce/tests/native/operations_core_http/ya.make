UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

TAG(
    ya:huge_logs
    ya:fat
)

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

ENV(
    YT_TESTS_USE_CORE_HTTP_CLIENT="yes"
)

SRCS(
    ../operations/helpers.cpp
    ../operations/job_binary.cpp
    ../operations/jobs.cpp
    ../operations/operation_commands.cpp
    ../operations/operation_tracker.cpp
    ../operations/operation_watch.cpp
    ../operations/operations.cpp
    ../operations/prepare_operation.cpp
    ../operations/raw_operations.cpp
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

SET(YT_CONFIG_PATCH {init_operations_archive=%true;})
INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

REQUIREMENTS(
    cpu:4
    ram_disk:32
)

SIZE(LARGE)
TIMEOUT(1200)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(5)

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_multi_slots.inc)

END()
