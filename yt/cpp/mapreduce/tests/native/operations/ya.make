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

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_operations_archive.inc)
ENDIF()

REQUIREMENTS(
    cpu:4
    ram_disk:32
)

SIZE(LARGE)
TIMEOUT(1200)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(5)

END()
