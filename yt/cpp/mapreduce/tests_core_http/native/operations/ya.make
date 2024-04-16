YT_UNITTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

TAG(
    ya:huge_logs
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
    library/cpp/testing/unittest
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/library/lazy_sort
    yt/cpp/mapreduce/library/operation_tracker
    yt/cpp/mapreduce/tests_core_http/yt_unittest_lib
    yt/cpp/mapreduce/tests_core_http/yt_unittest_main
    yt/cpp/mapreduce/tests_core_http/native/proto_lib
    yt/cpp/mapreduce/util
)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_operations_archive.inc)
ENDIF()

SIZE(MEDIUM)

FORK_SUBTESTS(MODULO)
SPLIT_FACTOR(10)

END()
