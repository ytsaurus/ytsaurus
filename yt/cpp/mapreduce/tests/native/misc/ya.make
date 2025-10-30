UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)
INCLUDE(misc_sources.make.inc)

EXPLICIT_DATA()

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

SRCS(${MISC_SRCS})

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/tests/native/proto_lib
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/util
    library/cpp/dwarf_backtrace
    library/cpp/dwarf_backtrace/registry
)

SIZE(MEDIUM)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(2)

INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

END()
