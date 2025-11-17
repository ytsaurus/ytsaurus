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

# Dummy signature generation for distributed write API
SET(YT_CONFIG_PATCH {proxy_config={signature_components={generation={generator={};cypress_key_writer={owner_id="test"};key_rotator={}};validation={cypress_key_reader={}}}};})

SIZE(MEDIUM)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(2)

INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

END()
