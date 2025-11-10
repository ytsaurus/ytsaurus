UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)
INCLUDE(${ARCADIA_ROOT}/yt/cpp/mapreduce/tests/native/misc/misc_sources.make.inc)

EXPLICIT_DATA()

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

ENV(
    YT_TESTS_USE_CORE_HTTP_CLIENT="yes"
)

SRCS(${MISC_SRCS})

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/tests/native/proto_lib
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/util
)

# Dummy signature generation for distributed write API
SET(YT_CONFIG_PATCH {proxy_config={signature_components={generation={generator={};cypress_key_writer={owner_id="test"};key_rotator={}};validation={cypress_key_reader={}}}};})

SIZE(MEDIUM)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(2)

INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

END()
