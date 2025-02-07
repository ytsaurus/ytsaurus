UNITTEST_WITH_CUSTOM_ENTRY_POINT()

SET(YT_RECIPE_BUILD_FROM_SOURCE yes)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)
    INCLUDE(${ARCADIA_ROOT}/library/recipes/s3mds/recipe.inc)
ENDIF()

TAG(
    ya:fat
)

SRCS(
    import_table_ut.cpp
)

PEERDIR(
    yt/yt/core/test_framework
    yt/yt/core
    yt/yt/core/http

    yt/yt/library/huggingface_client
    yt/yt/library/arrow_parquet_adapter

    yt/yt/tools/import_table/lib

    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/tests/yt_unittest_lib

    contrib/libs/apache/arrow
)

REQUIREMENTS(
    cpu:4
    ram_disk:32
)

SIZE(LARGE)
TIMEOUT(1200)

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_multi_slots.inc)

END()
