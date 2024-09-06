GO_TEST_FOR(yt/go/ytwalk)

SIZE(MEDIUM)

IF (OPENSOURCE AND AUTOCHECK)
    SKIP_TEST(Tests use docker)
ENDIF()

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)
ENDIF()

END()
