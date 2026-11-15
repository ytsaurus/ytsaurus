PY3TEST()

SET(YT_SIZE_BUNDLED_TESTS yes)

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/local_s3_recipe/recipe.inc)

PEERDIR(
    yt/yt/tests/integration/master
)

END()
