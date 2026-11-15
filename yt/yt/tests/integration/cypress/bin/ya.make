PY3TEST()

SET(YT_SIZE_BUNDLED_TESTS yes)

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/integration/cypress
)

END()
