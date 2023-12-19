PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/tests/integration/master
)

END()
