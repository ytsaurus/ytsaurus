PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

DEPENDS(
    yt/yt/tests/integration/fake_blackbox
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/tests/integration/proxies
)

END()
