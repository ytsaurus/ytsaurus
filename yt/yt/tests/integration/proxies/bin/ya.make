PY3TEST()

SET(YT_SIZE_BUNDLED_TESTS yes)

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

DEPENDS(
    yt/yt/tests/integration/fake_blackbox
    
    # Required for HTTPS proxy tests.
    contrib/libs/openssl/apps
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/integration/proxies
)

END()
