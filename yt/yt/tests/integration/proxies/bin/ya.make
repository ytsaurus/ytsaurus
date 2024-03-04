PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

DEPENDS(
    yt/yt/tests/integration/fake_blackbox
    
    # Required for HTTPS proxy tests.
    contrib/libs/openssl/apps
)

IF (NOT OPENSOURCE)
    DEPENDS(
        passport/infra/daemons/tvmtool/cmd
    )
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    yt/yt/tests/integration/proxies
)

END()
