PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

DEPENDS(
    contrib/libs/openssl/apps
)

PEERDIR(
    yt/yt/tests/integration/misc
)

END()
