PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/integration/s3
)

END()
