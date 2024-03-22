PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

DEPENDS(
    yt/yt/tools/scheduler_simulator/bin
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/integration/scheduler_simulator
)

END()
