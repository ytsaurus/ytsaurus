PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

#controller, scheduler
DEPENDS(
    yt/yt/tools/scheduler_simulator/bin/converter
    yt/yt/tools/scheduler_simulator/bin/simulator
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/integration/scheduler_simulator
)

END()
