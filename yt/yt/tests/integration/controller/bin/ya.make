PY3TEST()

INCLUDE(../../YaMakeBoilerplateForTestsWithConftest.txt)

#controller, scheduler
DEPENDS(
    yt/yt/tools/cuda_core_dump_injection
    yt/yt/tests/cuda_core_dump_simulator
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/tests/integration/controller
)

END()
