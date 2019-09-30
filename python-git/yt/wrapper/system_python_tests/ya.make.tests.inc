INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

SET(YT_ABI 19_4)

TAG(
    ya:dirty
    ya:noretries
    ya:fat
    ya:force_sandbox
    ya:privileged
)

# https://sandbox.yandex-team.ru/task/440379739
REQUIREMENTS(container:973076293)

SIZE(LARGE)

PEERDIR(
    ${YT_PYTHON_ROOT}/python/yt/wrapper/system_python_tests/test_runner
)

INCLUDE(${ARCADIA_ROOT}/${YT_PYTHON_ROOT}/python/yt/environment/ya_programs.make.inc)

DEPENDS(
    tools/fix_elf
)

# Prepare for dist run.
# DATA(
#     arcadia/ya
#     arcadia/${YT_ROOT}/yt/python
# )

NO_LINT()

NO_CHECK_IMPORTS()

