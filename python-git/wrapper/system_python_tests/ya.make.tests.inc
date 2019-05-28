INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

SET(YT_ABI 19_4)

TAG(
    ya:dirty
    ya:noretries
    ya:fat
    ya:force_sandbox
    ya:privileged
)

# https://sandbox.yandex-team.ru/task/434667495
REQUIREMENTS(container:960740055)

SIZE(LARGE)

PEERDIR(
    ${YT_PYTHON_ROOT}/python/yt/wrapper/system_python_tests/test_runner
)

INCLUDE(${ARCADIA_ROOT}/yt/python/yt/environment/ya_programs.make.inc)

DEPENDS(
    tools/fix_elf
)

NO_LINT()

NO_CHECK_IMPORTS()

