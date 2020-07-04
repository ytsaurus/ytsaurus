
SET(YT_ABI 19_4)

TAG(
    ya:dirty
    ya:noretries
    ya:fat
    ya:force_sandbox
    ya:privileged
)

# https://sandbox.yandex-team.ru/task/545558892
REQUIREMENTS(container:1205365729)

SIZE(LARGE)

REQUIREMENTS(
    ram_disk:64
    cpu:8
)

PEERDIR(
    yt/python/yt/wrapper/system_python_tests/test_runner
)

INCLUDE(${ARCADIA_ROOT}/yt/python/yt/environment/ya_programs.make.inc)

DEPENDS(
    tools/fix_elf
    yt/yt/tools/yt_sudo_fixup
)

DATA(
    arcadia/ya
    arcadia/${YT_ROOT}/yt/python
    arcadia/yt/python
)

NO_LINT()

NO_CHECK_IMPORTS()

