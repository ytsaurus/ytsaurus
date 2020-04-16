INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

SET(YT_ABI 19_4)

TAG(
    #ya:dirty
    ya:noretries
    ya:fat
    ya:force_sandbox
    ya:privileged
)

# https://sandbox.yandex-team.ru/task/545558892
REQUIREMENTS(container:1205365729)

SIZE(LARGE)

REQUIREMENTS(
    disk_usage:400
    ram_disk:64
    cpu:8
)

PEERDIR(
    ${YT_PYTHON_ROOT}/python/yt/wrapper/system_python_tests/test_runner
)

INCLUDE(${ARCADIA_ROOT}/${YT_PYTHON_ROOT}/python/yt/environment/ya_programs.make.inc)

DATA(
    arcadia/yt/python
    arcadia/contrib/python/simplejson
    arcadia/contrib/python/cloudpickle
    arcadia/contrib/python/backports_abc
    arcadia/contrib/python/singledispatch
    arcadia/contrib/python/tornado
    arcadia/contrib/python/tqdm
    #arcadia/${YT_ROOT}/yt/python/yt_yson_bindings
    #arcadia/${YT_ROOT}/yt/python/yt_driver_bindings
    #arcadia/${YT_ROOT}/yt/python/yt_driver_rpc_bindings
)

NO_LINT()

NO_CHECK_IMPORTS()

