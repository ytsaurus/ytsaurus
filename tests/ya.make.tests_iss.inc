OWNER(
    g:yp
    ignat
)

SET(YP_TEST_SRCDIR yp/tests)

SET(YP_TEST_SRCS
    __init__.py
    conftest.py
    test_iss.py
)

INCLUDE(../python/ya_programs.make.inc)

DEPENDS(
    ${YT_ROOT}/yt/packages/tests_package
)

PEERDIR(
    yp/python
    yp/tests/helpers

    ${YT_PYTHON_ROOT}/python/yt/environment/arcadia_interop

    infra/iss_local/iss
)

REQUIREMENTS(
    cpu:8
    ram_disk:16
    disk_usage:100
    container:1041033321
)

DATA(
    sbr://1183503665 # yp_tests_iss_local_qemu.img
    sbr://876951675  # yp_tests_pod_agent_rootfs.tar.gz
    sbr://876951930  # yp_tests_pod_agent
    sbr://876952869  # yp_tests_workload_rootfs.tar.gz
)

SIZE(LARGE)

TAG(
    require:kvm
    ya:fat
    ya:privileged
    ya:force_sandbox
    ya:noretries
)

NO_LINT()
