PY3TEST()

################################################################################

TAG(
    ya:fat
    ya:full_logs
    ya:sys_info
    ya:force_sandbox
    ya:sandbox_coverage
    ya:noretries
    ya:yt
    ya:relwithdebinfo
)

YT_SPEC(yt/yt/tests/integration/spec.yson)

IF (AUTOCHECK OR YT_TEAMCITY)
    FORK_TESTS(MODULO)

    SPLIT_FACTOR(70)

    TIMEOUT(1800)
ENDIF()

IF (SANITIZER_TYPE)
    IF (YT_TEAMCITY)
        TAG(ya:manual)
    ENDIF()
ENDIF()

SIZE(LARGE)

REQUIREMENTS(
    sb_vault:YT_TOKEN=value:ignat:robot-yt-test-token
)

REQUIREMENTS(
    cpu:16
    ram_disk:4
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        ram:24
    )
ELSE()
    REQUIREMENTS(
        ram:16
    )
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/opensource_tests.inc)

PEERDIR(
    contrib/python/pytz
    yt/yt/tests/library
    yt/yt/tests/conftest_lib
)

DEPENDS(
    yt/yt/tools/yt_sudo_fixup
    yt/yt/packages/tests_package
    yt/packages/23_2
)

PY_SRCS(
    # This is a workaround for weird Arcadia Python issue. If we fail to specify separate namespace for the
    # test file from the original YT integration tests, packaging system would put test_journals.py
    # into module yt.yt.<...> resulting in module yt being overridden (!) resulting in lots of import errors
    # from module yt (in particular, yt.packages.six).
    NAMESPACE original_tests
    yt/yt/tests/integration/master/test_master_snapshots.py
    yt/yt/tests/integration/misc/test_get_supported_features.py
    yt/yt/tests/integration/controller/test_map_operation.py
    yt/yt/tests/integration/controller/test_merge_operation.py
    yt/yt/tests/integration/controller/test_reduce_operation.py
    yt/yt/tests/integration/controller/test_join_reduce_operation.py
    yt/yt/tests/integration/controller/test_map_reduce_operation.py
    yt/yt/tests/integration/controller/test_remote_copy_operation.py
    yt/yt/tests/integration/controller/test_sort_operation.py
    yt/yt/tests/integration/node/test_disk_quota.py
    yt/yt/tests/integration/queues/test_queue_agent.py
)

TEST_SRCS(
    test_compatibility.py
    test_get_supported_features.py
    test_master_snapshots_compatibility.py
    test_map_operation.py
    test_merge_operation.py
    test_reduce_operation.py
    test_join_reduce_operation.py
    test_map_reduce_operation.py
    test_remote_copy_operation.py
    test_sort_operation.py
    test_scheduler_update.py
    test_disk_quota.py
    test_operations_archive.py
    test_queue_agent.py
)

END()
