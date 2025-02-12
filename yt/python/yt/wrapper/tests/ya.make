PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SIZE(LARGE)

IF (AUTOCHECK OR YT_TEAMCITY)
    FORK_SUBTESTS()

    SPLIT_FACTOR(30)

    TIMEOUT(3600)
ENDIF()

YT_SPEC(yt/yt/tests/integration/spec.yson)

TAG(
    ya:fat
    ya:full_logs
    ya:noretries
    ya:yt
    ya:force_sandbox
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        sb_vault:YT_TOKEN=value:ignat:robot-yt-test-token
        cpu:20
        ram:32
        ram_disk:4
    )
ELSE()
    REQUIREMENTS(
        sb_vault:YT_TOKEN=value:ignat:robot-yt-test-token
        cpu:10
        ram:32
        ram_disk:4
    )
ENDIF()

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/wrapper/testlib
    yt/python/yt/testlib
    yt/python/yt/yson

    yt/yt/python/yt_driver_bindings
    yt/yt/python/yt_driver_rpc_bindings
    yt/yt/python/yt_yson_bindings

    library/python/porto

    library/python/resource

    # To test z-lz4 codec.
    library/python/codecs

    contrib/python/allure-pytest
    contrib/python/flaky
    contrib/python/mock
    contrib/python/requests-mock

    # It is required to test job shell.
    contrib/python/tornado/tornado-4
)

IF (PYTHON2)
    PEERDIR(
        contrib/deprecated/python/ujson
    )
ELSE()
    PEERDIR(
        contrib/python/ujson
    )
ENDIF()

DEPENDS(
    yt/yt/packages/tests_package

    # It is used in some tests to run separate cluster.
    # TODO(ignat): improve tests machinery to avoid using yt_local.
    yt/python/yt/local/bin/yt_local_native_make

    # It is used for yt job-tool tests.
    # yt/python/yt/wrapper/bin/yt_make

    # It is used for client impl tests.
    yt/python/yt/wrapper/bin/generate_client_impl

    yt/yt/tools/yt_sudo_fixup
    yt/yt/experiments/public/ytserver_dummy

    # Used in some tests to check cpp binaries in operations.
    yt/python/yt/wrapper/tests/files/cpp_bin

    # These python used for various tests.
    yt/python/yt/wrapper/tests/yt_python
    yt/python/yt/wrapper/tests/yt_ipython
)

EXPLICIT_DATA()

IF (NOT OPENSOURCE)
    DATA(
        # Used for tests with gdb.
        # Directory with cpp_bin_core_crash binary, create it locally and upload with:
        # ya upload ... --ttl=inf
        # cpp_bin_core_crash/
        #     cpp_bin_core_crash
        sbr://4804987727
    )
ENDIF()

RESOURCE(
    ${CURDIR}/files/accumulate_c.py /yt_python_test/accumulate_c.py
    ${CURDIR}/files/capitalize_b.py /yt_python_test/files/capitalize_b.py
    ${CURDIR}/files/collect.py /yt_python_test/files/collect.py
    ${CURDIR}/files/driver_read_request_catch_sigint.py /yt_python_test/files/driver_read_request_catch_sigint.py
    ${CURDIR}/files/empty /yt_python_test/files/empty
    ${CURDIR}/files/getnumber.cpp /yt_python_test/files/getnumber.cpp
    ${CURDIR}/files/helpers.py /yt_python_test/files/helpers.py
    ${CURDIR}/files/many_output.py /yt_python_test/files/many_output.py
    ${CURDIR}/files/my_op.py /yt_python_test/files/my_op.py
    ${CURDIR}/files/split.py /yt_python_test/files/split.py
    ${CURDIR}/files/standalone_binary.py /yt_python_test/files/standalone_binary.py
    ${CURDIR}/files/stderr_download.py /yt_python_test/files/stderr_download.py
    ${CURDIR}/files/main_interrupted_by_ping_failed.py /yt_python_test/files/main_interrupted_by_ping_failed.py
    ${CURDIR}/files/yt_test_lib.cpp /yt_python_test/files/yt_test_lib.cpp

    yt/python/yt/wrapper/bin/yt /binaries/yt
    yt/python/yt/wrapper/bin/mapreduce-yt /binaries/mapreduce-yt
    yt/java/ytsaurus-client-core/src/test/resources/good-rich-ypath.txt /good-rich-ypath.txt
)


IF (NOT OPENSOURCE)
    RESOURCE(
        yt/python/yt/wrapper/client_impl_yandex.py /modules/client_impl_yandex.py
    )
ELSE()
    # Keep it sync with yt/python/yt/wrapper/tests/system_python/test_paths.txt
    RESOURCE_FILES(
        yt/python/yt/wrapper/tests/test_operations_pickling.py
        yt/python/yt/wrapper/tests/test_tmpfs.py
    )
ENDIF()

TEST_SRCS(
    __init__.py
    helpers_cli.py
    conftest.py
    helpers.py
    test_acl_commands.py
    test_admin.py
    test_authentication.py
    test_batch_execution.py
    test_chaos_commands.py
    test_client.py
    test_command_params.py
    test_cypress_commands.py
    test_docker_respawn.py
    test_driver.py
    test_dirtable_commands.py
    test_download_core_dump.py
    test_dynamic_table_commands.py
    test_errors.py
    test_file_commands.py
    test_fuse.py
    test_ipython.py
    test_job_commands.py
    test_job_tool.py
    test_jupyter.py
    test_mapreduce.py
    test_misc.py
    test_module.py
    test_operations.py
    test_operations_tracker.py
    test_parse_ypath.py
    test_ping_failed_modes.py
    test_queue_commands.py
    test_query_commands.py
    test_random_sample.py
    test_run_compression_benchmarks.py
    test_spark.py
    test_spec_builders.py
    test_table_commands.py
    test_user_statistics.py
    test_yamr_mode.py
    test_yt_cli.py
)

INCLUDE(${ARCADIA_ROOT}/devtools/large_on_single_slots.inc)

END()

RECURSE_FOR_TESTS(
    serverless
    typed_api
)

IF (NOT OPENSOURCE)
    RECURSE(
        yt_python
        yt_ipython
    )

    RECURSE_FOR_TESTS(
        system_python
        arcadia_python
        features
    )
ENDIF()
