
SET(YT_TEST_SRCDIR yt/python/yt/wrapper/tests)

SET(YT_TEST_SRCS
    __init__.py
    conftest.py
    helpers.py
    test_acl_commands.py
    test_admin.py
    test_authentication.py
    test_batch_execution.py
    test_client.py
    test_command_params.py
    test_common.py
    test_cypress_commands.py
    test_driver.py
    test_download_core_dump.py
    test_dynamic_table_commands.py
    test_errors.py
    test_file_commands.py
    test_formats.py
    test_fuse.py
    test_ipython.py
    test_job_commands.py
    test_job_tool.py
    test_misc.py
    test_module.py
    test_operations.py
    test_operations_tracker.py
    test_parse_ypath.py
    test_random_sample.py
    test_schema.py
    test_spark.py
    test_spec_builders.py
    test_table_commands.py
    test_thread_pool.py
    test_user_statistics.py
    test_yamr_mode.py
)

PEERDIR(
    yt/python/yt/wrapper
    yt/python/yt/clickhouse
    yt/python/yt/yson
    yt/python/yt/local
    yt/python/yt/environment
    yt/python/yt/environment/arcadia_interop
    ${YT_ROOT}/yt/python/yt_driver_bindings
    ${YT_ROOT}/yt/python/yt_driver_rpc_bindings
    ${YT_ROOT}/yt/python/yt_yson_bindings

    infra/porto/api_py

    contrib/python/flaky
)

DEPENDS(
    yt/yt/packages/tests_package
    yt/python/yt/local/bin/yt_local_make
    yt/python/yt/wrapper/bin/yt_make
    yt/python/yt/wrapper/tests/yt_python
    yt/python/yt/wrapper/tests/yt_python3
    yt/python/yt/wrapper/tests/yt_ipython
    ${YT_ROOT}/yt/tools/yt_sudo_fixup
    ${YT_ROOT}/yt/experiments/ytserver_dummy
    yt/python/yt/wrapper/tests/files/cpp_bin
)

IF(NOT GCC)
    DEPENDS(
        ${YT_ROOT}/yt/server/clickhouse_server/bin
        ${YT_ROOT}/yt/server/clickhouse_trampoline
        ${YT_ROOT}/yt/server/log_tailer/bin
    )

    SET(YT_TEST_SRCS
        ${YT_TEST_SRCS}
        test_clickhouse.py
    )
ENDIF()

DATA(
    # Directory with cpp_bin_core_crash binary, create it locally and upload with:
    # ya upload ... --ttl=inf
    # cpp_bin_core_crash/
    #     cpp_bin_core_crash
    sbr://1681775644 
)

RESOURCE(
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/accumulate_c.py /yt_python_test/accumulate_c.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/capitalize_b.py /yt_python_test/files/capitalize_b.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/collect.py /yt_python_test/files/collect.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/driver_catch_sigint.py /yt_python_test/files/driver_catch_sigint.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/driver_read_request_catch_sigint.py /yt_python_test/files/driver_read_request_catch_sigint.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/empty /yt_python_test/files/empty
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/getnumber.cpp /yt_python_test/files/getnumber.cpp
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/helpers.py /yt_python_test/files/helpers.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/many_output.py /yt_python_test/files/many_output.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/my_op.py /yt_python_test/files/my_op.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/split.py /yt_python_test/files/split.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/standalone_binary.py /yt_python_test/files/standalone_binary.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/stderr_download.py /yt_python_test/files/stderr_download.py
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/files/yt_test_lib.cpp /yt_python_test/files/yt_test_lib.cpp
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/test_mapreduce.sh /yt_python_test/test_mapreduce.sh
    ${ARCADIA_ROOT}/${YT_TEST_SRCDIR}/test_yt.sh /yt_python_test/test_yt.sh

    ${ARCADIA_ROOT}/yt/python/yt/wrapper/bin/yt /binaries/yt
    ${ARCADIA_ROOT}/yt/python/yt/wrapper/bin/mapreduce-yt /binaries/mapreduce-yt
)

SIZE(LARGE)

TIMEOUT(1800)

FORK_TEST_FILES()
FORK_SUBTESTS()
SPLIT_FACTOR(6)

TAG(
    ya:fat
    ya:huge_logs
    ya:full_logs
    ya:sys_info
    ya:noretries
)

NO_LINT()
