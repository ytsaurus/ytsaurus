
SET(YT_TEST_SRCDIR yt/python/yt/wrapper/tests)

SET(YT_TEST_SRCS
    __init__.py
    conftest.py
    helpers.py
    test_acl_commands.py
    test_authentication.py
    test_batch_execution.py
    test_clickhouse.py
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
    ${YT_ROOT}/yt/python/yt_driver_bindings
    ${YT_ROOT}/yt/python/yt_driver_rpc_bindings
    ${YT_ROOT}/yt/python/yt_yson_bindings

    infra/porto/api_py

    contrib/python/flaky
)

DEPENDS(
    yt/yt/packages/tests_package
    yt/python/yt/local/bin/yt_local_make
    yt/python/yt/wrapper/bin/yt-job-tool_make
    yt/python/yt/wrapper/tests/yt_python
    yt/python/yt/wrapper/tests/yt_python3
    yt/python/yt/wrapper/tests/yt_ipython
    ${YT_ROOT}/yt/tools/yt_sudo_fixup
    ${YT_ROOT}/yt/server/clickhouse_server/bin
    ${YT_ROOT}/yt/server/clickhouse_trampoline
    ${YT_ROOT}/yt/server/log_tailer/bin
    ${YT_ROOT}/yt/experiments/ytserver_dummy
    yt/python/yt/wrapper/tests/files/cpp_bin
    yt/python/yt/wrapper/tests/files/cpp_bin_core_crash
)

RESOURCE_FILES(
    PREFIX yt_python_test/
    ../files/accumulate_c.py
    ../files/capitalize_b.py
    ../files/collect.py
    ../files/driver_catch_sigint.py
    ../files/driver_read_request_catch_sigint.py
    ../files/empty
    ../files/getnumber.cpp
    ../files/helpers.py
    ../files/many_output.py
    ../files/my_op.py
    ../files/split.py
    ../files/standalone_binary.py
    ../files/stderr_download.py
    ../files/yt_test_lib.cpp
    ../test_mapreduce.sh
    ../test_yt.sh
    ../../bin/yt
    ../../bin/mapreduce-yt
)

SIZE(LARGE)

TIMEOUT(1800)

FORK_TEST_FILES()
FORK_TESTS()
SPLIT_FACTOR(12)

REQUIREMENTS(
    ram_disk:4
    cpu:4
)

TAG(
    ya:fat
    ya:force_distbuild
    ya:huge_logs
    ya:full_logs
    ya:sys_info
    ya:noretries
)

NO_LINT()
