INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)

SET(YT_TEST_SRCDIR yt/python/yt/wrapper/tests)

SET(YT_TEST_SRCS
    __init__.py
    conftest.py
    helpers.py
    test_acl_commands.py
    test_authentication.py
    test_batch_execution.py
    test_client.py
    test_command_params.py
    test_common.py
    test_config.py
    test_cypress_commands.py
    test_driver.py
    test_dynamic_table_commands.py
    test_errors.py
    test_file_commands.py
    test_formats.py
    test_fuse.py
    test_job_commands.py
    test_job_tool.py
    test_misc.py
    test_module.py
    test_operations.py
    test_parse_ypath.py
    test_spec_builders.py
    test_random_sample.py
    test_table_commands.py
    test_thread_pool.py
    test_user_statistics.py
    test_yamr_mode.py
)

PEERDIR(
    ${YT_PYTHON_ROOT}/python/yt/wrapper
    ${YT_PYTHON_ROOT}/python/yt/yson
    ${YT_PYTHON_ROOT}/python/yt/local
    ${YT_PYTHON_ROOT}/python/yt/environment/arcadia_interop
    ${YT_ROOT}/yt/python/yt_driver_bindings
    ${YT_ROOT}/yt/python/yt_driver_rpc_bindings
    ${YT_ROOT}/yt/python/yt_yson_bindings

    contrib/python/flaky
)

INCLUDE(../../environment/ya_programs.make.inc)

DEPENDS(
    ${YT_PYTHON_ROOT}/python/yt/local/bin/yt_local_make
    ${YT_PYTHON_ROOT}/python/yt/wrapper/bin/yt-job-tool_make
    ${YT_PYTHON_ROOT}/python/yt/wrapper/tests/yt_python
)

DATA(
    arcadia/${YT_PYTHON_ROOT}/python/yt
)

REQUIREMENTS(
    ram_disk:56
    cpu:4
)

SIZE(LARGE)

TAG(
    ya:fat
    ya:force_sandbox
)

FORK_TEST_FILES()

NO_LINT()
