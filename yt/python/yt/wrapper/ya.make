PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/python/yt

    yt/python/yt/yson
    yt/python/yt/skiff
    yt/python/yt/entry
    yt/python/yt/ypath
    yt/python/yt/wire_format
    yt/python/yt/packages
    yt/python/yt/type_info
    # implicit deps in arc for yandex.type_info
    library/python/type_info

    yt/python/contrib/python-dill
    yt/python/contrib/python-fusepy
    yt/python/contrib/python-requests
    yt/python/contrib/python-urllib3
    yt/python/contrib/python-idna

    contrib/libs/brotli/python
    contrib/python/argcomplete
    contrib/python/decorator
    contrib/python/distro
    contrib/python/six
    contrib/python/typing-extensions
)

IF (OPENSOURCE)
    PEERDIR(
        contrib/python/charset-normalizer
    )
ELSE()
    PEERDIR(
        yt/python/contrib/python-chardet
        library/python/svn_version
        library/python/oauth
    )
ENDIF()

IF (PYTHON2)
    PEERDIR(
        contrib/deprecated/python/typing
        contrib/deprecated/python/ujson
    )
ELSE()
    # CONTRIB-2709 - Временно оключаем
    #PEERDIR(
    #    contrib/python/ujson
    #)
ENDIF()

NO_CHECK_IMPORTS(
    yt.wrapper.cypress_fuse
)

SET(SRCS
    __init__.py
    _py_runner.py
    acl_commands.py
    admin_commands.py
    auth_commands.py
    batch_api.py
    batch_client.py
    batch_execution.py
    batch_helpers.py
    batch_response.py
    chaos_commands.py
    cli_helpers.py
    cli_impl.py
    client_api.py
    client_helpers.py
    client_state.py
    client.py
    command.py
    common.py
    completers.py
    compression.py
    config.py
    config_remote_patch.py
    constants.py
    cypress_commands.py
    cypress_fuse.py
    default_config.py
    dirtable_commands.py
    download_core_dump.py
    driver.py
    dynamic_table_commands.py
    errors.py
    etc_commands.py
    exceptions_catcher.py
    file_commands.py
    flow_commands.py
    format.py
    framing.py
    heavy_commands.py
    http_driver.py
    http_helpers.py
    job_commands.py
    job_shell.py
    job_tool.py
    local_mode.py
    lock_commands.py
    mappings.py
    native_driver.py
    operation_commands.py
    operations_tracker.py
    parallel_reader.py
    parallel_writer.py
    pickling.py
    prepare_operation.py
    progress_bar.py
    py_runner_helpers.py
    py_wrapper.py
    query_commands.py
    queue_commands.py
    random_sample.py
    response_stream.py
    retries.py
    run_command_with_lock.py
    run_compression_benchmarks.py
    run_operation_commands.py
    schema/__init__.py
    schema/helpers.py
    schema/internal_schema.py
    schema/table_schema.py
    schema/types.py
    schema/variant.py
    shuffle.py
    skiff.py
    spark.py
    spec_builders.py
    spec_builder_helpers.py
    strawberry.py
    stream.py
    string_iter_io.py
    system_random.py
    table_commands.py
    table_helpers.py
    table.py
    thread_pool.py
    transaction_commands.py
    transaction.py
    transform.py
    tvm.py
    user_statistics.py
    version_check.py
    yamr_mode.py
    yamr_record.py
    ypath.py
    yson.py
)

IF (NOT OPENSOURCE)
    SET(SRCS
        ${SRCS}
        client_impl_yandex.py
        idm_cli_helpers.py
        idm_client_helpers.py
        idm_client.py
        sky_share.py
        yandex_constants.py
    )
ELSE()
    SET(SRCS
        ${SRCS}
        client_impl.py
    )
ENDIF()

PY_SRCS(
    NAMESPACE yt.wrapper
    ${SRCS}
)


END()
