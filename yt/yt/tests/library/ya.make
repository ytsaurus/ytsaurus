PY3_LIBRARY()

NO_CHECK_IMPORTS()

PEERDIR(
    yt/python/yt
    yt/python/yt/environment
    yt/python/yt/environment/arcadia_interop
    yt/python/yt/sequoia_tools
    yt/python/yt/wrapper
    yt/yt/python/yt_driver_bindings
    yt/yt/python/yt_yson_bindings
    yt/yt_proto/yt/core
    yt/yt_proto/yt/client
    contrib/python/decorator
    contrib/python/flaky
    contrib/python/grpcio
    contrib/python/python-dateutil
    library/python/porto
    library/cpp/xdelta3/py_bindings
)

PY_NAMESPACE(.)

PY_SRCS(
    yt_error_codes.py
    yt_commands.py
    yt_env_setup.py
    yt_helpers.py
    yt_sequoia_helpers.py
    yt_type_helpers.py
    yt_scheduler_helpers.py
    yt_dynamic_tables_base.py
    yt_chaos_test_base.py
    yt_queries.py
    yt_queue_agent_test_base.py
    yt_mock_server.py
    gdb_helpers.py
    decimal_helpers.py
)

IF (OPENSOURCE)
    PY_SRCS(
        yt_tests_opensource_settings.py
    )
ELSE()
    PY_SRCS(
        yt_tests_settings.py
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    test
)
