PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/environment)
ELSE()
    PEERDIR(
        yt/python/yt
        yt/python/yt/test_helpers
        yt/python/yt/yson
        yt/python/yt/wrapper
        yt/python/yt/environment/migrationlib
        yt/python/yt/environment/api

        yt/python/contrib/python-requests

        contrib/python/six
        contrib/python/zstandard
    )

    PY_SRCS(
        NAMESPACE yt.environment

        __init__.py
        component.py
        configs_provider.py
        default_config.py
        local_cluster_configuration.py
        helpers.py
        init_cluster.py
        init_operations_archive.py
        init_queue_agent_state.py
        init_query_tracker_state.py
        porto_helpers.py
        tls_helpers.py
        yt_env.py
        watcher.py
        local_cypress.py
    )
ENDIF()

END()

RECURSE(
    api
    components
    migrationlib
)

RECURSE_FOR_TESTS(
    tests
)
