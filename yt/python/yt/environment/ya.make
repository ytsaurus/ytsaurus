PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/python/yt
    yt/python/yt/test_helpers
    yt/python/yt/yson
    yt/python/yt/wrapper
    yt/python/yt/environment/migrationlib
    yt/python/yt/environment/api

    yt/python/contrib/python-requests

    yt/yt/library/tracing/py

    contrib/python/six
)

PY_SRCS(
    NAMESPACE yt.environment

    __init__.py
    configs_provider.py
    default_config.py
    external_component.py
    local_cluster_configuration.py
    helpers.py
    init_cluster.py
    init_operation_archive.py
    init_queue_agent_state.py
    init_query_tracker_state.py
    porto_helpers.py
    tls_helpers.py
    yt_env.py
    watcher.py
    local_cypress.py
)

END()

RECURSE(
    api
    migrationlib
)

RECURSE_FOR_TESTS(
    tests
)
