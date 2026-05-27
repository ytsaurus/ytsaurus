PY3_LIBRARY()

PEERDIR(
    yt/python/yt/admin/helpers
)

PY_SRCS(
    NAMESPACE yt.admin.metrics

    __init__.py
    cli.py
    config.py
    docker_runner.py
    dump.py
    grafana.py
    openmetrics.py
    prometheus.py
    promql.py
    replay.py
    spec.py
)

END()
