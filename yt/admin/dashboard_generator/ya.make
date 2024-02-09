PY3_LIBRARY()

PY_SRCS(
    TOP_LEVEL

    yt_dashboard_generator/__init__.py
    yt_dashboard_generator/cli.py
    yt_dashboard_generator/dashboard.py
    yt_dashboard_generator/diff.py
    yt_dashboard_generator/helpers.py
    yt_dashboard_generator/sensor.py
    yt_dashboard_generator/serializer.py
    yt_dashboard_generator/taggable.py
    yt_dashboard_generator/postprocessors.py
    
    yt_dashboard_generator/backends/__init__.py

    yt_dashboard_generator/backends/grafana/__init__.py
    yt_dashboard_generator/backends/grafana/cli.py

    yt_dashboard_generator/backends/monitoring/__init__.py
    yt_dashboard_generator/backends/monitoring/cli.py
    yt_dashboard_generator/backends/monitoring/sensors.py

    yt_dashboard_generator/specific_tags/__init__.py
    yt_dashboard_generator/specific_tags/tags.py

    yt_dashboard_generator/specific_sensors/__init__.py
    yt_dashboard_generator/specific_sensors/monitoring.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/tabulate
    contrib/python/colorama
    contrib/python/lark-parser

    contrib/python/grpcio/py3
    contrib/python/protobuf/py3
)

END()
