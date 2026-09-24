PY23_LIBRARY()

STYLE_PYTHON()

PEERDIR(
    contrib/python/argcomplete
    contrib/python/grpcio

    yt/python/yt/wrapper

    yt/python/yt/yson
    yt/yt/python/yt_yson_bindings

    yt/python/yt

    yt/yt_proto/yt/core

    yt/yt_proto/yt/orm/client/proto
)

PY_SRCS(
    NAMESPACE yt.orm.library

    cli_helpers.py
    common.py
    completion.py
    crash_info.py
    dynamic_config.py
    iteration_watcher.py
    garbage_collection.py
    monitoring_client.py
    orchid_client.py
    retries.py
)

END()
