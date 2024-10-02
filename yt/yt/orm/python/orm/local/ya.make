PY23_LIBRARY()

PEERDIR(
    yt/yt/orm/python/orm/yt_manager/local
    yt/yt/orm/python/orm/library

    yt/python/yt
    yt/python/yt/environment
    yt/python/yt/local
    yt/python/yt/packages
    yt/python/yt/yson

    # Used for collect_cores.
    yt/python/yt/environment/arcadia_interop
)

PY_SRCS(
    NAMESPACE yt.orm.local

    local.py
    local_microservice.py
    local_ssl_keys.py
)

END()
