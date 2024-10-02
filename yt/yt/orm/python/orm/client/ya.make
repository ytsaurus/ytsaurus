PY23_LIBRARY()

PEERDIR(
    yt/yt/orm/python/orm/library

    yt/yt/python/yt_yson_bindings

    yt/python/yt
    yt/python/yt/wrapper

    yt/python/contrib/python-requests

    library/python/oauth

    contrib/python/grpcio
    contrib/python/protobuf
)

PY_SRCS(
    NAMESPACE yt.orm.client

    common.py
    client.py
    discovery.py
    transport_layer.py
)

RESOURCE(
    YandexInternalRootCA.crt /yt/orm/YandexInternalRootCA.crt
)

END()
