PY3_LIBRARY()

PEERDIR(
    yt/yt/orm/example/python/common

    yt/yt/orm/example/client/proto/api
    yt/yt/orm/example/client/proto/data_model

    yt/yt/orm/python/orm/library
    yt/yt/orm/python/orm/client

    yt/yt_proto/yt/core
)

PY_SRCS(
    __init__.py
    client.py
    data_model.py
)

RESOURCE(
    YandexInternalRootCA.crt /example/YandexInternalRootCA.crt
)

END()
