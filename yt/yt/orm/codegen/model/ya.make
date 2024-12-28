PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    client_plugin.py
    common.py
    compat.py
    descriptions.py
    enum.py
    field.py
    filters.py
    index.py
    loader.py
    message.py
    model.py
    object.py
    package.py
    reference.py
    snapshot.py
    tags.py
)

PEERDIR(
    library/python/resource

    yt/yt_proto/yt/orm/data_model
    yt/yt/orm/library/snapshot/codegen

    contrib/python/inflection
)

END()
