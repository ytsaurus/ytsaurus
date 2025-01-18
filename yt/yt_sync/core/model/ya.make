PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    cluster.py
    column.py
    database.py
    helpers.py
    native_consumer.py
    node_attributes.py
    node.py
    replica.py
    schema.py
    table_attributes.py
    table.py
    tablet_info.py
    tablet_state.py
    types.py
)

PEERDIR(
    yt/yt_sync/core/spec

    yt/python/client
)

END()

RECURSE_FOR_TESTS(
    ut
)
