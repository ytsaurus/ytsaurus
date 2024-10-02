PY3_LIBRARY()

PEERDIR(
    yt/yt/orm/example/python/admin/autogen
    yt/yt/orm/example/python/client

    yt/yt/orm/python/orm/admin
    yt/yt/orm/python/orm/library
)

PY_SRCS(
   __init__.py
    data_model_traits.py
    db_operations.py
)

END()
