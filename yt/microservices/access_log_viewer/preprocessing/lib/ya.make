PY3_LIBRARY()

PY_SRCS(
    cli.py
    operations.py
)

PEERDIR(
    yt/admin/core/operation_pipeline
    yt/microservices/id_to_path_mapping/client

    yt/python/client

    contrib/python/click
    contrib/python/cachetools
    contrib/python/python-dateutil
    contrib/python/psutil
)

END()
