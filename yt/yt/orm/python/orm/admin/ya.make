PY23_LIBRARY()

PEERDIR(
    yt/yt/orm/python/orm/client
    yt/yt/orm/python/orm/library

    yt/python/yt/wrapper
    yt/python/yt/yson
)

PY_SRCS(
    NAMESPACE yt.orm.admin

    ban_manager.py
    configure_db_bundle.py
    data_model_traits.py
    db_manager.py
    db_operations.py
    replicated_db_manager.py
    reshard_db_table.py
)

IF (PYTHON3)
    PEERDIR(
        yt/yt_sync
    )

    PY_SRCS(
        NAMESPACE yt.orm.admin

        db_sync.py
    )
ENDIF()

END()
