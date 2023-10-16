PY3_LIBRARY()

PEERDIR(
    contrib/python/Flask
    contrib/python/WTForms
    contrib/python/gunicorn
    yt/yt/python/yt_driver_rpc_bindings
)

IF (NOT OPENSOURCE)
    PEERDIR(
        yql/library/python
    )
ELSE()
    PEERDIR(
        yt/python/client
    )
ENDIF()

PY_SRCS(
    NAMESPACE lib

    create_tables.py
    run_application.py
)

END()
