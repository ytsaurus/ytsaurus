PY23_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
)

PY_SRCS(
    binary_janitor.py
    client_helpers.py
    cypress_helpers.py
    master_config_helpers.py
    portal_cell_helpers.py
)

IF (NOT OPENSOURCE)

    PEERDIR(
        yt/admin/core
    )

    PY_SRCS(
        master_binary_fetcher_and_uploader.py
        node_config_helpers.py
    )
ENDIF()

END()
