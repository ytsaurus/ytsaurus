PY3_PROGRAM()

STYLE_PYTHON()

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/pandas
    contrib/python/requests
    yt/python/client_with_rpc
    yt/yt/flow/library/python/client/flow_view
)

END()
