PY3_PROGRAM(mcp_yt_server)

SUBSCRIBER(g:yt g:yt-python)

PY_SRCS(
    MAIN mcp_yt_server.py
    __init__.py
)

PEERDIR(
    yt/python/yt/mcp/lib
    contrib/python/mcp
    contrib/python/pydantic/pydantic-2
)

END()
