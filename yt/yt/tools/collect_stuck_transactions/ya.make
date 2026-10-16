PY3_PROGRAM(collect_stuck_transactions)

PY_SRCS(
    __main__.py
)

PEERDIR(
    contrib/python/aiohttp
    yt/python/client
)

STYLE_RUFF()

END()
