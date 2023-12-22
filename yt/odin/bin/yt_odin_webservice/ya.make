PY3_PROGRAM(yt_odin_webservice)

PEERDIR(
    yt/odin/lib/yt_odin/odinserver
    yt/odin/lib/yt_odin/logserver
    yt/odin/lib/yt_odin/storage

    yt/python/yt/wrapper

    contrib/python/python-dateutil
    contrib/python/cheroot
    contrib/python/Flask
)

PY_SRCS(
    __main__.py
)

END()
