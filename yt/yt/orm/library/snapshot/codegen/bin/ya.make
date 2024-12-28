PY3_PROGRAM(yt-orm-snapshot-codegen)

STYLE_PYTHON()

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/yt/orm/library/snapshot/codegen

    contrib/python/click
)

END()
