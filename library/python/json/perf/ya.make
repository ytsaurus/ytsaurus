PY2_PROGRAM()

OWNER(pg)

PEERDIR(
    library/python/json
    contrib/python/ujson
    contrib/python/simplejson
)

PY_SRCS(__main__.py)

END()
