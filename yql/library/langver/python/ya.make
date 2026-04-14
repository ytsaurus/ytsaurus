PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    library/python/resource
)

RESOURCE(
    yql/essentials/data/language/langver.json /yql/langver.json
)

END()
