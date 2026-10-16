PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    definitions.py
    schemas.py
    presets.py
)

RESOURCE_FILES(
    PREFIX yt/yt/flow/library/pipeline_tables/
    STRIP ../../pipeline_tables/
    ../../pipeline_tables/definitions.yson
)

PEERDIR(
    library/python/resource
    yt/python/yt/yson
)

END()

RECURSE_FOR_TESTS(
    tests
)
