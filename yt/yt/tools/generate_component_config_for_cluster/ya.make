PY3_PROGRAM()

PEERDIR(
    yt/python/client
    contrib/python/click
)

PY_SRCS(
    __init__.py
    __main__.py
)

ALL_RESOURCE_FILES(
    PREFIX yt/yt/tools/generate_component_config_for_cluster/
    yson
    templates/components
)

END()
