PY3_LIBRARY()

COLLECT_YAML_CONFIG_FILES(YAML_FILES ${ARCADIA_ROOT}/yt/yt/ytlib/sequoia_client/records)

RESOURCE_FILES(
    STRIP ${ARCADIA_ROOT}/
    ${YAML_FILES}
)

PEERDIR(
    library/python/resource
    contrib/python/dacite
    contrib/python/pyaml
    yt/python/client
    yt/python/yt/environment/migrationlib
    yt/python/yt/wrapper
    yt/yt/tools/record_codegen/yt_record_codegen/lib
    yt/yt/tools/record_codegen/yt_record_render/lib
)

PY_SRCS(
    NAMESPACE yt.sequoia_tools

    migrations/__init__.py
    migrations/m0002.py
    migrations/m0003.py
    migrations/m0004.py

    __init__.py
    action_builder.py
    actions.py
    app.py
    config.py
    descriptors.py
    helpers.py
    initialization.py
    migration.py
    utils.py
)

END()
