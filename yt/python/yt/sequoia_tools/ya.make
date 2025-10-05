PY3_LIBRARY()

PEERDIR(
    yt/yt/tools/record_codegen/yt_record_codegen/lib
    yt/yt/tools/record_codegen/yt_record_render/lib
    library/python/resource
    contrib/python/dacite
    contrib/python/pyaml
)

PY_SRCS(
    NAMESPACE yt.sequoia_tools

    __init__.py
)

COLLECT_YAML_CONFIG_FILES(YAML_FILES ${ARCADIA_ROOT}/yt/yt/ytlib/sequoia_client/records)
RESOURCE_FILES(
    STRIP ${ARCADIA_ROOT}/
    ${YAML_FILES}
)

END()
