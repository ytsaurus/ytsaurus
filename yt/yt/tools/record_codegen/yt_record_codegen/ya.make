PY3_PROGRAM(record-codegen)

PY_SRCS(
    NAMESPACE yt_record_codegen
    __init__.py
)

PEERDIR(
    yt/yt/tools/record_codegen/yt_record_codegen/lib
    yt/yt/tools/record_codegen/yt_record_render/lib
    library/python/resource
    contrib/python/Jinja2
    contrib/python/dacite
    contrib/python/pyaml
)

RESOURCE(
    templates/h.j2 h.j2
    templates/cpp.j2 cpp.j2
)

END()

RECURSE(
    bin
    tests
)
