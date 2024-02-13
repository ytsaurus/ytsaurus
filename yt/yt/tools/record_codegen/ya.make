
PY3_PROGRAM(record-codegen)

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/python/yt/record_codegen_helpers
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
