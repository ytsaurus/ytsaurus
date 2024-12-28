PY3_PROGRAM()

STYLE_PYTHON()

PY_SRCS(__main__.py)

PEERDIR(
    yt/yt/orm/codegen/generator
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/codegen/generator/induced_deps.inc)

END()
