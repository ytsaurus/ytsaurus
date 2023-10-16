PY23_LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_COMPILER_WARNINGS()

NO_LINT()

ADDINCL(contrib/libs/brotli/include)

PEERDIR(
    contrib/libs/brotli/enc
    contrib/libs/brotli/dec
)

SRCS(
    _brotli.cc
)

PY_SRCS(
    TOP_LEVEL
    brotli.py
)

IF (USE_ARCADIA_PYTHON)
    PY_REGISTER(_brotli)
ENDIF()

END()

RECURSE(
    bro
    tests
)
