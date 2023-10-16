PY2_PROGRAM(bro)

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_LINT()

PEERDIR(
    contrib/libs/brotli/python
)

SRCDIR(contrib/libs/brotli/python)

PY_SRCS(
    TOP_LEVEL
    MAIN
    bro.py
)

END()
