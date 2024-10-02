PY3_PROGRAM(dmypy)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/deprecated/python/mypy
)

NO_LINT()

PY_MAIN(mypy.dmypy.client:console_entry)

END()
