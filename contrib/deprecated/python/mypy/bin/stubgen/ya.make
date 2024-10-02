PY3_PROGRAM(stubgen)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/deprecated/python/mypy
)

NO_LINT()

PY_MAIN(mypy.stubgen:main)

END()
