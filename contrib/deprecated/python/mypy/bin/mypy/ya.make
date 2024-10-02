PY3_PROGRAM(mypy)

VERSION(Service-proxy-version)

LICENSE(MIT)

PEERDIR(
    contrib/deprecated/python/mypy
    # plugins dependencies
    contrib/python/marshmallow-dataclass
)

NO_LINT()

PY_MAIN(mypy.__main__:console_entry)

END()
