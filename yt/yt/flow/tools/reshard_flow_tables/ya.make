PY3_PROGRAM()

STYLE_PYTHON()

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/yt/flow/tools/reshard_flow_tables/lib
)

END()

RECURSE(
    lib
)

RECURSE_FOR_TESTS(
    tests
)
