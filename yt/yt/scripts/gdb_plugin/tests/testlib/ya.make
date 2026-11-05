PY3_LIBRARY()

# Shared test scaffolding. Must be a PEERDIR'd library, not a sibling TEST_SRCS
# module, for pytest import resolution.
PY_SRCS(
    gdb_test_lib.py
)

PY_NAMESPACE(.)

PEERDIR(
    yt/yt/tests/library
)

END()
