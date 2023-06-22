PY2TEST()

OWNER(orivej halyavin g:yatool)

DATA(
    arcadia/devtools/gdb
)

DEPENDS(
    devtools/gdb/test/gdbtest
)

TEST_SRCS(
    test.py
)

FORK_SUBTESTS()
SPLIT_FACTOR(16)

END()

RECURSE(
    gdbtest
)
