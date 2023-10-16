PY3TEST()

INCLUDE(../YaMakeBoilerplateForTestsWithConftest.txt)

EXPLICIT_DATA()

DATA_FILES(
    devtools/gdb/yt_fibers_printer.py
    devtools/gdb/arcadia_printers.py
    devtools/gdb/arcadia_xmethods.py
    devtools/gdb/__init__.py
    devtools/gdb/libc_printers.py
    devtools/gdb/libcxx_printers.py
    devtools/gdb/libcxx_xmethods.py
    devtools/gdb/libpython_printers.py
    devtools/gdb/libstdcpp_printers.py
    devtools/gdb/yabs_printers.py
)

DEPENDS(
    yt/yt/core
    yt/yt/tests/integration/yt_fibers_printer/gdbtest
)

TEST_SRCS(
    test.py
)

TIMEOUT(180)

END()

RECURSE(
    gdbtest
)
