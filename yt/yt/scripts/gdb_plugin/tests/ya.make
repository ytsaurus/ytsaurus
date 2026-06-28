PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTestsWithConftest.txt)

EXPLICIT_DATA()

DATA_FILES(
    yt/yt/scripts/gdb_plugin/lib/__init__.py
    yt/yt/scripts/gdb_plugin/lib/_announce.py
    yt/yt/scripts/gdb_plugin/lib/sections.py
    yt/yt/scripts/gdb_plugin/lib/memory.py
    yt/yt/scripts/gdb_plugin/lib/tcmalloc.py
    yt/yt/scripts/gdb_plugin/lib/type_info.py
    yt/yt/scripts/gdb_plugin/lib/signature.py
    yt/yt/scripts/gdb_plugin/lib/ref_counted.py
    yt/yt/scripts/gdb_plugin/lib/ref_counted_tracker.py
    yt/yt/scripts/gdb_plugin/lib/holders.py
    yt/yt/scripts/gdb_plugin/lib/fiber.py
    yt/yt/scripts/gdb_plugin/lib/fiber_attribution.py
    yt/yt/scripts/gdb_plugin/lib/fiber_commands.py
    yt/yt/scripts/gdb_plugin/lib/commands.py
    yt/yt/scripts/gdb_plugin/lib/printers.py
)

DEPENDS(
    yt/yt/scripts/gdb_plugin/tests/fixture
)

PEERDIR(
    yt/yt/scripts/gdb_plugin/tests/testlib
)

TEST_SRCS(
    test_refcount.py
    test_printers.py
    test_fibers.py
    test_tcmalloc.py
)

TIMEOUT(180)

END()

RECURSE(
    fixture
    testlib
)
