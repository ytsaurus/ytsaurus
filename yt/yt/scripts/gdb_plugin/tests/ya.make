PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/integration/YaMakeBoilerplateForTestsWithConftest.txt)

EXPLICIT_DATA()

DATA_FILES(
    yt/yt/scripts/gdb_plugin/lib/__init__.py
    yt/yt/scripts/gdb_plugin/lib/sections.py
    yt/yt/scripts/gdb_plugin/lib/memory.py
    yt/yt/scripts/gdb_plugin/lib/type_info.py
    yt/yt/scripts/gdb_plugin/lib/signature.py
    yt/yt/scripts/gdb_plugin/lib/ref_counted.py
    yt/yt/scripts/gdb_plugin/lib/holders.py
    yt/yt/scripts/gdb_plugin/lib/fiber.py
    yt/yt/scripts/gdb_plugin/lib/fiber_attribution.py
    yt/yt/scripts/gdb_plugin/lib/commands.py
)

DEPENDS(
    yt/yt/scripts/gdb_plugin/tests/fixture
)

TEST_SRCS(
    test.py
)

TIMEOUT(180)

END()

RECURSE(
    fixture
)
