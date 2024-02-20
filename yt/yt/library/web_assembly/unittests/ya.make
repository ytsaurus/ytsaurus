GTEST(unittester-web-assembly)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    disable_system_libraries.cpp
    wasm_ut.cpp
)

# This flag is required for linking code of bc functions.
# Note that -rdynamic enabled by default in arcadia build, but we do not want to rely on it.
LDFLAGS(-rdynamic)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

ADDINCL(
    contrib/restricted/wavm/Include
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

PEERDIR(
    yt/yt/build
    yt/yt/core/test_framework
    yt/yt/library/web_assembly/engine
    yt/yt/library/web_assembly/api
)

SIZE(MEDIUM)

END()
