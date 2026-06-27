PROGRAM(fixture)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

# Debug info for gdb. NB: do not force ABI-affecting defines (the ref-counted
# signature, the fiber unwind anchor) here -- they alter type layout and must be
# globally consistent, so they're left to the build mode. The test runs in both
# debug and release, covering the frame-pointer (rbp-walk) and frame-pointer-less
# (anchor / CFI) unwinding paths respectively.
CFLAGS(
    -g
)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/core
)

END()
