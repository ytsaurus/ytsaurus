LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL compartment.cpp
    GLOBAL function.cpp
    intrinsics.cpp
    GLOBAL type_builder.cpp
)

ADDINCL(
    contrib/restricted/wavm/Include
)

PEERDIR(
    yt/yt/core
    contrib/restricted/wavm/Lib
)

CFLAGS(
    -DWASM_C_API=WAVM_API
    -DWAVM_API=
)

END()
