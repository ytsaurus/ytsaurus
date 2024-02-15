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

IF (NOT OPENSOURCE)
    FROM_SANDBOX(
        FILE 4987655926 OUT_NOAUTO lib.so.wasm
    )

    FROM_SANDBOX(
        FILE 5750993567 OUT_NOAUTO all-udfs.so.wasm
    )

    FROM_SANDBOX(
        FILE 5040591399 OUT_NOAUTO compiled-libc
    )

    RESOURCE(
        lib.so.wasm libc.so.wasm
        all-udfs.so.wasm all-udfs.so.wasm
        compiled-libc compiled-libc
    )
ENDIF()

END()
