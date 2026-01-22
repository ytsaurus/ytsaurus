LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    builtins.cpp
    GLOBAL compartment.cpp
    GLOBAL data_transfer.cpp
    GLOBAL function.cpp
    intrinsics.cpp
    GLOBAL memory_pool.cpp
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
        FILE 9512287245 OUT_NOAUTO libemscripten-system-libraries-dll.so
    )

    FROM_SANDBOX(
        FILE 9512289270 OUT_NOAUTO libemscripten-system-libraries-dll.so.compiled
    )

    FROM_SANDBOX(
        FILE 9513256380 OUT_NOAUTO libwasm-udfs-builtin-ytql-udfs.so
    )

    RESOURCE(
        libemscripten-system-libraries-dll.so libemscripten-system-libraries-dll.so
        libemscripten-system-libraries-dll.so.compiled libemscripten-system-libraries-dll.so.compiled
        libwasm-udfs-builtin-ytql-udfs.so libwasm-udfs-builtin-ytql-udfs.so
    )
ENDIF()

END()
