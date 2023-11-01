LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL compartment.cpp
    GLOBAL function.cpp
    GLOBAL type_builder.cpp
)

PEERDIR(
    yt/yt/core
)

END()
