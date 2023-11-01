LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    compartment.cpp
    function.cpp
    type_builder.cpp
)

PEERDIR(
    yt/yt/core
)

END()
