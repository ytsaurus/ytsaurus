LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    compartment.cpp
    data_transfer.cpp
    function.cpp
    memory_pool.cpp
    type_builder.cpp
)

PEERDIR(
    yt/yt/core
)

END()
