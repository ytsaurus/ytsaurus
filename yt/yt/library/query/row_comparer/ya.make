LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL row_comparer_generator.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/core
    yt/yt/library/codegen
)

END()
