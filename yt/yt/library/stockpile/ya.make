LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    GLOBAL configure_stockpile.cpp
)

PEERDIR(
    yt/yt/core
    library/cpp/yt/stockpile
)

END()
