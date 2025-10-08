LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    orchid_service.cpp
    orchid_ypath_service.cpp

    proto/orchid_service.proto
)

PEERDIR(
    yt/yt/core
)

END()
