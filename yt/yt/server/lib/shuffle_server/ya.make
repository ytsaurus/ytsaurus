LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    shuffle_controller.cpp
    shuffle_manager.cpp
    shuffle_service.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
