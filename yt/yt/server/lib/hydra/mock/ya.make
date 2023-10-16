LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    simple_hydra_manager_mock.cpp
)

PEERDIR(
    yt/yt/server/lib/hydra
)

END()
