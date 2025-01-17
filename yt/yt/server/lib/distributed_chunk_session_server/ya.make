LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    distributed_chunk_session_coordinator.cpp
    distributed_chunk_session_manager.cpp
    distributed_chunk_session_service.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
