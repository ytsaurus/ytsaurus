LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    sequencer.cpp
    session_manager.cpp
    session_service.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()

RECURSE_FOR_TESTS(
    unittests
)
