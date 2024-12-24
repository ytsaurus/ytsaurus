LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    service_id.cpp
    service_directory.cpp
)

PEERDIR(
    yt/yt/core
)

END()

RECURSE_FOR_TESTS(
    unittests
)
