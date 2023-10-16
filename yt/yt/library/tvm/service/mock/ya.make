LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    mock_tvm_service.cpp
)

PEERDIR(
    library/cpp/testing/gtest
    yt/yt/library/tvm/service
)

END()
