LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/testing/gtest
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/client
)

SRCS(
    main.cpp
)

END()
