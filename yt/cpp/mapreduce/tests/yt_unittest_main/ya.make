LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
)

SRCS(
    main.cpp
)

END()
