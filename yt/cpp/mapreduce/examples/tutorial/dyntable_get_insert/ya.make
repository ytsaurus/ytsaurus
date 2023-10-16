PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/util
)

END()

