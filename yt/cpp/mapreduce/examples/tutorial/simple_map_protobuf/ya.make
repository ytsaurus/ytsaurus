PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
    data.proto
)

PEERDIR(
    yt/cpp/mapreduce/client
)

END()

