PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    data.proto
    main.cpp
)

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/library/lambda
)

END()
