PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
    grepper.proto
)

PEERDIR(
    yt/cpp/mapreduce/client
)

END()

