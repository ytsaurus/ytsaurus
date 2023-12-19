PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/client
)

SRCS(
    main.cpp
)

END()
