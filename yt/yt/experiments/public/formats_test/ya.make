PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(main.cpp)

PEERDIR(yt/yt/client/driver)

END()

RECURSE(formattest)
