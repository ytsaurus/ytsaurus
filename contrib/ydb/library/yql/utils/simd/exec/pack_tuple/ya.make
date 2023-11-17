PROGRAM()

SRCS(main.cpp)

CFLAGS(-mavx2)

PEERDIR(contrib/ydb/library/yql/utils/simd)

END()