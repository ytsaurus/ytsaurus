PROGRAM(uring)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/libs/liburing
)

END()
