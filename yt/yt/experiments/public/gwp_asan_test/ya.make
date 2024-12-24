PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/library/program
    yt/yt/library/tcmalloc
    library/cpp/getopt
)

END()
