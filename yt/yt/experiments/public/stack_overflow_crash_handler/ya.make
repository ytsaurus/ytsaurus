PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/library/program
    library/cpp/yt/backtrace/symbolizers/dwarf
)

END()
