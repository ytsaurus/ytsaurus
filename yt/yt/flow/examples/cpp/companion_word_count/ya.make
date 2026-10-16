PROGRAM()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    main.cpp
)

PEERDIR(
    yt/yt/flow/examples/cpp/word_count/lib
    yt/yt/flow/library/cpp/companion/server
)

END()
