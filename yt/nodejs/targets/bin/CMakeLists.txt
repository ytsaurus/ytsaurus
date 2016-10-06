PROGRAM(ytnode)

OWNER(
    snowball
)

NO_COMPILER_WARNINGS()

SET(NODE_DIR contrib/libs/node_0_8_28)

PEERDIR(
    ${NODE_DIR}/targets/libnode
    yt/18_5/yt/nodejs/targets/lib
)

SRCS(
    main.cc
)

END()

