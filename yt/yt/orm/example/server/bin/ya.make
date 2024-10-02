PROGRAM(ytserver-orm-example)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/ya_cpp.make.inc)

RUN_PROGRAM(
    yt/yt/orm/example/codegen render-main-cpp --output-path ${BINDIR}/main.cpp
    OUT
        ${BINDIR}/main.cpp
    OUTPUT_INCLUDES
        yt/yt/orm/example/server/library/autogen/program.h
)

PEERDIR(
    yt/yt/orm/example/server/library
)

END()
