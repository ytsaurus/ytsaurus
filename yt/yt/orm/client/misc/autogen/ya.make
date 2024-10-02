LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

RUN_PROGRAM(
    yt/yt/orm/codegen/bin render-error-cpp-enum --output-path ${BINDIR}/error.h
    OUT
        ${BINDIR}/error.h
    OUTPUT_INCLUDES
        library/cpp/yt/misc/enum.h
)

END()
