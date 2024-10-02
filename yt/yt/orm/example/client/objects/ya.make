LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)

SET(OUTDIR ${BINDIR}/autogen)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/codegen/config.inc)
INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/codegen/generator/build/client_objects.make.inc)

END()
