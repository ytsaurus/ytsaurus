INCLUDE(${ARCADIA_ROOT}/yt/ya.make.environment.inc)
INCLUDE(${ARCADIA_ROOT}/${YT_PYTHON_ROOT}/python/yt/environment/ya_programs.make.inc)

DEPENDS(
    yp/server/master_program
)

