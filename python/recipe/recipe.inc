DEPENDS(${ARCADIA_ROOT}/yp/python/recipe/bin)

INCLUDE(../../YaMakeBoilerplate.txt)

DEFAULT(YP_COUNT 1)

# WARNING: This is experimental feature and will likely break local YP.
# WARNING: Use on your own risk.
# WARNING: Contact: roboslone@.
DEFAULT(YP_STARTUP_THREADS 1)

IF(YT_INSIDE_ARCADIA)
    USE_RECIPE(yp/python/recipe/bin/yp-recipe --count ${YP_COUNT} --startup-threads ${YP_STARTUP_THREADS})
ELSE()
    USE_RECIPE(yp/python/recipe/bin/yp-recipe --outside-arcadia --count ${YP_COUNT --startup-threads ${YP_STARTUP_THREADS}})
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yp/python/ya_programs.make.inc)
